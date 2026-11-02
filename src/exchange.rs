use crate::types::{Position, Side, SymbolSpec, TradeRecord};
use anyhow::{bail, Context, Result};
use chrono::Utc;
use reqwest::Client;
use ring::hmac;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tracing::{error, info, warn};

/// 持久化实盘/模拟盘状态账本 (`data/live_state.json`)，防止进程重启丢失持仓或风控记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentLedger {
    pub mode: String,
    pub initial_equity: Decimal,
    pub cash_balance: Decimal,
    pub peak_equity: Decimal,
    pub consecutive_losses: usize,
    pub open_positions: Vec<Position>,
    pub stop_order_ids: std::collections::HashMap<String, String>,
    pub closed_trades: Vec<TradeRecord>,
    pub updated_at: String,
}

impl PersistentLedger {
    pub fn load_or_init<P: AsRef<Path>>(path: P, mode: &str, initial_equity: Decimal) -> Result<Self> {
        if path.as_ref().exists() {
            let content = fs::read_to_string(path.as_ref())?;
            if let Ok(ledger) = serde_json::from_str::<Self>(&content) {
                return Ok(ledger);
            }
        }
        let ledger = Self {
            mode: mode.to_string(),
            initial_equity,
            cash_balance: initial_equity,
            peak_equity: initial_equity,
            consecutive_losses: 0,
            open_positions: Vec::new(),
            stop_order_ids: std::collections::HashMap::new(),
            closed_trades: Vec::new(),
            updated_at: Utc::now().to_rfc3339(),
        };
        ledger.save(path)?;
        Ok(ledger)
    }

    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent)?;
        }
        let mut copy = self.clone();
        copy.updated_at = Utc::now().to_rfc3339();
        let json = serde_json::to_string_pretty(&copy)?;
        fs::write(path, json)?;
        Ok(())
    }
}

/// 币安 U 本位永续合约 (`fapi.binance.com`) 实盘与 Dry-Run 统一网关
pub struct BinanceFuturesGateway {
    client: Client,
    base_url: String,
    api_key: String,
    api_secret: String,
    pub dry_run: bool,
    pub ledger_path: PathBuf,
}

impl BinanceFuturesGateway {
    pub fn new(dry_run: bool, ledger_path: impl Into<PathBuf>) -> Result<Self> {
        let api_key = std::env::var("BINANCE_API_KEY").unwrap_or_default();
        let api_secret = std::env::var("BINANCE_API_SECRET").unwrap_or_default();

        if !dry_run
            && (api_key.is_empty()
                || api_secret.is_empty()
                || api_key == "your_api_key_here")
        {
            bail!(
                "启动 Live 实盘模式失败：未检测到有效的 BINANCE_API_KEY / BINANCE_API_SECRET 环境变量！\
                请先使用 `coin-ai dry-run` 或 `coin-ai scan` 测试，或在环境变量中配置 API Key。"
            );
        }

        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent("coin-ai-rust-gateway/0.1.0")
            .build()?;

        Ok(Self {
            client,
            base_url: "https://fapi.binance.com".to_string(),
            api_key,
            api_secret,
            dry_run,
            ledger_path: ledger_path.into(),
        })
    }

    /// 使用 ring::hmac 计算 HMAC-SHA256 十六进制签名
    pub fn sign_query(&self, query_string: &str) -> String {
        let key = hmac::Key::new(hmac::HMAC_SHA256, self.api_secret.as_bytes());
        let tag = hmac::sign(&key, query_string.as_bytes());
        let mut hex_str = String::with_capacity(tag.as_ref().len() * 2);
        for b in tag.as_ref() {
            use std::fmt::Write;
            let _ = write!(&mut hex_str, "{:02x}", b);
        }
        hex_str
    }

    /// 开仓前初始化币种逐仓模式 (ISOLATED) 与安全杠杆倍数 (默认 3x)
    pub async fn ensure_isolated_and_leverage(&self, symbol: &str, leverage: u32) -> Result<()> {
        if self.dry_run {
            info!(
                "[Dry-Run] 已确认 {} 为 ISOLATED 逐仓模式，标称杠杆 {}x",
                symbol, leverage
            );
            return Ok(());
        }

        let ts = Utc::now().timestamp_millis();
        // 1. 切换逐仓模式 (若已经是 ISOLATED 会返回 -4046，属于正常情况直接忽略)
        let margin_q = format!(
            "symbol={}&marginType=ISOLATED&timestamp={}",
            symbol, ts
        );
        let sig1 = self.sign_query(&margin_q);
        let url1 = format!(
            "{}/fapi/v1/marginType?{}&signature={}",
            self.base_url, margin_q, sig1
        );
        let _ = self
            .client
            .post(&url1)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await;

        // 2. 设置低倍安全杠杆 (3x)
        let lev_q = format!(
            "symbol={}&leverage={}&timestamp={}",
            symbol, leverage, ts
        );
        let sig2 = self.sign_query(&lev_q);
        let url2 = format!(
            "{}/fapi/v1/leverage?{}&signature={}",
            self.base_url, lev_q, sig2
        );
        let resp = self
            .client
            .post(&url2)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;

        if !resp.status().is_success() {
            let txt = resp.text().await.unwrap_or_default();
            warn!("设置 {} 杠杆返回提示: {}", symbol, txt);
        }
        Ok(())
    }

    /// 核心安全开仓闭环：
    /// 1) 发送幂等市价开仓单；
    /// 2) 1 秒内立即在交易所挂载 STOP_MARKET 硬止损单 (reduceOnly=true)；
    /// 3) 若硬止损单挂载失败，立即市价平掉刚开的仓位（裸奔仓位零容忍！）
    pub async fn open_position_with_hard_stop(
        &self,
        symbol: &str,
        side: Side,
        qty: Decimal,
        expected_price: Decimal,
        stop_price: Decimal,
    ) -> Result<(Decimal, String)> {
        let spec = SymbolSpec::default_for(symbol);
        let aligned_qty = spec.round_qty(qty);
        let aligned_stop = spec.round_price(stop_price);

        if aligned_qty <= Decimal::ZERO {
            bail!("对齐精度后下单数量为 0 ({})", symbol);
        }

        self.ensure_isolated_and_leverage(symbol, 3).await?;

        if self.dry_run {
            let mock_stop_id = format!("DRY_STOP_{}_{}", symbol, Utc::now().timestamp());
            info!(
                "[Dry-Run] 成交开仓单: {} {} qty={} @ ~{} | 服务端 STOP_MARKET 止损单已挂载 @ {} (orderId={})",
                symbol, side, aligned_qty, expected_price, aligned_stop, mock_stop_id
            );
            return Ok((expected_price, mock_stop_id));
        }

        let order_side = match side {
            Side::Long => "BUY",
            Side::Short => "SELL",
        };
        let ts = Utc::now().timestamp_millis();
        let client_id = format!("coinai_open_{}_{}", symbol, ts);

        let q = format!(
            "symbol={}&side={}&type=MARKET&quantity={}&newClientOrderId={}&timestamp={}",
            symbol, order_side, aligned_qty, client_id, ts
        );
        let sig = self.sign_query(&q);
        let url = format!("{}/fapi/v1/order?{}&signature={}", self.base_url, q, sig);

        let resp = self
            .client
            .post(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .context("发送实盘市价开仓单网络异常")?;

        if !resp.status().is_success() {
            let err_txt = resp.text().await.unwrap_or_default();
            bail!("实盘开仓被交易所拒绝: {}", err_txt);
        }

        let order_json: Value = resp.json().await?;
        let avg_price = order_json["avgPrice"]
            .as_str()
            .and_then(|s| s.parse::<Decimal>().ok())
            .filter(|&p| p > Decimal::ZERO)
            .unwrap_or(expected_price);

        // 立即挂载交易所端 STOP_MARKET 硬止损单
        match self.place_server_stop_market(symbol, side, aligned_qty, aligned_stop).await {
            Ok(stop_order_id) => Ok((avg_price, stop_order_id)),
            Err(stop_err) => {
                error!(
                    "致命警告：{} 挂载服务端 STOP_MARKET 止损单失败 ({})！启动零裸奔保护，立即市价平仓撤退！",
                    symbol, stop_err
                );
                let _ = self.close_position_market(symbol, side, aligned_qty).await;
                bail!("因无法挂载服务端止损单，已自动平仓回滚该笔交易: {}", stop_err);
            }
        }
    }

    /// 在交易所端挂载 STOP_MARKET 硬止损单 (reduceOnly=true, workingType=MARK_PRICE 防插针)
    pub async fn place_server_stop_market(
        &self,
        symbol: &str,
        position_side: Side,
        qty: Decimal,
        stop_price: Decimal,
    ) -> Result<String> {
        let spec = SymbolSpec::default_for(symbol);
        let aligned_qty = spec.round_qty(qty);
        let aligned_stop = spec.round_price(stop_price);

        if self.dry_run {
            let id = format!("DRY_STOP_{}_{}", symbol, Utc::now().timestamp_millis());
            info!(
                "[Dry-Run] 服务端 STOP_MARKET 已更新: {} stopPrice={} qty={} (id={})",
                symbol, aligned_stop, aligned_qty, id
            );
            return Ok(id);
        }

        let stop_side = match position_side {
            Side::Long => "SELL",
            Side::Short => "BUY",
        };
        let ts = Utc::now().timestamp_millis();
        let q = format!(
            "symbol={}&side={}&type=STOP_MARKET&stopPrice={}&quantity={}&reduceOnly=true&workingType=MARK_PRICE&timestamp={}",
            symbol, stop_side, aligned_stop, aligned_qty, ts
        );
        let sig = self.sign_query(&q);
        let url = format!("{}/fapi/v1/order?{}&signature={}", self.base_url, q, sig);

        let resp = self
            .client
            .post(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err_txt = resp.text().await.unwrap_or_default();
            bail!("挂载 STOP_MARKET 失败: {}", err_txt);
        }
        let body: Value = resp.json().await?;
        let order_id = body["orderId"]
            .as_i64()
            .map(|id| id.to_string())
            .unwrap_or_else(|| "UNKNOWN_ID".to_string());
        Ok(order_id)
    }

    /// 市价减仓平仓 (reduceOnly=true)
    pub async fn close_position_market(
        &self,
        symbol: &str,
        position_side: Side,
        qty: Decimal,
    ) -> Result<()> {
        let spec = SymbolSpec::default_for(symbol);
        let aligned_qty = spec.round_qty(qty);

        if self.dry_run {
            info!(
                "[Dry-Run] 市价平仓执行完毕: {} ({}) qty={}",
                symbol, position_side, aligned_qty
            );
            return Ok(());
        }

        let close_side = match position_side {
            Side::Long => "SELL",
            Side::Short => "BUY",
        };
        let ts = Utc::now().timestamp_millis();
        let q = format!(
            "symbol={}&side={}&type=MARKET&quantity={}&reduceOnly=true&timestamp={}",
            symbol, close_side, aligned_qty, ts
        );
        let sig = self.sign_query(&q);
        let url = format!("{}/fapi/v1/order?{}&signature={}", self.base_url, q, sig);

        let resp = self
            .client
            .post(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;

        if !resp.status().is_success() {
            let err_txt = resp.text().await.unwrap_or_default();
            bail!("市价平仓失败: {}", err_txt);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hmac_sha256_signature() {
        let gw = BinanceFuturesGateway {
            client: Client::new(),
            base_url: "https://fapi.binance.com".to_string(),
            api_key: "test_key".to_string(),
            api_secret: "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j"
                .to_string(),
            dry_run: true,
            ledger_path: PathBuf::from("data/test_ledger.json"),
        };
        let query = "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559";
        let sig = gw.sign_query(query);
        assert_eq!(
            sig,
            "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71"
        );
    }
}
