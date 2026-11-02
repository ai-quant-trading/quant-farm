use crate::types::Candle;
use anyhow::{Context, Result};
use reqwest::Client;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;
use tracing::{info, warn};

#[derive(Clone)]
pub struct DataManager {
    client: Client,
    data_dir: PathBuf,
}

impl DataManager {
    pub fn new<P: AsRef<Path>>(data_dir: P) -> Result<Self> {
        let dir = data_dir.as_ref().to_path_buf();
        if !dir.exists() {
            fs::create_dir_all(&dir)?;
        }
        let client = Client::builder()
            .timeout(Duration::from_secs(6))
            .user_agent("coin-ai-quant/0.1.0")
            .build()?;
        Ok(Self {
            client,
            data_dir: dir,
        })
    }

    pub fn cache_path(&self, symbol: &str) -> PathBuf {
        self.data_dir.join(format!("{}_1h.json", symbol))
    }

    /// 获取指定币种的 1h 历史 K 线数据（优先读取本地缓存，刷新时增量合并，绝不冲掉长周期历史缓存）
    pub async fn load_or_fetch_candles(
        &self,
        symbol: &str,
        target_bars: usize,
        force_download: bool,
    ) -> Result<Vec<Candle>> {
        let path = self.cache_path(symbol);
        let mut cached_candles: Vec<Candle> = if path.exists() {
            fs::read_to_string(&path)
                .ok()
                .and_then(|content| serde_json::from_str::<Vec<Candle>>(&content).ok())
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        if !force_download && cached_candles.len() >= target_bars {
            return Ok(cached_candles);
        }

        let fetch_count = if force_download && cached_candles.len() >= target_bars {
            // 已有长周期缓存时，强制刷新只需拉取最近 100 根 K 线增量合并，速度提升 30 倍
            100.min(target_bars).max(50)
        } else {
            target_bars
        };

        info!(
            "正在拉取 {} 的 1h 行情数据 (请求 {} 根 K 线, 本地已有 {} 根)...",
            symbol,
            fetch_count,
            cached_candles.len()
        );

        // 优先尝试 Binance Vision 官方免代理公开行情源 -> 其次 Binance Futures -> 其次 OKX
        let fetch_result: Result<Vec<Candle>> = async {
            match self.fetch_from_binance_vision(symbol, fetch_count).await {
                Ok(c) if !c.is_empty() => Ok(c),
                Err(e1) => {
                    warn!(
                        "Binance Vision 接口请求受限 ({})，尝试 Binance Futures 接口...",
                        e1
                    );
                    match self.fetch_from_binance_fapi(symbol, fetch_count).await {
                        Ok(c) if !c.is_empty() => Ok(c),
                        Err(e2) => {
                            warn!("Binance Futures 接口受限 ({})，尝试 OKX 公开行情接口...", e2);
                            self.fetch_from_okx(symbol, fetch_count).await
                        }
                        _ => self.fetch_from_okx(symbol, fetch_count).await,
                    }
                }
                _ => self.fetch_from_okx(symbol, fetch_count).await,
            }
        }
        .await;

        // 三个公开行情源全部不可用时，绝不因为一次网络抖动让守护进程退出：
        // 只要本地还有历史缓存就直接回退使用，等待下一次整点增量刷新自然恢复。
        let fetched = match fetch_result {
            Ok(c) if !c.is_empty() => c,
            other => {
                if cached_candles.is_empty() {
                    match other {
                        Ok(_) => anyhow::bail!("{} 所有行情源均返回空数据", symbol),
                        Err(e) => return Err(e),
                    }
                }
                warn!(
                    "{} 所有行情源暂时不可用，回退使用本地缓存 {} 根 K 线（守护进程继续运行，等待下次刷新）",
                    symbol,
                    cached_candles.len()
                );
                return Ok(cached_candles);
            }
        };

        // 将新拉取的 K 线与本地历史缓存按时间戳合并（新数据覆盖同时间戳旧数据）
        let mut merged_map: std::collections::BTreeMap<i64, Candle> =
            std::collections::BTreeMap::new();
        for c in cached_candles.drain(..) {
            merged_map.insert(c.timestamp, c);
        }
        for c in fetched {
            merged_map.insert(c.timestamp, c);
        }
        let merged: Vec<Candle> = merged_map.into_values().collect();

        let serialized = serde_json::to_string(&merged)?;
        fs::write(&path, serialized)
            .with_context(|| format!("写入本地行情缓存失败: {}", path.display()))?;
        info!("{} 行情数据已同步落盘: 本地共缓存 {} 根 1h K 线", symbol, merged.len());
        Ok(merged)
    }

    async fn fetch_from_binance_vision(&self, symbol: &str, target_bars: usize) -> Result<Vec<Candle>> {
        self.fetch_binance_compatible(
            "https://data-api.binance.vision/api/v3/klines",
            symbol,
            target_bars,
        )
        .await
    }

    async fn fetch_from_binance_fapi(&self, symbol: &str, target_bars: usize) -> Result<Vec<Candle>> {
        self.fetch_binance_compatible(
            "https://fapi.binance.com/fapi/v1/klines",
            symbol,
            target_bars,
        )
        .await
    }

    async fn fetch_binance_compatible(
        &self,
        base_url: &str,
        symbol: &str,
        target_bars: usize,
    ) -> Result<Vec<Candle>> {
        let mut all_candles: Vec<Candle> = Vec::with_capacity(target_bars);
        let mut end_time: Option<i64> = None;
        let batch_limit = 1000usize;

        while all_candles.len() < target_bars {
            let remaining = target_bars - all_candles.len();
            let limit = remaining.min(batch_limit);
            let mut url = format!(
                "{}?symbol={}&interval=1h&limit={}",
                base_url, symbol, limit
            );
            if let Some(et) = end_time {
                url.push_str(&format!("&endTime={}", et));
            }

            let resp = self.client.get(&url).send().await?.error_for_status()?;
            let rows: Vec<Vec<Value>> = resp.json().await?;
            if rows.is_empty() {
                break;
            }

            let mut batch = Vec::with_capacity(rows.len());
            for row in &rows {
                if row.len() < 6 {
                    continue;
                }
                let ts = row[0].as_i64().unwrap_or(0);
                let open = parse_dec(&row[1]);
                let high = parse_dec(&row[2]);
                let low = parse_dec(&row[3]);
                let close = parse_dec(&row[4]);
                let volume = parse_dec(&row[5]);

                // 模拟或附加基础资金费率 (每 8 小时一次结算点: 00:00, 08:00, 16:00 UTC)
                let hour = (ts / 3_600_000) % 24;
                let funding_rate = if hour % 8 == 0 {
                    dec!(0.0001)
                } else {
                    Decimal::ZERO
                };

                batch.push(Candle {
                    timestamp: ts,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    funding_rate,
                });
            }

            if batch.is_empty() {
                break;
            }

            let earliest_ts = batch.first().unwrap().timestamp;
            end_time = Some(earliest_ts - 1);

            batch.extend(all_candles);
            all_candles = batch;

            if rows.len() < limit {
                break;
            }
        }

        all_candles.sort_by_key(|c| c.timestamp);
        all_candles.dedup_by_key(|c| c.timestamp);
        Ok(all_candles)
    }

    async fn fetch_from_okx(&self, symbol: &str, target_bars: usize) -> Result<Vec<Candle>> {
        let inst_id = if let Some(base) = symbol.strip_suffix("USDT") {
            format!("{}-USDT-SWAP", base)
        } else {
            symbol.to_string()
        };

        let mut all_candles: Vec<Candle> = Vec::with_capacity(target_bars);
        let mut after_ts: Option<i64> = None;

        while all_candles.len() < target_bars {
            let mut url = format!(
                "https://www.okx.com/api/v5/market/history-candles?instId={}&bar=1H&limit=100",
                inst_id
            );
            if let Some(after) = after_ts {
                url.push_str(&format!("&after={}", after));
            }

            let resp = self.client.get(&url).send().await?.error_for_status()?;
            let body: Value = resp.json().await?;
            let data = body["data"]
                .as_array()
                .context("OKX 返回数据格式不含 data 数组")?;

            if data.is_empty() {
                break;
            }

            for row in data {
                let arr = match row.as_array() {
                    Some(a) if a.len() >= 6 => a,
                    _ => continue,
                };
                let ts = arr[0]
                    .as_str()
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(0);
                let open = parse_dec(&arr[1]);
                let high = parse_dec(&arr[2]);
                let low = parse_dec(&arr[3]);
                let close = parse_dec(&arr[4]);
                let volume = parse_dec(&arr[5]);
                let hour = (ts / 3_600_000) % 24;
                let funding_rate = if hour % 8 == 0 {
                    dec!(0.0001)
                } else {
                    Decimal::ZERO
                };

                all_candles.push(Candle {
                    timestamp: ts,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    funding_rate,
                });
            }

            let oldest = all_candles.iter().map(|c| c.timestamp).min().unwrap_or(0);
            after_ts = Some(oldest);

            if data.len() < 100 {
                break;
            }
        }

        all_candles.sort_by_key(|c| c.timestamp);
        all_candles.dedup_by_key(|c| c.timestamp);
        Ok(all_candles)
    }
}

/// 原始 K 线行（无资金费率占位、不落 JSON 缓存，供秒级 tick 固化与回放压测使用）
#[derive(Debug, Clone, Copy)]
pub struct RawKline {
    pub ts: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

impl DataManager {
    /// 从 Binance 兼容接口按 interval 拉取一页原始 K 线。
    /// startTime 正向翻页模式（升序返回），与 fetch_binance_compatible 的 endTime 回溯模式互不影响，
    /// 现有 1h 缓存接口行为零改动。
    pub async fn fetch_klines_page(
        &self,
        base_url: &str,
        symbol: &str,
        interval: &str,
        start_ts: i64,
        limit: usize,
    ) -> Result<Vec<RawKline>> {
        let url = format!(
            "{}?symbol={}&interval={}&startTime={}&limit={}",
            base_url,
            symbol,
            interval,
            start_ts,
            limit.min(1000)
        );
        let resp = self.client.get(&url).send().await?.error_for_status()?;
        let rows: Vec<Vec<Value>> = resp.json().await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            if row.len() < 6 {
                continue;
            }
            let ts = row[0].as_i64().unwrap_or(0);
            if ts == 0 {
                continue;
            }
            out.push(RawKline {
                ts,
                open: parse_f64(&row[1]),
                high: parse_f64(&row[2]),
                low: parse_f64(&row[3]),
                close: parse_f64(&row[4]),
                volume: parse_f64(&row[5]),
            });
        }
        Ok(out)
    }

    /// 拉取聚合逐笔成交一页（回放压测 Phase 4 亚秒级深钻用），升序返回 (ts, price, qty)
    pub async fn fetch_agg_trades_page(
        &self,
        base_url: &str,
        symbol: &str,
        start_ts: i64,
        limit: usize,
    ) -> Result<Vec<(i64, f64, f64)>> {
        let url = format!(
            "{}?symbol={}&startTime={}&limit={}",
            base_url,
            symbol,
            start_ts,
            limit.min(1000)
        );
        let resp = self.client.get(&url).send().await?.error_for_status()?;
        let rows: Vec<Value> = resp.json().await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let ts = row["T"].as_i64().unwrap_or(0);
            let price = row["p"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            let qty = row["q"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            if ts > 0 && price > 0.0 {
                out.push((ts, price, qty));
            }
        }
        Ok(out)
    }
}

fn parse_f64(v: &Value) -> f64 {
    if let Some(s) = v.as_str() {
        s.parse::<f64>().unwrap_or(0.0)
    } else {
        v.as_f64().unwrap_or(0.0)
    }
}

fn parse_dec(v: &Value) -> Decimal {
    if let Some(s) = v.as_str() {
        Decimal::from_str(s).unwrap_or(Decimal::ZERO)
    } else if let Some(f) = v.as_f64() {
        Decimal::from_str(&format!("{:.8}", f)).unwrap_or(Decimal::ZERO)
    } else {
        Decimal::ZERO
    }
}
