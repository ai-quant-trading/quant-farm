//! 策略农场：多策略并行账本 + 参数白名单提交 + 自动验证门槛 + 轻量 Web UI。
//!
//! 架构（书第 11/20 课的落地：策略即数据、影子部署、渐进推广）：
//! - `Book` = 泛化的影子引擎——任意策略模式、独立零资金账本、与生产同源决策链
//! - `StrategySpec` = 参数化模板提交（JSON）：模式 + 参数白名单 + 选币池，**不执行任意代码**
//! - `validate_spec` = 提交即验证：全样本回测 + 前半样本 + 成本×1.5 压力，不过门拒收
//! - `Farm` = 账本群 + 注册表（重启恢复）；`serve` = axum API + 单页排行榜
//!
//! 安全边界：所有 Book 零资金、独立熔断、绝不触碰生产订单路径；参数白名单外一律拒收。

use crate::backtest::BacktestEngine;
use crate::config::{AppConfig, StrategyMode};
use crate::data::DataManager;
use crate::db::{ApiQuoteRow, DbOrderRow, DbPositionRow, TradingDatabase};
use crate::monte_carlo::cost_stress_totals;
use crate::paper_engine::{LiveExitDecision, SqlitePaperEngine};
use crate::risk::RiskManager;
use crate::strategy::scan_entry_signals;
use crate::types::{Candle, MarketRegime, Side};
use anyhow::{bail, Context, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{info, warn};

// ============================================================================
// Book：泛化的影子引擎（任意模式、独立账本、生产同源决策链）
// ============================================================================

pub struct Book {
    pub name: String,
    pub cfg: AppConfig,
    pub db: TradingDatabase,
    risk_mgr: RiskManager,
    pub pending_entries: Vec<(String, Side, Decimal)>,
    seq: usize,
}

impl Book {
    pub fn new(name: &str, cfg: AppConfig, db_path: &str) -> Result<Self> {
        let db = TradingDatabase::open(db_path, cfg.account.initial_equity)?;
        let risk_mgr = RiskManager::new(
            cfg.account.initial_equity,
            cfg.risk.clone(),
            cfg.costs.clone(),
        );
        Ok(Self {
            name: name.to_string(),
            cfg,
            db,
            risk_mgr,
            pending_entries: Vec::new(),
            seq: 0,
        })
    }

    /// 整点指标刷新后：按本 Book 的模式重算候选信号
    pub fn on_metrics(
        &mut self,
        btc_candles: &[Candle],
        universe_candles: &HashMap<String, Vec<Candle>>,
        regime: MarketRegime,
    ) {
        // 只喂本 Book 选币池内的币（默认全池）
        let slices: HashMap<String, &[Candle]> = universe_candles
            .iter()
            .filter(|(k, v)| {
                !v.is_empty() && self.cfg.strategy.universe.iter().any(|u| u == *k)
            })
            .map(|(k, v)| {
                let start = v.len().saturating_sub(121);
                (k.clone(), &v[start..])
            })
            .collect();
        let signals = scan_entry_signals(regime, &slices, btc_candles, &self.cfg.strategy);
        self.pending_entries = signals
            .into_iter()
            .map(|s| (s.symbol, s.side, s.atr_val))
            .collect();
        if !self.pending_entries.is_empty() {
            info!(
                "[农场:{}] 候选信号 {} 个: {:?}",
                self.name,
                self.pending_entries.len(),
                self.pending_entries
                    .iter()
                    .map(|(s, _, _)| s.clone())
                    .collect::<Vec<_>>()
            );
        }
    }

    /// 每 10 秒一拍：盯市出场 + 开仓门控（与生产/影子完全同源）
    pub fn on_cycle(
        &mut self,
        quotes: &HashMap<String, ApiQuoteRow>,
        regime: MarketRegime,
        coin_metrics: &HashMap<String, (Decimal, Decimal, Decimal)>,
        now_ms: i64,
    ) -> Result<()> {
        let fee_rate = self.cfg.costs.taker_fee_rate;
        let now_str = Utc::now().to_rfc3339();
        let mut acct = self.db.get_account_state()?;
        let mut positions = self.db.list_positions()?;

        // ---- 出场 ----
        let mut total_upnl = Decimal::ZERO;
        let mut surviving: Vec<DbPositionRow> = Vec::new();
        for p in positions.drain(..) {
            let q = match quotes.get(&p.symbol) {
                Some(q) => q,
                None => {
                    surviving.push(p);
                    continue;
                }
            };
            let atr_opt = coin_metrics.get(&p.symbol).map(|&(_, a, _)| a);
            match SqlitePaperEngine::check_position_exit(
                p,
                q,
                regime,
                atr_opt,
                false,
                now_ms,
                &self.cfg.strategy,
            ) {
                LiveExitDecision::Hold(mut p2) => {
                    total_upnl += p2.unrealized_pnl;
                    p2.updated_at = now_str.clone();
                    self.db.upsert_position(&p2)?;
                    surviving.push(p2);
                }
                LiveExitDecision::Exit {
                    p: mut p2,
                    unrealized_pnl: upnl,
                    mark_exit_px,
                    reason,
                    ..
                } => {
                    p2.updated_at = now_str.clone();
                    let exit_notional = p2.qty * mark_exit_px;
                    let exit_fee = exit_notional * fee_rate;
                    let total_fees = p2.entry_fee + exit_fee;
                    let net = upnl - total_fees;
                    acct.cash_balance += upnl - exit_fee;
                    acct.total_fees_paid += exit_fee;
                    acct.total_trades += 1;
                    if net > Decimal::ZERO {
                        acct.win_trades += 1;
                    } else {
                        acct.loss_trades += 1;
                    }
                    self.seq += 1;
                    let exit_oid = format!("FARM_EXIT_{}_{}_{}", self.name, p2.symbol, self.seq);
                    let _ = self
                        .db
                        .update_order_status(&p2.stop_order_id, "TRIGGERED_OR_CANCELED");
                    self.db.insert_order(&DbOrderRow {
                        order_id: exit_oid.clone(),
                        symbol: p2.symbol.clone(),
                        side: if p2.side == "LONG" {
                            "SELL".to_string()
                        } else {
                            "BUY".to_string()
                        },
                        action: if p2.side == "LONG" {
                            "CLOSE_LONG".to_string()
                        } else {
                            "CLOSE_SHORT".to_string()
                        },
                        order_type: "FARM_MARKET".to_string(),
                        qty: p2.qty,
                        fill_price: mark_exit_px,
                        notional_usd: exit_notional,
                        fee_usd: exit_fee,
                        status: "FILLED".to_string(),
                        created_at: now_str.clone(),
                    })?;
                    let init_risk = (p2.entry_price - p2.initial_stop).abs() * p2.qty;
                    let r_mult = if init_risk > Decimal::ZERO {
                        net / init_risk
                    } else {
                        Decimal::ZERO
                    };
                    self.db.insert_closed_trade(
                        &p2.symbol,
                        &p2.side,
                        &p2.entry_order_id,
                        &exit_oid,
                        p2.qty,
                        p2.entry_price,
                        mark_exit_px,
                        upnl,
                        total_fees,
                        net,
                        r_mult,
                        &format!("[农场:{}] {}", self.name, reason),
                        acct.cash_balance,
                        &p2.opened_at,
                        &now_str,
                    )?;
                    self.db.remove_position(&p2.symbol)?;
                    info!(
                        "[农场:{}] 平仓 {} {} @ {} | {} | 净 {:+.3}U | 余额 {:.2}U",
                        self.name, p2.symbol, p2.side, mark_exit_px, reason, net, acct.cash_balance
                    );
                }
            }
        }

        // ---- 开仓（完整门控：冷却/点差/以损定仓/模式化风险缩放）----
        if !self.pending_entries.is_empty()
            && !matches!(
                self.risk_mgr.state,
                crate::risk::CircuitBreakerState::Halted
                    | crate::risk::CircuitBreakerState::DailyCooldown { .. }
            )
            && surviving.len() < self.cfg.risk.max_concurrent_positions
        {
            let decisions = SqlitePaperEngine::check_new_entries(
                &self.pending_entries,
                quotes,
                &surviving,
                acct.cash_balance + total_upnl,
                &self.risk_mgr,
                &self.cfg.strategy,
                |sym| self.db.is_symbol_in_cooldown(sym, 1800).unwrap_or(false),
                &HashMap::new(),
            );
            for d in decisions {
                let notional = d.approved_qty * d.fill_entry;
                let entry_fee = notional * fee_rate;
                acct.cash_balance -= entry_fee;
                acct.total_fees_paid += entry_fee;
                self.seq += 1;
                let entry_oid = format!("FARM_OPEN_{}_{}_{}", self.name, d.symbol, self.seq);
                let stop_oid = format!("FARM_STOP_{}_{}_{}", self.name, d.symbol, self.seq);
                let side_str = if d.side == Side::Long { "LONG" } else { "SHORT" };
                self.db.insert_order(&DbOrderRow {
                    order_id: entry_oid.clone(),
                    symbol: d.symbol.clone(),
                    side: if side_str == "LONG" {
                        "BUY".to_string()
                    } else {
                        "SELL".to_string()
                    },
                    action: if side_str == "LONG" {
                        "OPEN_LONG".to_string()
                    } else {
                        "OPEN_SHORT".to_string()
                    },
                    order_type: "FARM_MARKET".to_string(),
                    qty: d.approved_qty,
                    fill_price: d.fill_entry,
                    notional_usd: notional,
                    fee_usd: entry_fee,
                    status: "FILLED".to_string(),
                    created_at: now_str.clone(),
                })?;
                self.db.insert_order(&DbOrderRow {
                    order_id: stop_oid.clone(),
                    symbol: d.symbol.clone(),
                    side: if side_str == "LONG" {
                        "SELL".to_string()
                    } else {
                        "BUY".to_string()
                    },
                    action: "STOP_MARKET_BIND".to_string(),
                    order_type: "STOP_MARKET".to_string(),
                    qty: d.approved_qty,
                    fill_price: d.initial_stop,
                    notional_usd: d.approved_qty * d.initial_stop,
                    fee_usd: Decimal::ZERO,
                    status: "NEW_TRIGGER".to_string(),
                    created_at: now_str.clone(),
                })?;
                let pos = DbPositionRow {
                    symbol: d.symbol.clone(),
                    side: side_str.to_string(),
                    qty: d.approved_qty,
                    entry_price: d.fill_entry,
                    entry_order_id: entry_oid.clone(),
                    stop_order_id: stop_oid.clone(),
                    initial_stop: d.initial_stop,
                    active_stop: d.initial_stop,
                    take_profit: d.take_profit,
                    highest_mark: d.fill_entry,
                    lowest_mark: d.fill_entry,
                    last_mark_price: d.fill_entry,
                    unrealized_pnl: Decimal::ZERO,
                    entry_fee,
                    bars_held: 0,
                    opened_at: now_str.clone(),
                    updated_at: now_str.clone(),
                };
                self.db.upsert_position(&pos)?;
                surviving.push(pos);
                info!(
                    "[农场:{}] 开仓 {} {} @ {} (名义 {:.2}U) | 止损 {} | 止盈 {}",
                    self.name, d.symbol, side_str, d.fill_entry, notional, d.initial_stop, d.take_profit
                );
            }
            self.pending_entries.clear();
        }

        acct.unrealized_pnl = total_upnl;
        acct.total_equity = acct.cash_balance + total_upnl;
        if acct.total_equity > acct.peak_equity {
            acct.peak_equity = acct.total_equity;
        }
        acct.updated_at = now_str;
        self.db.update_account_state(&acct)?;
        self.risk_mgr.update_equity(now_ms, acct.total_equity);
        Ok(())
    }

    /// 结构化状态（排行榜用）
    pub fn status(&self) -> BookStatus {
        let (Ok(acct), Ok(trades), Ok(positions)) = (
            self.db.get_account_state(),
            self.db.list_recent_trades(10_000),
            self.db.list_positions(),
        ) else {
            return BookStatus {
                name: self.name.clone(),
                status: "error".to_string(),
                ..Default::default()
            };
        };
        let wins = trades.iter().filter(|t| t.net_pnl > Decimal::ZERO).count();
        let gross_win: Decimal = trades.iter().map(|t| t.net_pnl.max(Decimal::ZERO)).sum();
        let gross_loss: Decimal = trades.iter().map(|t| t.net_pnl.min(Decimal::ZERO)).sum();
        let pf = if gross_loss < Decimal::ZERO {
            gross_win / gross_loss.abs()
        } else if gross_win > Decimal::ZERO {
            dec!(999)
        } else {
            Decimal::ZERO
        };
        let dd = if acct.peak_equity > Decimal::ZERO {
            (acct.peak_equity - acct.total_equity) / acct.peak_equity * dec!(100)
        } else {
            Decimal::ZERO
        };
        BookStatus {
            name: self.name.clone(),
            status: "active".to_string(),
            equity: acct.total_equity,
            initial_equity: acct.initial_equity,
            return_pct: (acct.total_equity / acct.initial_equity - Decimal::ONE) * dec!(100),
            drawdown_pct: dd,
            trades: trades.len(),
            wins,
            win_rate_pct: if trades.is_empty() {
                Decimal::ZERO
            } else {
                Decimal::from(wins) * dec!(100) / Decimal::from(trades.len())
            },
            profit_factor: pf,
            open_positions: positions.len(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BookStatus {
    pub name: String,
    pub status: String,
    pub equity: Decimal,
    pub initial_equity: Decimal,
    pub return_pct: Decimal,
    pub drawdown_pct: Decimal,
    pub trades: usize,
    pub wins: usize,
    pub win_rate_pct: Decimal,
    pub profit_factor: Decimal,
    pub open_positions: usize,
}

// ============================================================================
// StrategySpec：参数化模板提交（白名单，不执行任意代码）
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategySpec {
    /// 唯一策略名 [a-z0-9_-]{1,32}
    pub name: String,
    pub author: String,
    /// 策略家族: long_only_breakout / bear_short / chop_mean_reversion / regime_router
    pub mode: String,
    /// 参数覆盖（白名单内的 StrategyConfig 字段）
    #[serde(default)]
    pub params: HashMap<String, serde_json::Value>,
    /// 选币池（必须 ⊆ 生产 universe；缺省 = 生产 universe）
    #[serde(default)]
    pub universe: Option<Vec<String>>,
    /// 账本虚拟本金（默认 100）
    #[serde(default = "default_spec_equity")]
    pub initial_equity: Decimal,
}

fn default_spec_equity() -> Decimal {
    dec!(100)
}

/// 白名单允许的参数键（防止任意配置注入；风控参数刻意不可覆盖——所有账本共享生产风控纪律）
pub const PARAM_ALLOWLIST: &[&str] = &[
    "top_k_momentum", "donchian_period", "atr_period", "volume_surge_mult", "max_bias_pct",
    "initial_stop_atr_mult", "chandelier_atr_mult", "breakeven_r_mult", "time_stop_bars",
    "time_stop_min_r", "regime_adx_threshold", "min_atr_percentile", "max_recent_move_bps",
    "chop_mr_range_min_pct", "chop_mr_range_max_pct", "chop_mr_stretch_atr", "chop_mr_stop_atr",
    "chop_mr_tp_atr", "chop_mr_time_stop_bars",
    "bear_stop_atr", "bear_tp_atr", "bear_maturity_bars", "bear_time_stop_bars",
    "bear_entry_variant", "bear_max_atr_percentile",
];

fn json_to_decimal(v: &serde_json::Value) -> Option<Decimal> {
    match v {
        serde_json::Value::Number(n) => Decimal::from_str(&n.to_string()).ok(),
        serde_json::Value::String(s) => Decimal::from_str(s).ok(),
        _ => None,
    }
}

fn json_to_usize(v: &serde_json::Value) -> Option<usize> {
    match v {
        serde_json::Value::Number(n) => n.as_u64().map(|x| x as usize),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// 把 spec 解析为可运行的 AppConfig（模式 + 白名单参数 + 选币池校验）
pub fn spec_to_config(base: &AppConfig, spec: &StrategySpec) -> Result<AppConfig> {
    if spec.name.is_empty()
        || spec.name.len() > 32
        || !spec.name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
    {
        bail!("策略名非法: {:?} (仅小写字母/数字/_/-, 最长32)", spec.name);
    }
    let mut cfg = base.clone();
    cfg.strategy.mode = match spec.mode.as_str() {
        "long_only_breakout" => StrategyMode::LongOnlyBreakout,
        "bear_short" => StrategyMode::BearShort,
        "chop_mean_reversion" => StrategyMode::ChopMeanReversion,
        "regime_router" => StrategyMode::RegimeRouter,
        other => bail!(
            "未知模式: {} (可选: long_only_breakout / bear_short / chop_mean_reversion / regime_router)",
            other
        ),
    };
    if let Some(u) = &spec.universe {
        if u.is_empty() {
            bail!("选币池不能为空");
        }
        for s in u {
            if !base.strategy.universe.contains(s) && s != &base.strategy.benchmark_symbol {
                bail!("币种 {} 不在生产选币池内 (行情源约束)", s);
            }
        }
        cfg.strategy.universe = u.clone();
    }
    let mut applied = Vec::new();
    for (k, v) in &spec.params {
        if !PARAM_ALLOWLIST.contains(&k.as_str()) {
            bail!(
                "未知参数: {} (白名单: {})",
                k,
                PARAM_ALLOWLIST.join(", ")
            );
        }
        let s = &mut cfg.strategy;
        let bad = || anyhow::anyhow!("参数 {} 的值类型非法: {:?}", k, v);
        match k.as_str() {
            "top_k_momentum" => s.top_k_momentum = json_to_usize(v).ok_or_else(bad)?,
            "donchian_period" => s.donchian_period = json_to_usize(v).ok_or_else(bad)?,
            "atr_period" => s.atr_period = json_to_usize(v).ok_or_else(bad)?,
            "time_stop_bars" => s.time_stop_bars = json_to_usize(v).ok_or_else(bad)?,
            "chop_mr_time_stop_bars" => s.chop_mr_time_stop_bars = json_to_usize(v).ok_or_else(bad)?,
            "bear_maturity_bars" => s.bear_maturity_bars = json_to_usize(v).ok_or_else(bad)?,
            "bear_time_stop_bars" => s.bear_time_stop_bars = json_to_usize(v).ok_or_else(bad)?,
            "bear_entry_variant" => match v.as_str() {
                Some(x @ ("breakdown" | "rejection" | "both")) => s.bear_entry_variant = x.to_string(),
                _ => return Err(bad()),
            },
            _ => {
                // Decimal 类型参数
                let d = json_to_decimal(v).ok_or_else(bad)?;
                match k.as_str() {
                    "volume_surge_mult" => s.volume_surge_mult = d,
                    "max_bias_pct" => s.max_bias_pct = d,
                    "initial_stop_atr_mult" => s.initial_stop_atr_mult = d,
                    "chandelier_atr_mult" => s.chandelier_atr_mult = d,
                    "breakeven_r_mult" => s.breakeven_r_mult = d,
                    "time_stop_min_r" => s.time_stop_min_r = d,
                    "regime_adx_threshold" => s.regime_adx_threshold = d,
                    "min_atr_percentile" => s.min_atr_percentile = d,
                    "max_recent_move_bps" => s.max_recent_move_bps = d,
                    "chop_mr_range_min_pct" => s.chop_mr_range_min_pct = d,
                    "chop_mr_range_max_pct" => s.chop_mr_range_max_pct = d,
                    "chop_mr_stretch_atr" => s.chop_mr_stretch_atr = d,
                    "chop_mr_stop_atr" => s.chop_mr_stop_atr = d,
                    "chop_mr_tp_atr" => s.chop_mr_tp_atr = d,
                    "bear_stop_atr" => s.bear_stop_atr = d,
                    "bear_tp_atr" => s.bear_tp_atr = d,
                    "bear_max_atr_percentile" => s.bear_max_atr_percentile = d,
                    _ => unreachable!(),
                }
            }
        }
        applied.push(k.clone());
    }
    let _ = applied;
    cfg.account.initial_equity = spec.initial_equity.max(dec!(100));
    cfg.validate()?;
    Ok(cfg)
}

// ============================================================================
// 验证门：提交即验证（全样本 + 前半样本 + 成本×1.5）
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationReport {
    pub spec_name: String,
    pub pass: bool,
    pub reasons: Vec<String>,
    pub full_return_pct: Decimal,
    pub full_pf: Decimal,
    pub full_trades: usize,
    pub half_return_pct: Decimal,
    pub half_pf: Decimal,
    pub cost15_equity: Decimal,
    pub validated_at: String,
}

/// 验证门槛：全样本 PF>1.1 且净利>0 且 ≥10 笔；前半样本净利>0；成本×1.5 仍>0
pub async fn validate_spec(
    base: &AppConfig,
    spec: &StrategySpec,
    data_mgr: &DataManager,
    bars: usize,
) -> Result<ValidationReport> {
    let cfg = spec_to_config(base, spec)?;
    let mut symbols = vec![cfg.strategy.benchmark_symbol.clone()];
    for s in &cfg.strategy.universe {
        if !symbols.contains(s) {
            symbols.push(s.clone());
        }
    }
    let mut all: HashMap<String, Vec<Candle>> = HashMap::new();
    for sym in symbols {
        let candles = data_mgr.load_or_fetch_candles(&sym, bars, false).await?;
        all.insert(sym, candles);
    }

    // 全样本
    let full = BacktestEngine::new(cfg.clone()).run(&all)?;
    // 前半样本（去掉最近一半）
    let mut half_all: HashMap<String, Vec<Candle>> = all
        .iter()
        .map(|(k, v)| {
            let keep = v.len().saturating_sub(v.len() / 2);
            (k.clone(), v[..keep.max(400)].to_vec())
        })
        .collect();
    let half = BacktestEngine::new(cfg.clone()).run(&half_all)?;
    half_all.clear();
    // 成本×1.5 压力（复用 Monte Carlo 的重结算）
    let costs = cost_stress_totals(&full.trades, full.initial_equity, &[dec!(1.5)]);
    let cost15 = costs.first().map(|(_, e)| *e).unwrap_or(full.initial_equity);

    let mut reasons = Vec::new();
    let mut pass = true;
    if full.trades.len() < 10 {
        pass = false;
        reasons.push(format!("交易数 {} < 10（样本不足）", full.trades.len()));
    }
    if full.profit_factor <= dec!(1.1) {
        pass = false;
        reasons.push(format!("全样本 PF {:.2} <= 1.1", full.profit_factor));
    }
    if full.final_equity <= full.initial_equity {
        pass = false;
        reasons.push(format!("全样本净利 {:.2} <= 0", full.final_equity - full.initial_equity));
    }
    if half.final_equity <= half.initial_equity {
        pass = false;
        reasons.push(format!("前半样本净利 {:.2} <= 0（边际不稳定）", half.final_equity - half.initial_equity));
    }
    if cost15 <= full.initial_equity {
        pass = false;
        reasons.push("成本×1.5 后净利翻负（成本敏感）".to_string());
    }
    if pass {
        reasons.push("全部门槛通过".to_string());
    }

    Ok(ValidationReport {
        spec_name: spec.name.clone(),
        pass,
        reasons,
        full_return_pct: full.total_return_pct,
        full_pf: full.profit_factor,
        full_trades: full.trades.len(),
        half_return_pct: half.total_return_pct,
        half_pf: half.profit_factor,
        cost15_equity: cost15,
        validated_at: Utc::now().to_rfc3339(),
    })
}

// ============================================================================
// Farm：账本群 + 注册表（重启恢复）
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredSpec {
    pub spec: StrategySpec,
    /// active / rejected / archived
    pub status: String,
    pub validation: Option<ValidationReport>,
    pub submitted_at: String,
}

pub struct Farm {
    pub base_cfg: AppConfig,
    pub data_mgr: DataManager,
    pub books: HashMap<String, Book>,
    pub registry: Vec<RegisteredSpec>,
    dir: PathBuf,
}

impl Farm {
    pub fn load(base_cfg: &AppConfig, data_mgr: &DataManager, dir: &str) -> Result<Self> {
        let dir = PathBuf::from(dir);
        std::fs::create_dir_all(dir.join("books"))?;
        let mut farm = Farm {
            base_cfg: base_cfg.clone(),
            data_mgr: data_mgr.clone(),
            books: HashMap::new(),
            registry: Vec::new(),
            dir: dir.clone(),
        };
        let reg_path = dir.join("registry.json");
        if reg_path.exists() {
            match serde_json::from_str::<Vec<RegisteredSpec>>(&std::fs::read_to_string(&reg_path)?) {
                Ok(reg) => {
                    for r in reg {
                        if r.status == "active" {
                            match spec_to_config(&farm.base_cfg, &r.spec)
                                .map_err(|e| e.to_string())
                                .and_then(|cfg| {
                                    let db = farm
                                        .dir
                                        .join("books")
                                        .join(format!("{}.db", r.spec.name))
                                        .to_str()
                                        .unwrap_or_default()
                                        .to_string();
                                    Book::new(&r.spec.name, cfg, &db).map_err(|e| e.to_string())
                                }) {
                                Ok(book) => {
                                    farm.books.insert(r.spec.name.clone(), book);
                                }
                                Err(e) => warn!("[农场] 恢复 {} 失败: {}", r.spec.name, e),
                            }
                        }
                        farm.registry.push(r);
                    }
                    info!("[农场] 注册表恢复: {} 条, 活跃账本 {} 个", farm.registry.len(), farm.books.len());
                }
                Err(e) => warn!("[农场] 注册表损坏 ({}), 从空农场启动", e),
            }
        }
        Ok(farm)
    }

    fn persist(&self) -> Result<()> {
        let path = self.dir.join("registry.json");
        std::fs::write(&path, serde_json::to_string_pretty(&self.registry)?)?;
        Ok(())
    }

    /// 提交（含验证门）：PASS 开账本，FAIL 登记 rejected
    pub async fn submit(&mut self, spec: StrategySpec) -> Result<ValidationReport> {
        if self.registry.iter().any(|r| r.spec.name == spec.name) {
            bail!("策略名已存在: {}（先 DELETE 再重新提交）", spec.name);
        }
        // 先做静态解析（快速失败），再跑验证（~10-30s）
        spec_to_config(&self.base_cfg, &spec)?;
        let report = validate_spec(&self.base_cfg, &spec, &self.data_mgr, 9000).await?;
        let status = if report.pass { "active" } else { "rejected" };
        if report.pass {
            let cfg = spec_to_config(&self.base_cfg, &spec)?;
            let db = self
                .dir
                .join("books")
                .join(format!("{}.db", spec.name))
                .to_str()
                .unwrap_or_default()
                .to_string();
            let book = Book::new(&spec.name, cfg, &db)?;
            self.books.insert(spec.name.clone(), book);
        }
        self.registry.push(RegisteredSpec {
            spec,
            status: status.to_string(),
            validation: Some(report.clone()),
            submitted_at: Utc::now().to_rfc3339(),
        });
        self.persist()?;
        Ok(report)
    }

    /// 下线归档
    pub fn remove(&mut self, name: &str) -> Result<()> {
        if let Some(r) = self.registry.iter_mut().find(|r| r.spec.name == name) {
            r.status = "archived".to_string();
        }
        self.books.remove(name);
        self.persist()?;
        Ok(())
    }

    pub fn on_metrics(
        &mut self,
        btc: &[Candle],
        universe: &HashMap<String, Vec<Candle>>,
        regime: MarketRegime,
    ) {
        for book in self.books.values_mut() {
            book.on_metrics(btc, universe, regime);
        }
    }

    pub fn on_cycle(
        &mut self,
        quotes: &HashMap<String, ApiQuoteRow>,
        regime: MarketRegime,
        coin_metrics: &HashMap<String, (Decimal, Decimal, Decimal)>,
        now_ms: i64,
    ) {
        for book in self.books.values_mut() {
            if let Err(e) = book.on_cycle(quotes, regime, coin_metrics, now_ms) {
                warn!("[农场:{}] 巡检失败: {}", book.name, e);
            }
        }
    }

    /// 排行榜：活跃账本实时状态 + 被拒/归档条目
    pub fn leaderboard(&self) -> Vec<serde_json::Value> {
        let mut rows = Vec::new();
        for r in &self.registry {
            if r.status == "active" {
                if let Some(book) = self.books.get(&r.spec.name) {
                    let mut st = serde_json::to_value(book.status()).unwrap_or_default();
                    st["author"] = serde_json::Value::String(r.spec.author.clone());
                    st["mode"] = serde_json::Value::String(r.spec.mode.clone());
                    rows.push(st);
                }
            } else {
                rows.push(serde_json::json!({
                    "name": r.spec.name,
                    "status": r.status,
                    "author": r.spec.author,
                    "mode": r.spec.mode,
                    "reasons": r.validation.as_ref().map(|v| v.reasons.clone()),
                }));
            }
        }
        rows
    }
}

// ============================================================================
// axum Web API + 单页 UI
// ============================================================================

use axum::{extract::State, http::StatusCode, response::Html, routing::{delete, get, post}, Json, Router};

pub async fn serve(farm: Arc<tokio::sync::Mutex<Farm>>, port: u16) -> Result<()> {
    let app = Router::new()
        .route("/", get(index_page))
        .route("/api/strategies", get(list_strategies).post(submit_strategy))
        .route("/api/strategies/:name", delete(remove_strategy))
        .with_state(farm);
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .with_context(|| format!("端口 {} 绑定失败", port))?;
    info!("[农场] Web UI: http://127.0.0.1:{}/", port);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn list_strategies(State(farm): State<Arc<tokio::sync::Mutex<Farm>>>) -> Json<serde_json::Value> {
    let f = farm.lock().await;
    Json(serde_json::json!({ "strategies": f.leaderboard() }))
}

async fn submit_strategy(
    State(farm): State<Arc<tokio::sync::Mutex<Farm>>>,
    Json(spec): Json<StrategySpec>,
) -> Result<(StatusCode, Json<ValidationReport>), (StatusCode, String)> {
    let mut f = farm.lock().await;
    let report = f.submit(spec).await.map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            format!("提交失败: {}", e),
        )
    })?;
    // 注: 验证含 ~10-30s 回测，持锁期间 UI 只读接口会排队——v1 可接受（提交是低频操作）
    let code = if report.pass { StatusCode::CREATED } else { StatusCode::UNPROCESSABLE_ENTITY };
    Ok((code, Json(report)))
}

async fn remove_strategy(
    State(farm): State<Arc<tokio::sync::Mutex<Farm>>>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let mut f = farm.lock().await;
    f.remove(&name).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok(Json(serde_json::json!({ "removed": name })))
}

async fn index_page() -> Html<&'static str> {
    Html(INDEX_HTML)
}

const INDEX_HTML: &str = r#"<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Quant Farm · 策略农场</title>
<style>
:root {
  --bg: #070b14; --bg2: #0c1220;
  --card: rgba(148,163,184,.05); --card-hover: rgba(148,163,184,.09);
  --border: rgba(148,163,184,.13); --border-hi: rgba(148,163,184,.25);
  --text: #e6edf7; --muted: #8b98ad; --dim: #5b6a80;
  --blue: #3b82f6; --violet: #8b5cf6; --cyan: #22d3ee;
  --green: #34d399; --red: #f87171; --amber: #fbbf24;
  --grad: linear-gradient(135deg,#3b82f6,#8b5cf6);
  --grad-soft: linear-gradient(135deg,rgba(59,130,246,.12),rgba(139,92,246,.12));
}
* { box-sizing: border-box; margin: 0; }
html { background: var(--bg); }
body {
  font-family: "Inter","SF Pro Display","PingFang SC",-apple-system,sans-serif;
  background:
    radial-gradient(1200px 500px at 80% -10%, rgba(59,130,246,.10), transparent 60%),
    radial-gradient(900px 400px at 10% 0%, rgba(139,92,246,.08), transparent 55%),
    var(--bg);
  color: var(--text); min-height: 100vh; padding: 0 0 60px;
}
.wrap { max-width: 1280px; margin: 0 auto; padding: 0 28px; }

/* ---------- header ---------- */
header { border-bottom: 1px solid var(--border); backdrop-filter: blur(10px); position: sticky; top: 0; z-index: 10; background: rgba(7,11,20,.75); }
.nav { display: flex; align-items: center; gap: 14px; padding: 16px 0; }
.logo { width: 34px; height: 34px; border-radius: 10px; background: var(--grad); display: flex; align-items: center; justify-content: center; font-size: 17px; box-shadow: 0 4px 18px rgba(99,102,241,.35); }
.brand { font-size: 16px; font-weight: 700; letter-spacing: .2px; }
.brand small { display: block; font-size: 11px; color: var(--muted); font-weight: 500; letter-spacing: .5px; }
.live { margin-left: auto; display: flex; align-items: center; gap: 8px; font-size: 12px; color: var(--muted); background: var(--card); border: 1px solid var(--border); padding: 6px 14px; border-radius: 99px; }
.live .dot { width: 8px; height: 8px; border-radius: 50%; background: var(--green); box-shadow: 0 0 0 0 rgba(52,211,153,.6); animation: pulse 2s infinite; }
@keyframes pulse { 70% { box-shadow: 0 0 0 8px rgba(52,211,153,0); } 100% { box-shadow: 0 0 0 0 rgba(52,211,153,0); } }

/* ---------- stats ---------- */
.stats { display: grid; grid-template-columns: repeat(auto-fit,minmax(200px,1fr)); gap: 14px; margin: 26px 0 8px; }
.stat { background: var(--card); border: 1px solid var(--border); border-radius: 16px; padding: 18px 20px; position: relative; overflow: hidden; transition: border-color .2s; }
.stat:hover { border-color: var(--border-hi); }
.stat .k { font-size: 12px; color: var(--muted); letter-spacing: .4px; }
.stat .v { font-size: 26px; font-weight: 700; margin-top: 6px; font-variant-numeric: tabular-nums; }
.stat .v.pos { color: var(--green); } .stat .v.neg { color: var(--red); }
.stat .glow { position: absolute; right: -20px; top: -20px; width: 90px; height: 90px; border-radius: 50%; background: var(--grad); opacity: .12; filter: blur(20px); }

/* ---------- section titles ---------- */
h2.sec { font-size: 14px; font-weight: 600; color: var(--muted); letter-spacing: 1.2px; text-transform: uppercase; margin: 34px 0 14px; display: flex; align-items: center; gap: 10px; }
h2.sec::after { content: ""; flex: 1; height: 1px; background: var(--border); }
h2.sec .num { background: var(--grad); color: white; width: 20px; height: 20px; border-radius: 6px; display: inline-flex; align-items: center; justify-content: center; font-size: 11px; }

/* ---------- templates ---------- */
.tpls { display: grid; grid-template-columns: repeat(auto-fill,minmax(280px,1fr)); gap: 12px; }
.tpl { background: var(--card); border: 1px solid var(--border); border-radius: 14px; padding: 16px 18px; cursor: pointer; transition: all .18s ease; position: relative; }
.tpl:hover { border-color: rgba(99,102,241,.55); background: var(--card-hover); transform: translateY(-2px); box-shadow: 0 8px 24px rgba(0,0,0,.35); }
.tpl .t { font-weight: 650; font-size: 14px; display: flex; align-items: center; gap: 8px; }
.tpl .d { font-size: 12px; color: var(--muted); margin-top: 6px; line-height: 1.55; }
.tpl .tag { font-size: 10px; padding: 2px 8px; border-radius: 99px; font-weight: 600; }
.tag.ok { background: rgba(52,211,153,.14); color: var(--green); }
.tag.warn { background: rgba(251,191,36,.14); color: var(--amber); }
.tpl .fill { position: absolute; right: 14px; bottom: 12px; font-size: 11px; color: var(--dim); opacity: 0; transition: opacity .15s; }
.tpl:hover .fill { opacity: 1; color: var(--blue); }

/* ---------- form card ---------- */
.grid2 { display: grid; grid-template-columns: 420px 1fr; gap: 18px; align-items: start; }
@media (max-width: 980px) { .grid2 { grid-template-columns: 1fr; } }
.card { background: var(--card); border: 1px solid var(--border); border-radius: 16px; padding: 20px 22px; }
.row { display: flex; gap: 10px; margin-bottom: 12px; flex-wrap: wrap; }
.field { flex: 1; min-width: 110px; }
.field label { display: block; font-size: 11px; color: var(--muted); margin-bottom: 5px; letter-spacing: .3px; }
input, select, textarea {
  width: 100%; background: rgba(2,6,17,.6); color: var(--text); border: 1px solid var(--border);
  padding: 9px 12px; border-radius: 10px; font-size: 13px; outline: none; transition: border-color .15s, box-shadow .15s;
  font-family: inherit;
}
input:focus, select:focus, textarea:focus { border-color: var(--blue); box-shadow: 0 0 0 3px rgba(59,130,246,.15); }
textarea { font-family: "SF Mono",ui-monospace,Menlo,monospace; font-size: 12px; height: 168px; resize: vertical; line-height: 1.6; }
.btn { background: var(--grad); color: white; border: none; padding: 10px 22px; border-radius: 10px; cursor: pointer; font-size: 13px; font-weight: 600; transition: all .15s; box-shadow: 0 4px 16px rgba(99,102,241,.3); }
.btn:hover:not(:disabled) { transform: translateY(-1px); box-shadow: 0 6px 22px rgba(99,102,241,.45); }
.btn:disabled { opacity: .55; cursor: wait; }
.btn.ghost { background: var(--card); border: 1px solid var(--border); box-shadow: none; font-weight: 500; }
.btn.ghost:hover { border-color: var(--border-hi); box-shadow: none; }
.spin { display: inline-block; width: 12px; height: 12px; border: 2px solid rgba(255,255,255,.35); border-top-color: white; border-radius: 50%; animation: rot .7s linear infinite; vertical-align: -2px; margin-right: 6px; }
@keyframes rot { to { transform: rotate(360deg); } }
#msg { margin-top: 12px; font-size: 12.5px; line-height: 1.6; border-radius: 10px; padding: 0; transition: all .2s; white-space: pre-wrap; }
#msg.show { padding: 12px 14px; border: 1px solid var(--border); background: rgba(2,6,17,.5); }
#msg.ok { border-color: rgba(52,211,153,.4); color: var(--green); }
#msg.err { border-color: rgba(248,113,113,.4); color: var(--red); }

/* ---------- leaderboard ---------- */
.board-card { background: var(--card); border: 1px solid var(--border); border-radius: 16px; overflow: hidden; }
table { border-collapse: collapse; width: 100%; font-size: 13px; }
thead th { text-align: left; font-size: 11px; color: var(--muted); font-weight: 600; letter-spacing: .5px; padding: 12px 14px; border-bottom: 1px solid var(--border); background: rgba(2,6,17,.35); position: sticky; top: 0; }
tbody td { padding: 11px 14px; border-bottom: 1px solid rgba(148,163,184,.07); font-variant-numeric: tabular-nums; }
tbody tr { transition: background .12s; }
tbody tr:hover { background: rgba(148,163,184,.05); }
tbody tr:last-child td { border-bottom: none; }
.pill { font-size: 11px; font-weight: 600; padding: 3px 10px; border-radius: 99px; display: inline-flex; align-items: center; gap: 5px; }
.pill::before { content: ""; width: 6px; height: 6px; border-radius: 50%; }
.pill.active { background: rgba(52,211,153,.13); color: var(--green); } .pill.active::before { background: var(--green); }
.pill.rejected { background: rgba(248,113,113,.13); color: var(--red); } .pill.rejected::before { background: var(--red); }
.pill.archived { background: rgba(148,163,184,.13); color: var(--muted); } .pill.archived::before { background: var(--muted); }
.pos { color: var(--green); } .neg { color: var(--red); }
.mono { font-family: "SF Mono",ui-monospace,Menlo,monospace; font-size: 12px; }
.name-cell { font-weight: 650; }
.name-cell small { display: block; color: var(--dim); font-weight: 400; font-size: 11px; margin-top: 2px; }
.bar { height: 4px; border-radius: 4px; background: rgba(148,163,184,.15); margin-top: 5px; overflow: hidden; width: 80px; }
.bar > i { display: block; height: 100%; border-radius: 4px; }
.reasons { max-width: 260px; font-size: 11.5px; color: var(--muted); line-height: 1.5; }
.icon-btn { background: none; border: 1px solid var(--border); color: var(--muted); border-radius: 8px; padding: 4px 10px; cursor: pointer; font-size: 11px; transition: all .15s; }
.icon-btn:hover { color: var(--red); border-color: rgba(248,113,113,.5); }
.empty { padding: 46px 20px; text-align: center; color: var(--dim); font-size: 13px; }
.empty .big { font-size: 34px; margin-bottom: 10px; opacity: .5; }

details.help { margin-top: 14px; border: 1px solid var(--border); border-radius: 14px; background: var(--card); overflow: hidden; }
details.help summary { cursor: pointer; padding: 13px 18px; font-size: 13px; color: var(--muted); list-style: none; display: flex; align-items: center; gap: 8px; }
details.help summary::before { content: "▸"; transition: transform .15s; color: var(--blue); }
details.help[open] summary::before { transform: rotate(90deg); }
details.help .inner { padding: 0 18px 16px; font-size: 12.5px; }
details.help table { font-size: 12px; }
details.help code { background: rgba(59,130,246,.12); color: #93c5fd; padding: 1px 6px; border-radius: 5px; font-size: 11px; font-family: "SF Mono",ui-monospace,Menlo,monospace; }
.legend { font-size: 11.5px; color: var(--dim); margin-top: 10px; }
footer { text-align: center; margin-top: 46px; color: var(--dim); font-size: 11.5px; }
footer a { color: var(--muted); text-decoration: none; }
footer a:hover { color: var(--blue); }
.toast { position: fixed; bottom: 24px; right: 24px; background: #131a2b; border: 1px solid var(--border-hi); border-radius: 12px; padding: 12px 18px; font-size: 13px; box-shadow: 0 10px 34px rgba(0,0,0,.5); opacity: 0; transform: translateY(8px); transition: all .25s; pointer-events: none; }
.toast.show { opacity: 1; transform: translateY(0); }
</style>
</head>
<body>

<header><div class="wrap"><div class="nav">
  <div class="logo">🌾</div>
  <div class="brand">Quant Farm<small>STRATEGY FARM · PROOF OF ALPHA</small></div>
  <div class="live"><span class="dot"></span>实时运行中 · 每 10s 刷新</div>
</div></div></header>

<div class="wrap">

<div class="stats">
  <div class="stat"><div class="glow"></div><div class="k">提交策略总数</div><div class="v" id="st-total">—</div></div>
  <div class="stat"><div class="k">活跃账本</div><div class="v" id="st-active">—</div></div>
  <div class="stat"><div class="k">最佳收益</div><div class="v pos" id="st-best">—</div></div>
  <div class="stat"><div class="k">最差收益</div><div class="v neg" id="st-worst">—</div></div>
</div>

<h2 class="sec"><span class="num">1</span>选择模板 · 一键填充</h2>
<div class="tpls" id="tpls"></div>

<h2 class="sec"><span class="num">2</span>提交策略</h2>
<div class="grid2">
  <div class="card">
    <div class="row">
      <div class="field"><label>策略名（唯一 · a-z0-9_-）</label><input id="name" placeholder="my_bear_v2"></div>
      <div class="field" style="max-width:110px"><label>作者</label><input id="author" placeholder="名字"></div>
    </div>
    <div class="row">
      <div class="field"><label>策略家族</label>
      <select id="mode">
        <option value="bear_short">🐻 bear_short · 熊市空头</option>
        <option value="long_only_breakout">🐂 long_only · 牛市突破</option>
        <option value="chop_mean_reversion">🔁 chop_mr · 震荡回归</option>
        <option value="regime_router">🧭 regime_router · 状态路由</option>
      </select></div>
    </div>
    <div class="field"><label>参数 JSON（白名单见下方速查）</label>
    <textarea id="params" spellcheck="false">{
  "bear_stop_atr": "2.5",
  "bear_tp_atr": "4.0",
  "bear_maturity_bars": 24
}</textarea></div>
    <div style="display:flex;gap:8px;margin-top:12px;flex-wrap:wrap">
      <button class="btn" id="submitBtn" onclick="submitStrategy()">🚀 提交并验证</button>
    </div>
    <div id="msg"></div>
  </div>

  <div>
    <div class="board-card" id="board-wrap">
      <table id="board">
        <thead><tr>
          <th>策略</th><th>状态</th><th style="text-align:right">净值</th><th style="text-align:right">收益</th>
          <th style="text-align:right">回撤</th><th style="text-align:right">闭环</th><th style="text-align:right">胜率</th>
          <th style="text-align:right">PF</th><th style="text-align:right">持仓</th><th>验证门备注</th><th></th>
        </tr></thead>
        <tbody></tbody>
      </table>
      <div class="empty" id="empty" style="display:none"><div class="big">🌱</div>还没有策略——选个模板提交第一个吧</div>
    </div>
    <p class="legend">💡 通过验证门的策略以<b>零资金账本</b>与生产并行；攒满 20 笔闭环且 PF&gt;1.1 才具备转正评估资格。所有账本共享生产风控纪律（单笔风险/熔断不可覆盖）。</p>
  </div>
</div>

<details class="help">
<summary>📐 参数速查 · 25 个白名单旋钮</summary>
<div class="inner">
<table>
<tr><td><code>top_k_momentum</code></td><td>动量选币取前 N</td><td>1-3</td><td><code>donchian_period</code></td><td>突破通道回看(小时)</td><td>16-30</td></tr>
<tr><td><code>volume_surge_mult</code></td><td>放量确认倍数</td><td>1.1-1.6</td><td><code>atr_period</code></td><td>ATR 周期</td><td>10-20</td></tr>
<tr><td><code>max_bias_pct</code></td><td>乖离上限防追高</td><td>0.05-0.10</td><td><code>regime_adx_threshold</code></td><td>趋势确认门槛</td><td>18-25</td></tr>
<tr><td><code>initial_stop_atr_mult</code></td><td>多头止损 ×ATR</td><td>1.8-2.8</td><td><code>chandelier_atr_mult</code></td><td>吊灯止盈 ×ATR</td><td>2.5-4.0</td></tr>
<tr><td><code>breakeven_r_mult</code></td><td>N R 保本移损</td><td>1.0-2.0</td><td><code>time_stop_bars</code></td><td>多头时间止损</td><td>10-24</td></tr>
<tr><td><code>bear_stop_atr</code></td><td>空头止损（宽于多头）</td><td>2.0-3.5</td><td><code>bear_tp_atr</code></td><td>空头止盈 TP&gt;SL</td><td>3.0-5.0</td></tr>
<tr><td><code>bear_maturity_bars</code></td><td>熊市确认小时数</td><td>12-48</td><td><code>bear_time_stop_bars</code></td><td>空头时间止损</td><td>8-16</td></tr>
<tr><td><code>bear_entry_variant</code></td><td>breakdown/rejection/both</td><td>both</td><td><code>bear_max_atr_percentile</code></td><td>危机护栏分位</td><td>90-99</td></tr>
<tr><td><code>min_atr_percentile</code></td><td>波动过滤(0=关)</td><td>0 或 70+</td><td><code>max_recent_move_bps</code></td><td>波动闸门 bps</td><td>30</td></tr>
<tr><td><code>chop_mr_*</code></td><td>震荡回归系列参数</td><td>见文档</td><td colspan="3" style="color:var(--dim)">完整说明：<code>docs/strategy-guide.md</code></td></tr>
</table>
</div>
</details>

<details class="help">
<summary>⚠️ 验证门判例 · 提交前自查（全部实测）</summary>
<div class="inner">
<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:10px">
<div>🚫 <b>去掉放量确认</b>（volume_surge 1.0）→ 414 笔亏 16%，假突破磨损</div>
<div>🚫 <b>止盈贴太近</b>（0.5×ATR）→ 0.2% 双边成本吃掉全部利润</div>
<div>🚫 <b>只对近期调参</b> → 前半样本门直接拒（两段都得正）</div>
<div>🚫 <b>交易 &lt;10 笔</b> → 样本不足拒收</div>
</div>
</div>
</details>

<footer>Quant Farm · <a href="https://github.com/ai-quant-trading/quant-farm" target="_blank">github.com/ai-quant-trading/quant-farm</a> · 策略必须证明自己的 Alpha，才配动用资金</footer>
</div>

<div class="toast" id="toast"></div>

<script>
const TEMPLATES = [
  {icon:"🐻", name:"空头 · 网格最优", quality:"ok", qtext:"实测通过", desc:"验证门实测通过的最优空头参数（全样本 +5.8% · PF 1.17 · 前半样本为正）", mode:"bear_short",
   params:{bear_stop_atr:"2.5", bear_tp_atr:"4.0", bear_maturity_bars:24, bear_time_stop_bars:10}},
  {icon:"🐻", name:"空头 · 只做压破", quality:"ok", qtext:"变体", desc:"只在放量跌破 20 期低点时做空——顺势追跌，不接反弹", mode:"bear_short",
   params:{bear_entry_variant:"breakdown", bear_stop_atr:"2.5", bear_tp_atr:"4.0"}},
  {icon:"🐻", name:"空头 · 只做衰竭", quality:"ok", qtext:"变体", desc:"只在反弹至均线附近阴线拒绝时做空——不追跌，等反弹死", mode:"bear_short",
   params:{bear_entry_variant:"rejection", bear_stop_atr:"3.0", bear_tp_atr:"4.0"}},
  {icon:"🐂", name:"多头 · 冠军原版", quality:"ok", qtext:"现役冠军", desc:"12 个月 +34% 的现役冠军参数——改个名字即可提交对照账本", mode:"long_only_breakout",
   params:{donchian_period:20, initial_stop_atr_mult:"2.2", chandelier_atr_mult:"3.0", time_stop_bars:16}},
  {icon:"🧭", name:"路由 · 全天候", quality:"ok", qtext:"组合", desc:"牛做多 / 熊做空自动切换（回测 +43% · 回撤 13.9%）", mode:"regime_router",
   params:{bear_stop_atr:"3.0", bear_tp_atr:"4.0"}},
  {icon:"🔁", name:"震荡 · 实验用", quality:"warn", qtext:"大概率被拒", desc:"历史 4 组参数全负——除非你有新想法，提交它是给门送人头", mode:"chop_mean_reversion",
   params:{chop_mr_stretch_atr:"2.0", chop_mr_tp_atr:"0.8"}},
];

function renderTemplates() {
  const box = document.getElementById('tpls');
  TEMPLATES.forEach((t, i) => {
    const d = document.createElement('div');
    d.className = 'tpl';
    d.innerHTML = `<div class="t">${t.icon} ${t.name} <span class="tag ${t.quality}">${t.qtext}</span></div>
      <div class="d">${t.desc}</div><div class="fill">点击填充 →</div>`;
    d.onclick = () => {
      document.getElementById('name').value = t.mode.slice(0,4) + '_' + i + '_' + Math.random().toString(36).slice(2,6);
      document.getElementById('mode').value = t.mode;
      document.getElementById('params').value = JSON.stringify(t.params, null, 2);
      toast('已填充「' + t.name + '」— 改个名字提交');
      document.getElementById('name').focus();
      document.getElementById('name').select();
    };
    box.appendChild(d);
  });
}

function toast(text) {
  const t = document.getElementById('toast');
  t.textContent = text; t.classList.add('show');
  clearTimeout(t._h); t._h = setTimeout(() => t.classList.remove('show'), 2600);
}

function setMsg(text, cls) {
  const m = document.getElementById('msg');
  m.textContent = text;
  m.className = cls ? 'show ' + cls : 'show';
}

async function refresh() {
  try {
    const r = await fetch('/api/strategies');
    const d = await r.json();
    const rows = d.strategies || [];
    const active = rows.filter(s => s.status === 'active');
    document.getElementById('st-total').textContent = rows.length;
    document.getElementById('st-active').textContent = active.length;
    const rets = active.map(s => +s.return_pct || 0);
    document.getElementById('st-best').textContent = rets.length ? Math.max(...rets).toFixed(2) + '%' : '—';
    document.getElementById('st-worst').textContent = rets.length ? Math.min(...rets).toFixed(2) + '%' : '—';
    const tb = document.querySelector('#board tbody');
    tb.innerHTML = '';
    document.getElementById('empty').style.display = rows.length ? 'none' : 'block';
    rows.sort((a,b)=>((+b.return_pct||-999)-(+a.return_pct||-999))).forEach(s => {
      const ret = s.return_pct !== undefined ? +s.return_pct : null;
      const retCls = ret === null ? '' : (ret >= 0 ? 'pos' : 'neg');
      const reasons = Array.isArray(s.reasons) ? s.reasons.join('；') : '';
      const tr = document.createElement('tr');
      const barPct = ret === null ? 0 : Math.min(Math.abs(ret) / 40 * 100, 100);
      tr.innerHTML = `
        <td class="name-cell">${s.name}<small>${s.author || ''} · ${s.mode || ''}</small></td>
        <td><span class="pill ${s.status}">${({active:"运行中",rejected:"已拒收",archived:"已下线"})[s.status]||s.status}</span></td>
        <td style="text-align:right">${fmt(s.equity)}</td>
        <td style="text-align:right"><span class="${retCls}">${ret === null ? '—' : ret.toFixed(2) + '%'}</span>
            ${ret !== null ? `<div class="bar"><i style="width:${barPct}%;background:${ret>=0?'var(--green)':'var(--red)'}"></i></div>` : ''}</td>
        <td style="text-align:right">${fmt(s.drawdown_pct)}</td>
        <td style="text-align:right">${s.trades ?? '—'}</td>
        <td style="text-align:right">${fmt(s.win_rate_pct)}</td>
        <td style="text-align:right">${fmt(s.profit_factor)}</td>
        <td style="text-align:right">${s.open_positions ?? '—'}</td>
        <td class="reasons">${reasons}</td>
        <td>${(s.status==='active'||s.status==='rejected') ? `<button class="icon-btn" onclick="del('${s.name}')">下线</button>` : ''}</td>`;
      tb.appendChild(tr);
    });
  } catch (e) { console.error(e); }
}

function fmt(v){ return (v===undefined||v===null||v==='') ? '—' : (+v).toFixed(2); }

function currentSpec() {
  let params = {};
  const raw = document.getElementById('params').value.trim();
  if (raw) params = JSON.parse(raw);
  return {
    name: document.getElementById('name').value.trim(),
    author: document.getElementById('author').value.trim() || 'anonymous',
    mode: document.getElementById('mode').value,
    params: params
  };
}

async function submitStrategy() {
  const btn = document.getElementById('submitBtn');
  try {
    const body = currentSpec();
    if (!body.name) { setMsg('❌ 请先填策略名（模板点击后自动生成，可改）', 'err'); return; }
    btn.disabled = true; btn.innerHTML = '<span class="spin"></span>验证中 10~30s（全样本+前半样本+成本压力）';
    setMsg('正在跑三重回测验证……', '');
    const r = await fetch('/api/strategies', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    const d = await r.json();
    if (!r.ok) { setMsg('❌ ' + (d.error || JSON.stringify(d)), 'err'); return; }
    if (d.pass) {
      setMsg('✅ 通过验证门，零资金账本已开启！全样本 ' + (+d.full_return_pct).toFixed(1) + '% · PF ' + (+d.full_pf).toFixed(2) + ' · 前半 ' + (+d.half_return_pct).toFixed(1) + '%', 'ok');
      toast('🚀 ' + body.name + ' 已上线');
    } else {
      setMsg('❌ 被验证门拒收：' + d.reasons.join('；') + '\n全样本 ' + (+d.full_return_pct).toFixed(1) + '% · PF ' + (+d.full_pf).toFixed(2) + ' · 前半 ' + (+d.half_return_pct).toFixed(1) + '%', 'err');
    }
    refresh();
  } catch (e) { setMsg('❌ ' + e, 'err'); }
  finally { btn.disabled = false; btn.textContent = '🚀 提交并验证'; }
}

async function del(name) {
  await fetch('/api/strategies/' + name, { method: 'DELETE' });
  toast('已下线 ' + name);
  refresh();
}

renderTemplates();
refresh();
setInterval(refresh, 10000);
</script>
</body>
</html>"#;
