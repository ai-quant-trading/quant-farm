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
    /// 组合子策略（(子cfg, 权重)），None = 单策略书
    pub subs: Option<Vec<(AppConfig, Decimal)>>,
    /// 组合书的待开信号（子序号, 真实symbol, 方向, ATR）
    pending_combo: Vec<(usize, String, Side, Decimal)>,
}

/// 剥离组合前缀 "C{i}:SYM" → (子序号, 真实symbol)
fn split_sub_symbol(s: &str) -> (usize, String) {
    match s.split_once(':') {
        Some((idx, sym)) if idx.starts_with('C') => {
            (idx[1..].parse().unwrap_or(0), sym.to_string())
        }
        _ => (0, s.to_string()),
    }
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
            subs: None,
            pending_combo: Vec::new(),
        })
    }

    /// 组合书：多个子策略共享账本与风控，按权重分仓
    pub fn new_combo(
        name: &str,
        base_cfg: AppConfig,
        subs: Vec<(AppConfig, Decimal)>,
        db_path: &str,
    ) -> Result<Self> {
        // 权重归一化
        let total: Decimal = subs.iter().map(|(_, w)| w).sum();
        let total = if total > Decimal::ZERO { total } else { Decimal::ONE };
        let subs: Vec<(AppConfig, Decimal)> = subs
            .into_iter()
            .map(|(c, w)| (c, w / total))
            .collect();
        let mut book = Book::new(name, base_cfg, db_path)?;
        book.subs = Some(subs);
        Ok(book)
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
        if let Some(subs) = &self.subs {
            self.pending_combo.clear();
            for (i, (sub_cfg, _w)) in subs.iter().enumerate() {
                let signals =
                    scan_entry_signals(regime, &slices, btc_candles, &sub_cfg.strategy);
                for s in signals {
                    self.pending_combo.push((i, s.symbol, s.side, s.atr_val));
                }
            }
            self.pending_combo.sort_by(|a, b| b.3.cmp(&a.3));
        }
        let signals = scan_entry_signals(regime, &slices, btc_candles, &self.cfg.strategy);
        self.pending_entries = signals
            .into_iter()
            .map(|s| (s.symbol, s.side, s.atr_val))
            .collect();
        if !self.pending_combo.is_empty() {
            info!(
                "[农场:{}] 组合候选 {} 个: {:?}",
                self.name,
                self.pending_combo.len(),
                self.pending_combo
                    .iter()
                    .map(|(i, s, _, _)| format!("C{}:{}", i, s))
                    .collect::<Vec<_>>()
            );
        }
        if self.subs.is_none() && !self.pending_entries.is_empty() {
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
            // 组合书: 剥前缀拿真实 symbol 的盘口/ATR 与子策略参数
            let (sub_idx, real_sym) = split_sub_symbol(&p.symbol);
            let sub_cfg: &AppConfig = self
                .subs
                .as_ref()
                .and_then(|s| s.get(sub_idx).map(|(c, _)| c))
                .unwrap_or(&self.cfg);
            let q = match quotes.get(&real_sym) {
                Some(q) => q,
                None => {
                    surviving.push(p);
                    continue;
                }
            };
            let atr_opt = coin_metrics.get(&real_sym).map(|&(_, a, _)| a);
            match SqlitePaperEngine::check_position_exit(
                p,
                q,
                regime,
                atr_opt,
                false,
                now_ms,
                &sub_cfg.strategy,
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

        // ---- 组合开仓: 每个子策略用 (净值×权重) 的资金预算独立过完整门控 ----
        if !self.pending_combo.is_empty() {
            let subs = match &self.subs {
                Some(s) => s.clone(),
                None => Vec::new(),
            };
            let equity = acct.cash_balance + total_upnl;
            let mut by_sub: HashMap<usize, Vec<(String, Side, Decimal)>> = HashMap::new();
            for (i, sym, side, atr) in self.pending_combo.drain(..) {
                by_sub.entry(i).or_default().push((sym, side, atr));
            }
            for (i, entries) in by_sub {
                let (sub_cfg, w) = match subs.get(i) {
                    Some(x) => x.clone(),
                    None => continue,
                };
                let mut surviving_this: Vec<DbPositionRow> = surviving
                    .iter()
                    .filter(|p| split_sub_symbol(&p.symbol).0 == i)
                    .cloned()
                    .collect();
                let sub_equity = equity * w;
                let decisions = SqlitePaperEngine::check_new_entries(
                    &entries,
                    quotes,
                    &surviving_this,
                    sub_equity,
                    &self.risk_mgr,
                    &sub_cfg.strategy,
                    |sym| {
                        let real = split_sub_symbol(sym).1;
                        let prefixed = format!("C{}:{}", i, real);
                        self.db
                            .is_symbol_in_cooldown(&prefixed, 1800)
                            .unwrap_or(false)
                    },
                    &HashMap::new(),
                );
                for d in decisions {
                    let notional = d.approved_qty * d.fill_entry;
                    let entry_fee = notional * fee_rate;
                    acct.cash_balance -= entry_fee;
                    acct.total_fees_paid += entry_fee;
                    self.seq += 1;
                    let prefixed = format!("C{}:{}", i, d.symbol);
                    let entry_oid = format!("FARM_OPEN_{}_{}_{}", self.name, prefixed, self.seq);
                    let stop_oid = format!("FARM_STOP_{}_{}_{}", self.name, prefixed, self.seq);
                    let side_str = if d.side == Side::Long { "LONG" } else { "SHORT" };
                    let pos = DbPositionRow {
                        symbol: prefixed.clone(),
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
                    surviving_this.push(pos);
                    info!(
                        "[农场:{}] C{} 开仓 {} {} @ {} (名义 {:.2}U)",
                        self.name, i, d.symbol, side_str, d.fill_entry, notional
                    );
                }
                surviving.retain(|p| split_sub_symbol(&p.symbol).0 != i);
                surviving.extend(surviving_this);
            }
        }

        // ---- 开仓（完整门控：冷却/点差/以损定仓/模式化风险缩放）----
        if self.subs.is_none() && !self.pending_entries.is_empty()
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

    /// 净值曲线（按闭环时间排序的 [ts_ms, equity] 序列，画图用）
    pub fn equity_curve(&self, limit: usize) -> Vec<(i64, Decimal)> {
        match self.db.list_recent_trades(10_000) {
            Ok(mut trades) => {
                trades.reverse();
                let init = self.cfg.account.initial_equity;
                let mut out = vec![(0i64, init)];
                for t in trades.iter().rev().take(limit) {
                    let ts = chrono::DateTime::parse_from_rfc3339(&t.closed_at)
                        .map(|d| d.timestamp_millis())
                        .unwrap_or(0);
                    out.push((ts, t.equity_after));
                }
                out
            }
            Err(_) => Vec::new(),
        }
    }

    /// 最近 N 笔交易（详情弹窗用）
    pub fn recent_trades_json(&self, n: usize) -> Vec<serde_json::Value> {
        match self.db.list_recent_trades(n) {
            Ok(trades) => trades
                .iter()
                .rev()
                .map(|t| {
                    serde_json::json!({
                        "symbol": t.symbol, "side": t.side,
                        "entry": t.entry_price.to_string(), "exit": t.exit_price.to_string(),
                        "net": t.net_pnl.to_string(), "r": t.r_multiple.round_dp(2).to_string(),
                        "reason": t.exit_reason, "closed_at": t.closed_at,
                    })
                })
                .collect(),
            Err(_) => Vec::new(),
        }
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

fn curve_json(curve: &[(i64, Decimal)]) -> serde_json::Value {
    serde_json::Value::Array(
        curve
            .iter()
            .map(|(ts, eq)| {
                serde_json::json!([ts, eq.to_string()])
            })
            .collect(),
    )
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
    /// 组合子策略（mode="combo" 时必填）：各子策略独立扫描/分仓，共享账本与风控
    #[serde(default)]
    pub combo: Option<Vec<ComboSub>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComboSub {
    pub mode: String,
    #[serde(default)]
    pub params: HashMap<String, serde_json::Value>,
    /// 资金权重（默认均分）
    #[serde(default)]
    pub weight: Option<Decimal>,
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
    "ema_fast_period", "ema_slow_period", "ema_cross_allow_short",
    "dip_trend_ema", "dip_depth_atr",
    "st_atr_period", "st_multiplier", "st_allow_short",
    "macd_fast", "macd_slow", "macd_signal", "macd_allow_short",
    "bb_period", "bb_std", "bb_allow_short",
    "ichi_tenkan", "ichi_kijun", "ichi_spanb", "ichi_allow_short",
    "grid_step_pct",
];

fn json_to_decimal(v: &serde_json::Value) -> Option<Decimal> {
    match v {
        serde_json::Value::Number(n) => Decimal::from_str(&n.to_string()).ok(),
        serde_json::Value::String(s) => Decimal::from_str(s).ok(),
        _ => None,
    }
}

fn json_to_bool(v: &serde_json::Value) -> Option<bool> {
    match v {
        serde_json::Value::Bool(b) => Some(*b),
        serde_json::Value::String(s) if s == "true" || s == "false" => Some(s == "true"),
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
        "ema_cross" => StrategyMode::EmaCross,
        "dip_buy" => StrategyMode::DipBuy,
        "supertrend" => StrategyMode::SuperTrend,
        "macd_cross" => StrategyMode::MacdCross,
        "bb_reversion" => StrategyMode::BbReversion,
        "ichimoku" => StrategyMode::Ichimoku,
        "grid_step" => StrategyMode::GridStep,
        "combo" => StrategyMode::Combo,
        other => bail!(
            "未知模式: {} (可选: long_only_breakout / bear_short / chop_mean_reversion / regime_router / ema_cross / dip_buy / supertrend / macd_cross / bb_reversion / ichimoku / grid_step / combo)",
            other
        ),
    };
    if spec.mode == "combo" {
        let subs = spec.combo.as_ref().ok_or_else(|| {
            anyhow::anyhow!("combo 模式必须提供 combo 数组（2~4 个子策略）")
        })?;
        if !(2..=4).contains(&subs.len()) {
            bail!("组合子策略数量须在 2~4 之间，当前 {}", subs.len());
        }
        // 每个子策略单独解析校验（递归，禁止嵌套 combo）
        for s in subs {
            let sub_spec = StrategySpec {
                name: format!("{}_sub", spec.name),
                author: spec.author.clone(),
                mode: s.mode.clone(),
                params: s.params.clone(),
                universe: spec.universe.clone(),
                initial_equity: spec.initial_equity,
                combo: None,
            };
            if s.mode == "combo" {
                bail!("组合不允许嵌套组合");
            }
            spec_to_config(base, &sub_spec)?;
        }
    }
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
            "st_atr_period" => s.st_atr_period = json_to_usize(v).ok_or_else(bad)?,
            "macd_fast" => s.macd_fast = json_to_usize(v).ok_or_else(bad)?,
            "macd_slow" => s.macd_slow = json_to_usize(v).ok_or_else(bad)?,
            "macd_signal" => s.macd_signal = json_to_usize(v).ok_or_else(bad)?,
            "bb_period" => s.bb_period = json_to_usize(v).ok_or_else(bad)?,
            "ichi_tenkan" => s.ichi_tenkan = json_to_usize(v).ok_or_else(bad)?,
            "ichi_kijun" => s.ichi_kijun = json_to_usize(v).ok_or_else(bad)?,
            "ichi_spanb" => s.ichi_spanb = json_to_usize(v).ok_or_else(bad)?,
            "ema_fast_period" => s.ema_fast_period = json_to_usize(v).ok_or_else(bad)?,
            "ema_slow_period" => s.ema_slow_period = json_to_usize(v).ok_or_else(bad)?,
            "dip_trend_ema" => s.dip_trend_ema = json_to_usize(v).ok_or_else(bad)?,
            "ema_slow_period" => s.ema_slow_period = json_to_usize(v).ok_or_else(bad)?,
            "dip_trend_ema" => s.dip_trend_ema = json_to_usize(v).ok_or_else(bad)?,
            "ema_cross_allow_short" => s.ema_cross_allow_short = json_to_bool(v).ok_or_else(bad)?,
            "st_allow_short" => s.st_allow_short = json_to_bool(v).ok_or_else(bad)?,
            "macd_allow_short" => s.macd_allow_short = json_to_bool(v).ok_or_else(bad)?,
            "bb_allow_short" => s.bb_allow_short = json_to_bool(v).ok_or_else(bad)?,
            "ichi_allow_short" => s.ichi_allow_short = json_to_bool(v).ok_or_else(bad)?,
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
                    "dip_depth_atr" => s.dip_depth_atr = d,
                    "st_multiplier" => s.st_multiplier = d,
                    "bb_std" => s.bb_std = d,
                    "grid_step_pct" => s.grid_step_pct = d,
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
    /// 生产账本基准（主循环每拍更新，排行榜置顶展示）
    pub production: Option<BookStatus>,
    pub production_curve: Vec<(i64, Decimal)>,
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
            production: None,
            production_curve: Vec::new(),
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
        // 组合策略：逐子策略独立过验证门，全部通过才算过
        let report = if spec.mode == "combo" {
            let subs = spec.combo.clone().unwrap_or_default();
            let mut all_pass = true;
            let mut reasons = Vec::new();
            let mut agg = ValidationReport {
                spec_name: spec.name.clone(),
                pass: true,
                reasons: vec![],
                full_return_pct: Decimal::ZERO,
                full_pf: Decimal::ZERO,
                full_trades: 0,
                half_return_pct: Decimal::ZERO,
                half_pf: Decimal::ZERO,
                cost15_equity: spec.initial_equity,
                validated_at: Utc::now().to_rfc3339(),
            };
            for (i, s) in subs.iter().enumerate() {
                let sub_spec = StrategySpec {
                    name: format!("{}_sub{}", spec.name, i),
                    author: spec.author.clone(),
                    mode: s.mode.clone(),
                    params: s.params.clone(),
                    universe: spec.universe.clone(),
                    initial_equity: spec.initial_equity,
                    combo: None,
                };
                let r = validate_spec(&self.base_cfg, &sub_spec, &self.data_mgr, 9000).await?;
                reasons.push(format!(
                    "子{}[{}]: {} ({:+.1}%/PF{:.2})",
                    i, s.mode, if r.pass { "通过" } else { "未过" }, r.full_return_pct, r.full_pf
                ));
                if !r.pass {
                    all_pass = false;
                }
                agg.full_return_pct += r.full_return_pct;
                agg.full_trades += r.full_trades;
            }
            agg.pass = all_pass;
            agg.reasons = if all_pass {
                vec!["组合全部子策略通过验证门".to_string()]
            } else {
                reasons
            };
            agg
        } else {
            validate_spec(&self.base_cfg, &spec, &self.data_mgr, 9000).await?
        };
        let status = if report.pass { "active" } else { "rejected" };
        if report.pass {
            let db = self
                .dir
                .join("books")
                .join(format!("{}.db", spec.name))
                .to_str()
                .unwrap_or_default()
                .to_string();
            if spec.mode == "combo" {
                let subs_spec = spec.combo.clone().unwrap_or_default();
                let mut subs = Vec::new();
                for s in &subs_spec {
                    let sub_spec = StrategySpec {
                        name: format!("{}_sub", spec.name),
                        author: spec.author.clone(),
                        mode: s.mode.clone(),
                        params: s.params.clone(),
                        universe: spec.universe.clone(),
                        initial_equity: spec.initial_equity,
                        combo: None,
                    };
                    let cfg = spec_to_config(&self.base_cfg, &sub_spec)?;
                    subs.push((cfg, s.weight.unwrap_or(Decimal::ONE)));
                }
                let book = Book::new_combo(&spec.name, self.base_cfg.clone(), subs, &db)?;
                self.books.insert(spec.name.clone(), book);
            } else {
                let cfg = spec_to_config(&self.base_cfg, &spec)?;
                let book = Book::new(&spec.name, cfg, &db)?;
                self.books.insert(spec.name.clone(), book);
            }
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

    /// 主循环每拍喂入生产账本状态（基准行）
    pub fn set_production(&mut self, st: BookStatus, curve: Vec<(i64, Decimal)>) {
        self.production = Some(st);
        self.production_curve = curve;
    }

    /// 排行榜：生产基准置顶 + 活跃账本实时状态 + 被拒/归档条目
    pub fn leaderboard(&self) -> Vec<serde_json::Value> {
        let mut rows = Vec::new();
        if let Some(p) = &self.production {
            let mut v = serde_json::to_value(p).unwrap_or_default();
            v["author"] = serde_json::Value::String("系统".to_string());
            v["mode"] = serde_json::Value::String("production".to_string());
            v["is_production"] = serde_json::Value::Bool(true);
            v["curve"] = curve_json(&self.production_curve);
            rows.push(v);
        }
        for r in &self.registry {
            if r.status == "active" {
                if let Some(book) = self.books.get(&r.spec.name) {
                    let mut st = serde_json::to_value(book.status()).unwrap_or_default();
                    st["author"] = serde_json::Value::String(r.spec.author.clone());
                    st["mode"] = serde_json::Value::String(r.spec.mode.clone());
                    st["curve"] = curve_json(&book.equity_curve(500));
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

const SHARED_CSS: &str = include_str!("pages/shared.css");

async fn page_dashboard() -> Html<String> {
    Html(include_str!("pages/dashboard.html").replace("{{CSS}}", SHARED_CSS))
}

async fn page_submit() -> Html<String> {
    Html(include_str!("pages/submit.html").replace("{{CSS}}", SHARED_CSS))
}

pub async fn serve(farm: Arc<tokio::sync::Mutex<Farm>>, port: u16) -> Result<()> {
    let app = Router::new()
        .route("/", get(page_dashboard))
        .route("/submit", get(page_submit))
        .route("/api/strategies", get(list_strategies).post(submit_strategy))
        .route("/api/strategies/:name", delete(remove_strategy).get(strategy_detail))
        .route("/api/strategies/:name/promote", post(promote_strategy))
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

async fn strategy_detail(
    State(farm): State<Arc<tokio::sync::Mutex<Farm>>>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let f = farm.lock().await;
    if name == "__production__" {
        if let Some(p) = &f.production {
            return Ok(Json(serde_json::json!({
                "status": p, "curve": curve_json(&f.production_curve), "trades": [],
            })));
        }
        return Err((StatusCode::NOT_FOUND, "生产账本尚未就绪".to_string()));
    }
    // 被拒/归档条目：没有账本但可以查看验证报告与参数
    let reg = f.registry.iter().find(|r| r.spec.name == name);
    let (book, curve, trades) = match f.books.get(&name) {
        Some(b) => (
            Some(b.status()),
            curve_json(&b.equity_curve(2000)),
            b.recent_trades_json(20),
        ),
        None => (None, serde_json::json!([]), Vec::new()),
    };
    if book.is_none() && reg.is_none() {
        return Err((StatusCode::NOT_FOUND, format!("策略 {} 不存在", name)));
    }
    Ok(Json(serde_json::json!({
        "status": book.unwrap_or_default(),
        "curve": curve,
        "trades": trades,
        "validation": reg.and_then(|r| r.validation.clone()),
        "spec": reg.map(|r| r.spec.clone()),
    })))
}

async fn promote_strategy(
    State(farm): State<Arc<tokio::sync::Mutex<Farm>>>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let f = farm.lock().await;
    let reg = f.registry.iter().find(|r| r.spec.name == name).ok_or((
        StatusCode::NOT_FOUND,
        format!("策略 {} 不存在", name),
    ))?;
    if reg.status != "active" {
        return Err((StatusCode::BAD_REQUEST, format!("策略 {} 不是活跃状态，无法转正", name)));
    }
    let book = f.books.get(&name).ok_or((
        StatusCode::NOT_FOUND,
        format!("策略 {} 没有账本", name),
    ))?;
    let st = book.status();
    let min_trades = f.base_cfg.shadow.min_trades_for_promotion;
    let warned = st.trades < min_trades;
    // 写 settings.toml: mode + 参数覆盖
    let spec = &reg.spec;
    let cfg_path = "config/settings.toml";
    let content = std::fs::read_to_string(cfg_path)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("读配置失败: {}", e)))?;
    let mut new_content = content;
    // 替换 mode
    new_content = new_content.replace(
        &format!("mode = \"{}\"", "regime_router"), // 当前生产模式
        &format!("mode = \"{}\"", spec.mode),
    );
    // 逐参数写入（在 [strategy] 段的 regime_adx_threshold 行后插入）
    let mut param_lines = String::new();
    for (k, v) in &spec.params {
        let val = match v {
            serde_json::Value::String(s) => format!("\"{}\"", s),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Bool(b) => b.to_string(),
            _ => continue,
        };
        // 如果文件里已有该键则替换，否则追加到 [strategy] 段末尾
        if new_content.contains(&format!("{} = ", k)) {
            new_content = regex_replace_param(&new_content, k, &val);
        } else {
            param_lines.push_str(&format!("{} = {}\n", k, val));
        }
    }
    if !param_lines.is_empty() {
        new_content = new_content.replace("min_atr_percentile", &format!("{}min_atr_percentile", param_lines));
    }
    std::fs::write(cfg_path, new_content)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("写配置失败: {}", e)))?;
    info!("[农场] 策略 {} 已转正为生产策略 (闭环{}笔 PF{:.2}) — 重启 farm-server 生效", name, st.trades, st.profit_factor);
    Ok(Json(serde_json::json!({
        "promoted": name,
        "mode": spec.mode,
        "trades": st.trades,
        "pf": st.profit_factor.to_string(),
        "warned": warned,
        "note": if warned { format!("已转正(注意: 仅{}笔闭环,未达{}笔建议门槛)", st.trades, min_trades) } else { "已写入 settings.toml，重启后生效".to_string() }
    })))
}

fn regex_replace_param(content: &str, key: &str, val: &str) -> String {
    // 简单逐行替换 "key = ..." 为 "key = val"
    let mut out = Vec::new();
    for line in content.lines() {
        let t = line.trim();
        if t.starts_with(&format!("{} = ", key)) && !t.starts_with(&format!("{} = \"", key)) || t.starts_with(&format!("{} = \"", key)) {
            out.push(format!("{} = {}", key, val));
        } else {
            out.push(line.to_string());
        }
    }
    out.join("\n") + "\n"
}




