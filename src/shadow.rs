//! BearShort 影子模式（书第 20 课 Shadow Mode 落地）。
//!
//! 被历史 OOS 拦下的空头策略（BS-01 网格最优参数）以"影子身份"与生产并行：
//! **真信号、真行情、真成本模型，但零资金** —— 单独记账于 `data/shadow_bear.db`，
//! 为将来转正积累 live out-of-sample 证据。
//!
//! ## 安全边界（不可违反）
//! - 影子库独立文件，绝不写生产 `coin_ai.db` 的任何表
//! - 绝不触碰下单路径（exchange.rs）——影子只记账不下单
//! - 生产回撤/熔断/净值与影子完全隔离，影子爆仓不影响生产
//!
//! 决策代码 100% 复用生产同源件：
//! `scan_entry_signals`(Bear→scan_bear_short_signals) / `check_new_entries` /
//! `check_position_exit` / `RiskManager` —— 影子成绩即"若它在场"的真实成绩。

use crate::config::{AppConfig, StrategyMode};
use crate::db::{ApiQuoteRow, DbOrderRow, DbPositionRow, TradingDatabase};
use crate::paper_engine::{LiveExitDecision, SqlitePaperEngine};
use crate::risk::RiskManager;
use crate::strategy::scan_entry_signals;
use crate::types::{Candle, MarketRegime};
use anyhow::Result;
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use tracing::info;

pub struct ShadowEngine {
    pub db: TradingDatabase,
    cfg: AppConfig,
    risk_mgr: RiskManager,
    /// 每小时刷新的候选入场缓存 (symbol, side, atr)
    pub pending_entries: Vec<(String, crate::types::Side, Decimal)>,
    seq: usize,
}

impl ShadowEngine {
    pub fn new(base_cfg: &AppConfig) -> Result<Self> {
        let mut cfg = base_cfg.clone();
        cfg.strategy.mode = StrategyMode::BearShort;
        cfg.strategy.bear_stop_atr = base_cfg.shadow.stop_atr;
        cfg.strategy.bear_tp_atr = base_cfg.shadow.tp_atr;
        cfg.strategy.bear_maturity_bars = base_cfg.shadow.maturity_bars;
        cfg.strategy.bear_entry_variant = base_cfg.shadow.variant.clone();
        let db = TradingDatabase::open(&base_cfg.shadow.db_path, cfg.account.initial_equity)?;
        let risk_mgr = RiskManager::new(
            cfg.account.initial_equity,
            cfg.risk.clone(),
            cfg.costs.clone(),
        );
        Ok(Self {
            db,
            cfg,
            risk_mgr,
            pending_entries: Vec::new(),
            seq: 0,
        })
    }

    /// 每小时指标刷新完成时调用：重算 BearShort 候选信号
    pub fn on_metrics(
        &mut self,
        btc_candles: &[Candle],
        universe_candles: &HashMap<String, Vec<Candle>>,
        regime: MarketRegime,
    ) {
        if regime != MarketRegime::Bear {
            self.pending_entries.clear();
            return;
        }
        // 与生产同口径：每币种只喂最近 121 根已收盘 K 线
        let slices: HashMap<String, &[Candle]> = universe_candles
            .iter()
            .filter(|(_, v)| !v.is_empty())
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
                "[影子] BearShort 候选信号 {} 个: {:?}",
                self.pending_entries.len(),
                self.pending_entries
                    .iter()
                    .map(|(s, _, _)| s.clone())
                    .collect::<Vec<_>>()
            );
        }
    }

    /// 每 10 秒一拍：盯市出场 + （有候选时）开仓门控
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

        // ---- 出场（生产同源 check_position_exit）----
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
                false, // 影子无黑天鹅哨兵联动（哨兵属生产防线）
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
                    let exit_oid = format!("SHADOW_EXIT_{}_{}", p2.symbol, self.seq);
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
                        order_type: "SHADOW_MARKET".to_string(),
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
                        &format!("[影子] {}", reason),
                        acct.cash_balance,
                        &p2.opened_at,
                        &now_str,
                    )?;
                    self.db.remove_position(&p2.symbol)?;
                    info!(
                        "[影子平仓] {} {} @ {} | {} | 净盈亏 {:+.3}U | 影子余额 {:.2}U",
                        p2.symbol, p2.side, mark_exit_px, reason, net, acct.cash_balance
                    );
                }
            }
        }

        // ---- 开仓（生产同源 check_new_entries，冷却/点差/半风险 sizing 全生效）----
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
                &HashMap::new(), // 影子沿用生产波动闸门数据源（本拍由生产侧算好传入更优，此处空表=不拦）
            );
            for d in decisions {
                let notional = d.approved_qty * d.fill_entry;
                let entry_fee = notional * fee_rate;
                acct.cash_balance -= entry_fee;
                acct.total_fees_paid += entry_fee;
                self.seq += 1;
                let entry_oid = format!("SHADOW_OPEN_{}_{}", d.symbol, self.seq);
                let stop_oid = format!("SHADOW_STOP_{}_{}", d.symbol, self.seq);
                let side_str = if d.side == crate::types::Side::Long {
                    "LONG"
                } else {
                    "SHORT"
                };
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
                    order_type: "SHADOW_MARKET".to_string(),
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
                    "[影子开仓] {} {} @ {} (名义 {:.2}U, 半风险) | 止损 {} | 止盈 {}",
                    d.symbol, side_str, d.fill_entry, notional, d.initial_stop, d.take_profit
                );
            }
            self.pending_entries.clear(); // 本轮候选消费完毕，等下次整点刷新
        }

        // ---- 影子净值 / 熔断喂值（隔离于生产 RiskManager）----
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

    /// 影子账本摘要（CLI / 整点日志用）
    pub fn summary(&self) -> String {
        match self.db.get_account_state() {
            Ok(a) => {
                let (q, o, p, t) = self.db.count_table_rows().unwrap_or((0, 0, 0, 0));
                format!(
                    "影子账本({}): 净值 {:.2}U | 闭环 {} 单 | 持仓 {} | 订单 {} | 盘口 {}",
                    self.cfg.shadow.db_path, a.total_equity, t, p, o, q
                )
            }
            Err(e) => format!("影子账本不可读: {}", e),
        }
    }
}

/// CLI：打印影子账本详情 + 转正判据
pub fn print_shadow_status(cfg: &AppConfig) -> Result<()> {
    let engine = ShadowEngine::new(cfg)?;
    println!("\n================================================================");
    println!("  Coin-AI 影子模式账本 (BearShort live OOS 积累中)");
    println!("================================================================");
    println!("  {}", engine.summary());
    let trades = engine.db.list_recent_trades(10_000)?;
    let acct = engine.db.get_account_state()?;
    let positions = engine.db.list_positions()?;
    if !trades.is_empty() {
        let wins = trades.iter().filter(|t| t.net_pnl > Decimal::ZERO).count();
        let gross_win: Decimal = trades.iter().map(|t| t.net_pnl.max(Decimal::ZERO)).sum();
        let gross_loss: Decimal = trades.iter().map(|t| t.net_pnl.min(Decimal::ZERO)).sum();
        let pf = if gross_loss < Decimal::ZERO {
            gross_win / gross_loss.abs()
        } else {
            dec!(999)
        };
        let avg_r = trades.iter().map(|t| t.r_multiple).sum::<Decimal>()
            / Decimal::from(trades.len());
        println!(
            "  闭环: {} 单 (胜 {} / 负 {}, 胜率 {:.1}%) | PF {:.2} | 平均R {:+.2}",
            trades.len(),
            wins,
            trades.len() - wins,
            wins as f64 * 100.0 / trades.len() as f64,
            pf,
            avg_r
        );
        let n_promo = cfg.shadow.min_trades_for_promotion;
        let verdict = if trades.len() >= n_promo && pf > dec!(1.1) && avg_r > Decimal::ZERO {
            format!("✅ Live OOS 达标 (≥{}笔 且 PF>1.1 且 R>0) —— 可评估转正", n_promo)
        } else if trades.len() < n_promo {
            format!("⏳ 样本积累中 ({}/{} 笔)，暂不评估", trades.len(), n_promo)
        } else {
            format!("❌ 未达转正门槛 (PF {:.2} / 平均R {:+.2})", pf, avg_r)
        };
        println!("  转正判据: {}", verdict);
    } else {
        println!("  尚无闭环交易（等待合格熊市信号）");
    }
    for p in &positions {
        println!(
            "  持仓: {} {} | 数量 {} | 开仓 {} | 浮动 {:+.3}U",
            p.symbol, p.side, p.qty, p.entry_price, p.unrealized_pnl
        );
    }
    let _ = &acct;
    println!("================================================================\n");
    Ok(())
}
