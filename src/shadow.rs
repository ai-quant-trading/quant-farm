//! BearShort 影子模式（书第 20 课 Shadow Mode 落地）——薄包装层。
//!
//! 被 OOS 协议拦下的空头策略以"影子身份"与生产并行：真信号、真行情、真成本模型、
//! 零资金，单独记账于 `data/shadow_bear.db`，积累 live out-of-sample 证据。
//! 运行时逻辑全部复用 `farm::Book`（同源，无第二份结算代码）。
//!
//! ## 安全边界（不可违反）
//! - 影子库独立文件，绝不写生产 `coin_ai.db` 的任何表
//! - 绝不触碰下单路径（exchange.rs）——影子只记账不下单

use crate::config::{AppConfig, StrategyMode};
use crate::db::{ApiQuoteRow, TradingDatabase};
use crate::farm::Book;
use crate::types::{Candle, MarketRegime};
use anyhow::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;

pub struct ShadowEngine {
    pub book: Book,
}

impl ShadowEngine {
    pub fn new(base_cfg: &AppConfig) -> Result<Self> {
        let mut cfg = base_cfg.clone();
        cfg.strategy.mode = StrategyMode::BearShort;
        cfg.strategy.bear_stop_atr = base_cfg.shadow.stop_atr;
        cfg.strategy.bear_tp_atr = base_cfg.shadow.tp_atr;
        cfg.strategy.bear_maturity_bars = base_cfg.shadow.maturity_bars;
        cfg.strategy.bear_entry_variant = base_cfg.shadow.variant.clone();
        let book = Book::new("shadow_bear", cfg, &base_cfg.shadow.db_path)?;
        Ok(Self { book })
    }

    pub fn on_metrics(
        &mut self,
        btc_candles: &[Candle],
        universe_candles: &HashMap<String, Vec<Candle>>,
        regime: MarketRegime,
    ) {
        self.book.on_metrics(btc_candles, universe_candles, regime);
    }

    pub fn on_cycle(
        &mut self,
        quotes: &HashMap<String, ApiQuoteRow>,
        regime: MarketRegime,
        coin_metrics: &HashMap<String, (Decimal, Decimal, Decimal)>,
        now_ms: i64,
    ) -> Result<()> {
        self.book.on_cycle(quotes, regime, coin_metrics, now_ms)
    }

    pub fn summary(&self) -> String {
        let s = self.book.status();
        format!(
            "影子账本: 净值 {:.2}U | 闭环 {} 单 | 持仓 {} | 胜率 {:.1}% | PF {:.2}",
            s.equity, s.trades, s.open_positions, s.win_rate_pct, s.profit_factor
        )
    }
}

/// CLI：打印影子账本详情 + 转正判据
pub fn print_shadow_status(cfg: &AppConfig) -> Result<()> {
    let engine = ShadowEngine::new(cfg)?;
    let acct = engine.book.db.get_account_state()?;
    let trades = engine.book.db.list_recent_trades(10_000)?;
    let positions = engine.book.db.list_positions()?;
    println!("\n================================================================");
    println!("  Coin-AI 影子模式账本 (BearShort live OOS 积累中)");
    println!("================================================================");
    let (_, order_cnt, _, _) = engine.book.db.count_table_rows().unwrap_or((0, 0, 0, 0));
    println!(
        "  影子账本: 净值 {:.2}U | 闭环 {} 单 | 持仓 {} | 订单 {} 笔",
        acct.total_equity, trades.len(), positions.len(), order_cnt
    );
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
            trades.len(), wins, trades.len() - wins,
            wins as f64 * 100.0 / trades.len() as f64, pf, avg_r
        );
        let n = cfg.shadow.min_trades_for_promotion;
        let verdict = if trades.len() >= n && pf > dec!(1.1) && avg_r > Decimal::ZERO {
            format!("✅ Live OOS 达标 (≥{}笔 且 PF>1.1 且 R>0) —— 可评估转正", n)
        } else if trades.len() < n {
            format!("⏳ 样本积累中 ({}/{} 笔)，暂不评估", trades.len(), n)
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
    println!("================================================================\n");
    Ok(())
}

/// 供单元测试访问影子底层账本（生产隔离断言用）
pub fn shadow_db_handle(engine: &ShadowEngine) -> &TradingDatabase {
    &engine.book.db
}
