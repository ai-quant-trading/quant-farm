//! 策略健康监测（边际衰减早警）：滚动实盘表现 vs 现场重算的回测基线。
//!
//! 依据书的第 17 课（在线学习与策略进化）与附录 B 死亡方式 #3（Regime 漂移型死亡）：
//! 冠军策略的边际会随市场结构漂移而衰减，等回撤 50% 才发现就晚了。
//! 四个监测器分级输出 GREEN / YELLOW / RED，整点由 paper-db 守护触发并推送告警。
//!
//! 基线不写死：每次检查用 BacktestEngine 在本地数据上现场重跑冠军回测（~2 秒），
//! 从 BacktestSummary 提取期望——基线永远与当前验证样本对齐，绝不与旧报告快照脱节。

use crate::backtest::BacktestEngine;
use crate::config::{AppConfig, HealthConfig};
use crate::data::DataManager;
use crate::db::TradingDatabase;
use crate::indicators::detect_market_regime;
use crate::types::{Candle, MarketRegime};
use anyhow::Result;
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use tracing::{info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    Green,
    Yellow,
    Red,
    /// 样本不足：不告警，如实展示
    Insufficient,
}

impl Level {
    pub fn label(&self) -> &'static str {
        match self {
            Level::Green => "GREEN",
            Level::Yellow => "YELLOW",
            Level::Red => "RED",
            Level::Insufficient => "GRAY(样本不足)",
        }
    }
    pub fn worst(a: Level, b: Level) -> Level {
        use Level::*;
        match (a, b) {
            (Red, _) | (_, Red) => Red,
            (Yellow, _) | (_, Yellow) => Yellow,
            (Insufficient, x) | (x, Insufficient) => x,
            _ => Green,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MonitorResult {
    pub name: &'static str,
    pub level: Level,
    pub detail: String,
}

/// 现场重算的冠军基线
#[derive(Debug, Clone)]
pub struct Baseline {
    pub trades: usize,
    pub win_rate_pct: Decimal,
    pub profit_factor: Decimal,
    pub avg_win_usd: Decimal,
    pub avg_loss_usd: Decimal,
    pub avg_r: Decimal,
    pub max_dd_pct: Decimal,
    /// 每千根 K 线的交易频率（基线样本总时长归一）
    pub trades_per_1k_bars: Decimal,
    /// 基线样本的 regime 构成（百分比）
    pub bull_pct: Decimal,
    pub bear_pct: Decimal,
    pub chop_pct: Decimal,
    /// 回测假设的均笔成本占名义比例（双边费用+滑点）
    pub assumed_cost_pct: Decimal,
}

/// 滚动窗口统计
#[derive(Debug, Clone, Default)]
pub struct WindowStats {
    pub n: usize,
    pub wins: usize,
    pub win_rate_pct: Decimal,
    pub profit_factor: Decimal,
    pub avg_r: Decimal,
    pub avg_cost_usd: Decimal,
    pub net_pnl: Decimal,
}

#[derive(Debug, Clone)]
pub struct HealthReport {
    pub monitors: Vec<MonitorResult>,
    pub overall: Level,
    pub baseline: Option<Baseline>,
    pub window_trades: WindowStats,
    pub window_days: WindowStats,
}

/// 用回测引擎在本地数据上现场重跑冠军基线
pub async fn derive_baseline(cfg: &AppConfig, data_mgr: &DataManager) -> Result<Option<Baseline>> {
    let hc: &HealthConfig = &cfg.health;
    let mut symbols = vec![cfg.strategy.benchmark_symbol.clone()];
    for s in &cfg.strategy.universe {
        if !symbols.contains(s) {
            symbols.push(s.clone());
        }
    }
    let mut all = HashMap::new();
    for sym in symbols {
        let candles = data_mgr.load_or_fetch_candles(&sym, hc.baseline_bars, false).await?;
        all.insert(sym, candles);
    }
    let btc_len = all.get(&cfg.strategy.benchmark_symbol).map(|v| v.len()).unwrap_or(0);
    if btc_len < 1000 {
        warn!("[健康基线] 本地 K 线不足以推导基线 ({} 根)", btc_len);
        return Ok(None);
    }

    let summary = BacktestEngine::new(cfg.clone()).run(&all)?;
    let trades = &summary.trades;
    if trades.is_empty() {
        return Ok(None);
    }

    let wins = trades.iter().filter(|t| t.net_pnl > Decimal::ZERO).count();
    let gross_win: Decimal = trades.iter().map(|t| t.net_pnl.max(Decimal::ZERO)).sum();
    let gross_loss: Decimal = trades.iter().map(|t| t.net_pnl.min(Decimal::ZERO)).sum();
    let avg_r: Decimal = if trades.is_empty() {
        Decimal::ZERO
    } else {
        trades.iter().map(|t| t.r_multiple).sum::<Decimal>() / Decimal::from(trades.len())
    };
    // 基线样本 regime 构成（与回测 C1 同口径：300 根滑动窗）
    let btc = &all[&cfg.strategy.benchmark_symbol];
    let mut bull = 0usize;
    let mut bear = 0usize;
    let mut chop = 0usize;
    for t in 300..btc.len() {
        match detect_market_regime(&btc[t - 300..=t], cfg.strategy.regime_adx_threshold) {
            MarketRegime::Bull => bull += 1,
            MarketRegime::Bear => bear += 1,
            MarketRegime::Chop => chop += 1,
        }
    }
    let total_rg = (bull + bear + chop).max(1);
    let bars = btc_len - 300;

    Ok(Some(Baseline {
        trades: trades.len(),
        win_rate_pct: summary.win_rate_pct,
        profit_factor: summary.profit_factor,
        avg_win_usd: summary.avg_win_usd,
        avg_loss_usd: summary.avg_loss_usd,
        avg_r,
        max_dd_pct: summary.max_drawdown_pct,
        trades_per_1k_bars: if bars > 0 {
            Decimal::from(trades.len()) * dec!(1000) / Decimal::from(bars)
        } else {
            Decimal::ZERO
        },
        bull_pct: Decimal::from(bull) * dec!(100) / Decimal::from(total_rg),
        bear_pct: Decimal::from(bear) * dec!(100) / Decimal::from(total_rg),
        chop_pct: Decimal::from(chop) * dec!(100) / Decimal::from(total_rg),
        assumed_cost_pct: (cfg.costs.taker_fee_rate + cfg.costs.slippage_rate) * dec!(2),
    }))
}

fn window_stats(trades: &[crate::db::DbTradeRow]) -> WindowStats {
    let n = trades.len();
    if n == 0 {
        return WindowStats::default();
    }
    let wins = trades.iter().filter(|t| t.net_pnl > Decimal::ZERO).count();
    let gross_win: Decimal = trades.iter().map(|t| t.net_pnl.max(Decimal::ZERO)).sum();
    let gross_loss: Decimal = trades.iter().map(|t| t.net_pnl.min(Decimal::ZERO)).sum();
    let pf = if gross_loss < Decimal::ZERO {
        gross_win / gross_loss.abs()
    } else {
        dec!(999)
    };
    let avg_r = trades.iter().map(|t| t.r_multiple).sum::<Decimal>() / Decimal::from(n);
    let avg_cost = trades
        .iter()
        .map(|t| t.total_fees)
        .sum::<Decimal>()
        / Decimal::from(n);
    WindowStats {
        n,
        wins,
        win_rate_pct: Decimal::from(wins) * dec!(100) / Decimal::from(n),
        profit_factor: pf,
        avg_r,
        avg_cost_usd: avg_cost,
        net_pnl: trades.iter().map(|t| t.net_pnl).sum(),
    }
}

/// 主入口：跑一次完整健康检查
pub async fn run_health_check(
    cfg: &AppConfig,
    db_path: &str,
    data_mgr: &DataManager,
) -> Result<HealthReport> {
    let hc = &cfg.health;
    let db = TradingDatabase::open(db_path, cfg.account.initial_equity)?;
    let all_trades = db.list_recent_trades(10_000)?;
    let acct = db.get_account_state()?;
    let open_positions = db.list_positions()?;
    let baseline = derive_baseline(cfg, data_mgr).await?;

    // 滚动窗口
    let by_trade: Vec<_> = all_trades
        .iter()
        .rev()
        .take(hc.window_trades)
        .cloned()
        .collect();
    let now = Utc::now();
    let cutoff_days = now - chrono::Duration::hours(hc.window_days as i64 * 24);
    let by_days: Vec<_> = all_trades
        .iter()
        .filter(|t| {
            chrono::DateTime::parse_from_rfc3339(&t.closed_at)
                .map(|d| d.with_timezone(&Utc) > cutoff_days)
                .unwrap_or(false)
        })
        .cloned()
        .collect();
    let w_trades = window_stats(&by_trade);
    let w_days = window_stats(&by_days);

    let mut monitors = Vec::new();

    // ---- M1 边际衰减 ----
    let w = &w_trades;
    if w.n < hc.min_trades {
        monitors.push(MonitorResult {
            name: "M1 边际衰减",
            level: Level::Insufficient,
            detail: format!("闭环样本 {} 笔 < 最少 {} 笔，暂不判定", w.n, hc.min_trades),
        });
    } else {
        let (level, detail) = if w.profit_factor < hc.pf_red && w.avg_r < hc.avg_r_red {
            (
                Level::Red,
                format!(
                    "近{}笔 PF={:.2} (<{:.2}) 且 平均R={:+.2} (<{:+.2})：边际深度衰减，建议停机复核",
                    w.n, w.profit_factor, hc.pf_red, w.avg_r, hc.avg_r_red
                ),
            )
        } else if w.profit_factor < hc.pf_yellow {
            (
                Level::Yellow,
                format!(
                    "近{}笔 PF={:.2} (<{:.2})：边际走弱，密切观察",
                    w.n, w.profit_factor, hc.pf_yellow
                ),
            )
        } else {
            let base_pf = baseline
                .as_ref()
                .map(|b| b.profit_factor)
                .unwrap_or(w.profit_factor);
            (
                Level::Green,
                format!(
                    "近{}笔 PF={:.2} / 平均R={:+.2} / 胜率{:.1}% (基线 PF={:.2}, 平均R={:+.2})",
                    w.n, w.profit_factor, w.avg_r, w.win_rate_pct, base_pf,
                    baseline.as_ref().map(|b| b.avg_r).unwrap_or(Decimal::ZERO)
                ),
            )
        };
        monitors.push(MonitorResult {
            name: "M1 边际衰减",
            level,
            detail,
        });
    }

    // ---- M2 结构漂移 + 信号饥饿 ----
    if let Some(b) = &baseline {
        // 近 60 天 regime 构成
        let btc = data_mgr
            .load_or_fetch_candles(&cfg.strategy.benchmark_symbol, 1500, false)
            .await
            .unwrap_or_default();
        let cutoff = now - chrono::Duration::days(60);
        let recent: Vec<Candle> = btc
            .iter()
            .filter(|c| {
                chrono::DateTime::from_timestamp_millis(c.timestamp)
                    .map(|d| d.with_timezone(&Utc) > cutoff)
                    .unwrap_or(false)
            })
            .cloned()
            .collect();
        if recent.len() > 400 {
            let mut bull = 0usize;
            let mut bear = 0usize;
            let mut chop = 0usize;
            for i in 300..recent.len() {
                match detect_market_regime(
                    &recent[i - 300..=i],
                    cfg.strategy.regime_adx_threshold,
                ) {
                    MarketRegime::Bull => bull += 1,
                    MarketRegime::Bear => bear += 1,
                    MarketRegime::Chop => chop += 1,
                }
            }
            let total = (bull + bear + chop).max(1);
            let recent_bull = Decimal::from(bull) * dec!(100) / Decimal::from(total);
            let dev = (recent_bull - b.bull_pct).abs();
            let level = if dev > hc.drift_bull_pp {
                Level::Yellow
            } else {
                Level::Green
            };
            monitors.push(MonitorResult {
                name: "M2 结构漂移",
                level,
                detail: format!(
                    "近60天 Bull {:.1}% vs 基线 {:.1}% (偏差 {:.1}pp, 阈值 {:.0}pp)",
                    recent_bull, b.bull_pct, dev, hc.drift_bull_pp
                ),
            });
        } else {
            monitors.push(MonitorResult {
                name: "M2 结构漂移",
                level: Level::Insufficient,
                detail: "近60天 K 线数据不足".to_string(),
            });
        }

        // 信号饥饿：当前 Bull 且超 2× 预期间隔无交易无持仓
        if !btc.is_empty() {
            let cur = detect_market_regime(&btc[btc.len().saturating_sub(300)..], cfg.strategy.regime_adx_threshold);
            if cur == MarketRegime::Bull && open_positions.is_empty() {
                let last_close = all_trades
                    .last()
                    .and_then(|t| chrono::DateTime::parse_from_rfc3339(&t.closed_at).ok())
                    .map(|d| d.with_timezone(&Utc));
                let expected_gap_hours = if b.trades_per_1k_bars > Decimal::ZERO {
                    dec!(1000) / b.trades_per_1k_bars
                } else {
                    dec!(48)
                };
                let starved = match last_close {
                    Some(lc) => (now - lc).num_hours() as i64 > (expected_gap_hours * dec!(2)).to_i64().unwrap_or(96),
                    None => false, // 从未交易过：新账本不算饥饿
                };
                monitors.push(MonitorResult {
                    name: "M2b 信号饥饿",
                    level: if starved { Level::Yellow } else { Level::Green },
                    detail: format!(
                        "当前 Bull 且空仓；预期间隔 {:.0}h, 距上次成交 {}h",
                        expected_gap_hours,
                        last_close.map(|l| (now - l).num_hours()).unwrap_or(-1)
                    ),
                });
            } else {
                monitors.push(MonitorResult {
                    name: "M2b 信号饥饿",
                    level: Level::Green,
                    detail: format!("当前状态 {:?} (仅 Bull+空仓才检查)", cur),
                });
            }
        }
    } else {
        monitors.push(MonitorResult {
            name: "M2 结构漂移",
            level: Level::Insufficient,
            detail: "基线不可用".to_string(),
        });
    }

    // ---- M3 回撤语境 ----
    let current_dd = if acct.peak_equity > Decimal::ZERO {
        (acct.peak_equity - acct.total_equity) / acct.peak_equity * dec!(100)
    } else {
        Decimal::ZERO
    };
    if let Some(b) = &baseline {
        let level = if current_dd > b.max_dd_pct {
            Level::Yellow
        } else {
            Level::Green
        };
        monitors.push(MonitorResult {
            name: "M3 回撤语境",
            level,
            detail: format!(
                "当前回撤 {:.2}% vs 基线最大回撤 {:.2}% (20% 硬熔断由风控独立处理)",
                current_dd, b.max_dd_pct
            ),
        });
    } else {
        monitors.push(MonitorResult {
            name: "M3 回撤语境",
            level: Level::Insufficient,
            detail: format!("当前回撤 {:.2}% (无基线对照)", current_dd),
        });
    }

    // ---- M4 成本健康 ----
    if let Some(b) = &baseline {
        let _ = b;
        if w_trades.n >= hc.min_trades && w_trades.avg_cost_usd > Decimal::ZERO {
            // 费用对名义本金计：假设费用 = 实盘平均名义 × 双边 Taker 费率。
            // 名义取 trades 表自身的 qty × entry_price（与仓位规模自洽，不引入盈亏近似）
            let avg_notional: Decimal = if by_trade.is_empty() {
                Decimal::ZERO
            } else {
                by_trade
                    .iter()
                    .map(|t| t.qty * t.entry_price)
                    .sum::<Decimal>()
                    / Decimal::from(by_trade.len())
            };
            let assumed_fee = avg_notional * cfg.costs.taker_fee_rate * dec!(2);
            let ratio = if assumed_fee > Decimal::ZERO {
                w_trades.avg_cost_usd / assumed_fee
            } else {
                Decimal::ONE
            };
            let level = if ratio > hc.cost_mult_yellow {
                Level::Yellow
            } else {
                Level::Green
            };
            monitors.push(MonitorResult {
                name: "M4 成本健康",
                level,
                detail: format!(
                    "实盘均笔费用 {:.3}U vs 假设 {:.3}U (名义{:.0}U×双边费率, {:.1}×, 阈值 {:.1}×)",
                    w_trades.avg_cost_usd, assumed_fee, avg_notional, ratio, hc.cost_mult_yellow
                ),
            });
        } else {
            monitors.push(MonitorResult {
                name: "M4 成本健康",
                level: Level::Insufficient,
                detail: "闭环样本不足".to_string(),
            });
        }
    }

    let overall = monitors
        .iter()
        .map(|m| m.level)
        .fold(Level::Green, Level::worst);

    Ok(HealthReport {
        monitors,
        overall,
        baseline,
        window_trades: w_trades,
        window_days: w_days,
    })
}

/// 控制台仪表盘 + Markdown 报告
pub fn render_report(report: &HealthReport, report_path: &str) -> Result<()> {
    println!("\n================================================================");
    println!("  Coin-AI 策略健康监测 | 总体状态: {}", report.overall.label());
    println!("================================================================");
    for m in &report.monitors {
        println!("  [{}] {} | {}", m.level.label(), m.name, m.detail);
    }
    println!("  ----------------------------------------------------------------");
    println!(
        "  近{}笔: {}胜/{}, PF {:.2}, 平均R {:+.2}, 净盈亏 {:+.2}U",
        report.window_trades.n,
        report.window_trades.wins,
        report.window_trades.n,
        report.window_trades.profit_factor,
        report.window_trades.avg_r,
        report.window_trades.net_pnl
    );
    if let Some(b) = &report.baseline {
        println!(
            "  基线({}笔): PF {:.2}, 胜率 {:.1}%, 平均R {:+.2}, 最大回撤 {:.2}%, Bull {:.1}%",
            b.trades, b.profit_factor, b.win_rate_pct, b.avg_r, b.max_dd_pct, b.bull_pct
        );
    }
    println!("================================================================\n");

    let mut md = String::from("# 策略健康监测报告\n\n");
    md.push_str(&format!("- 总体状态: **{}**\n", report.overall.label()));
    md.push_str(&format!(
        "- 生成时间: {}\n\n",
        Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
    ));
    md.push_str("| 监测器 | 状态 | 详情 |\n|---|---|---|\n");
    for m in &report.monitors {
        md.push_str(&format!(
            "| {} | {} | {} |\n",
            m.name,
            m.level.label(),
            m.detail
        ));
    }
    if let Some(parent) = std::path::Path::new(report_path).parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    std::fs::write(report_path, md)?;
    info!("[健康监测] 报告已写入 {}", report_path);
    Ok(())
}
