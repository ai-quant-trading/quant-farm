//! Monte Carlo 鲁棒性检验（书第 07 课 Quality Gate 的最后一块）：
//!
//! 1. **自助重抽样 (Bootstrap)**：对冠军回测的交易序列有放回重抽 ×N，
//!    得到期末净值/最大回撤的分布 → 正收益概率、p 值、分位数。
//! 2. **成本压力**：双边成本 ×0.8 / ×1.2 / ×1.5 重结算 → 净利是否仍为正。
//! 3. **参数扰动 ±20%**：关键参数上下扰动重跑回测 → 收益变化幅度（书的门槛：<30%）。
//! 4. **多重检验校正 (Bonferroni)**：本项目历史已测试 N 个策略变体，
//!    显著性门槛从 0.05 收紧到 0.05/N。
//!
//! 纯验证层：不改动任何生产交易行为。

use crate::backtest::BacktestEngine;
use crate::config::AppConfig;
use crate::data::DataManager;
use crate::types::TradeRecord;
use anyhow::Result;
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rust_decimal::prelude::ToPrimitive;
use std::collections::HashMap;
use tracing::info;

/// 确定性 xorshift64*（与 sim.rs 同实现思路，本地独立避免跨模块耦合）
struct Prng(u64);
impl Prng {
    fn new(seed: u64) -> Self {
        Prng(seed.max(1))
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    /// [0, n) 均匀整数
    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next_u64() % (n as u64)) as usize
        }
    }
}

/// 一次自助重抽样的结果
#[derive(Debug, Clone, Copy)]
pub struct BootstrapOutcome {
    pub final_equity: f64,
    pub max_dd_pct: f64,
}

/// 对交易净盈亏序列做有放回重抽样，走权益曲线
pub fn bootstrap_equity(
    trades: &[TradeRecord],
    initial_equity: Decimal,
    sims: usize,
    seed: u64,
) -> Vec<BootstrapOutcome> {
    let mut rng = Prng::new(seed);
    let n = trades.len();
    let init = initial_equity.to_f64().unwrap_or(100.0);
    let pnls: Vec<f64> = trades.iter().map(|t| t.net_pnl.to_f64().unwrap_or(0.0)).collect();
    let mut out = Vec::with_capacity(sims);
    if n == 0 {
        return out;
    }
    for _ in 0..sims {
        let mut eq = init;
        let mut peak = init;
        let mut max_dd = 0.0f64;
        for _ in 0..n {
            eq += pnls[rng.below(n)];
            if eq > peak {
                peak = eq;
            }
            if peak > 0.0 {
                let dd = (peak - eq) / peak * 100.0;
                if dd > max_dd {
                    max_dd = dd;
                }
            }
        }
        out.push(BootstrapOutcome {
            final_equity: eq,
            max_dd_pct: max_dd,
        });
    }
    out
}

/// 成本压力：每笔交易的净盈亏按费用倍数重结算
/// net' = net - fees*(mult-1)
pub fn cost_stress_totals(
    trades: &[TradeRecord],
    initial_equity: Decimal,
    multipliers: &[Decimal],
) -> Vec<(Decimal, Decimal)> {
    let base_fees: Decimal = trades.iter().map(|t| t.total_fees).sum();
    let base_net: Decimal = trades.iter().map(|t| t.net_pnl).sum();
    multipliers
        .iter()
        .map(|&m| {
            let adjusted = base_net - base_fees * (m - Decimal::ONE);
            let final_eq = initial_equity + adjusted;
            (m, final_eq)
        })
        .collect()
}

/// 参数扰动配置：(参数名, -20%值, +20%值)
fn perturbation_grid(cfg: &AppConfig) -> Vec<(&'static str, String, String)> {
    let s = &cfg.strategy;
    let fmt = |v: Decimal| format!("{}", v.round_dp(3));
    vec![
        ("donchian_period", format!("{}", (s.donchian_period as f64 * 0.8) as usize), format!("{}", (s.donchian_period as f64 * 1.2) as usize)),
        ("initial_stop_atr", fmt(s.initial_stop_atr_mult * dec!(0.8)), fmt(s.initial_stop_atr_mult * dec!(1.2))),
        ("chandelier_atr", fmt(s.chandelier_atr_mult * dec!(0.8)), fmt(s.chandelier_atr_mult * dec!(1.2))),
        ("time_stop_bars", format!("{}", (s.time_stop_bars as f64 * 0.8) as usize), format!("{}", (s.time_stop_bars as f64 * 1.2) as usize)),
        ("volume_surge_mult", fmt(s.volume_surge_mult * dec!(0.8)), fmt(s.volume_surge_mult * dec!(1.2))),
        ("atr_period", format!("{}", (s.atr_period as f64 * 0.8) as usize), format!("{}", (s.atr_period as f64 * 1.2) as usize)),
    ]
}

fn apply_perturbation(cfg: &AppConfig, name: &str, value: &str) -> AppConfig {
    let mut c = cfg.clone();
    match name {
        "donchian_period" => c.strategy.donchian_period = value.parse().unwrap_or(c.strategy.donchian_period),
        "initial_stop_atr" => c.strategy.initial_stop_atr_mult = value.parse().unwrap_or(c.strategy.initial_stop_atr_mult),
        "chandelier_atr" => c.strategy.chandelier_atr_mult = value.parse().unwrap_or(c.strategy.chandelier_atr_mult),
        "time_stop_bars" => c.strategy.time_stop_bars = value.parse().unwrap_or(c.strategy.time_stop_bars),
        "volume_surge_mult" => c.strategy.volume_surge_mult = value.parse().unwrap_or(c.strategy.volume_surge_mult),
        "atr_period" => c.strategy.atr_period = value.parse().unwrap_or(c.strategy.atr_period),
        _ => {}
    }
    c
}

/// Monte Carlo 检验主入口
pub async fn run_monte_carlo(
    cfg: &AppConfig,
    data_mgr: &DataManager,
    bars: usize,
    sims: usize,
    n_tested: usize,
    seed: u64,
    report_path: &str,
) -> Result<()> {
    let mut symbols = vec![cfg.strategy.benchmark_symbol.clone()];
    for s in &cfg.strategy.universe {
        if !symbols.contains(s) {
            symbols.push(s.clone());
        }
    }
    let mut all: HashMap<String, Vec<_>> = HashMap::new();
    for sym in symbols {
        let candles = data_mgr.load_or_fetch_candles(&sym, bars, false).await?;
        all.insert(sym, candles);
    }

    // 基线回测
    let summary = BacktestEngine::new(cfg.clone()).run(&all)?;
    let trades = &summary.trades;
    let initial = summary.initial_equity;
    let base_ret = summary.total_return_pct;
    info!(
        "[MonteCarlo] 基线: {} 笔, 净值 {:.2} → {:.2} ({:+.2}%)",
        trades.len(),
        summary.initial_equity,
        summary.final_equity,
        summary.total_return_pct
    );

    // ---- 1. Bootstrap ----
    let outcomes = bootstrap_equity(trades, initial, sims, seed);
    let mut finals: Vec<f64> = outcomes.iter().map(|o| o.final_equity).collect();
    finals.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mut dds: Vec<f64> = outcomes.iter().map(|o| o.max_dd_pct).collect();
    dds.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let pick = |v: &[f64], q: f64| -> f64 { v[(((v.len() as f64) * q) as usize).min(v.len() - 1)] };
    let n = finals.len();
    let init_f = initial.to_f64().unwrap_or(100.0);
    let p_positive = finals.iter().filter(|f| **f > init_f).count() as f64 / n as f64 * 100.0;
    // 单尾 p 值：重抽样期末 <= 本金的比例（无真实边际的原假设下）
    let p_value = finals.iter().filter(|f| **f <= init_f).count() as f64 / n as f64;
    // Bonferroni：门槛 0.05 / n_tested
    let bonferroni_threshold = 0.05f64 / n_tested.max(1) as f64;
    let significant = p_value < bonferroni_threshold;

    // ---- 2. 成本压力 ----
    let cost_mults = [dec!(0.8), dec!(1.2), dec!(1.5)];
    let cost_results = cost_stress_totals(trades, initial, &cost_mults);

    // ---- 3. 参数扰动 ±20% ----
    let grid = perturbation_grid(cfg);
    let mut perturb_rows: Vec<(String, String, Decimal)> = Vec::new(); // (param, direction, return%)
    for (name, lo, hi) in &grid {
        for (dir, val) in [("-20%", lo), ("+20%", hi)] {
            let p_cfg = apply_perturbation(cfg, name, val);
            let s = BacktestEngine::new(p_cfg).run(&all)?;
            perturb_rows.push((name.to_string(), dir.to_string(), s.total_return_pct));
        }
    }
    // 参数稳定性：最差扰动收益相对基线的变化幅度（书的门槛 <30% 相对变化，允许符号不变）
    let worst = perturb_rows
        .iter()
        .min_by(|a, b| a.2.partial_cmp(&b.2).unwrap())
        .map(|r| r.2)
        .unwrap_or(base_ret);
    let param_stable = worst > base_ret * dec!(0.7) && worst > Decimal::ZERO;

    // ---- 报告 ----
    println!("\n====================================================================");
    println!("  Coin-AI Monte Carlo 鲁棒性检验 | 基线 {:+.2}% ({} 笔) | {} 次自助重抽", base_ret, trades.len(), sims);
    println!("====================================================================");
    println!("  [1] Bootstrap 权益分布");
    println!("      期末净值  p5={:.2} / p50={:.2} / p95={:.2}", pick(&finals, 0.05), pick(&finals, 0.50), pick(&finals, 0.95));
    println!("      最大回撤  p50={:.2}% / p95={:.2}%", pick(&dds, 0.50), pick(&dds, 0.95));
    println!("      P(超过本金) = {:.1}%   p值 = {:.4}", p_positive, p_value);
    println!(
        "      Bonferroni 校正: 已测 {} 个变体, 门槛 0.05/{} = {:.5} → {}",
        n_tested, n_tested, bonferroni_threshold,
        if significant { "统计显著 PASS" } else { "未达显著 FAIL" }
    );
    println!("  [2] 成本压力 (双边费用倍数)");
    for (m, eq) in &cost_results {
        println!("      ×{:.1}: 净值 {:.2} {}", m, eq, if *eq > initial { "PASS" } else { "FAIL" });
    }
    println!("  [3] 参数扰动 ±20%");
    for (name, dir, r) in &perturb_rows {
        println!("      {:<18} {:>4}: {:+.2}%", name, dir, r);
    }
    println!("      最差扰动 {:+.2}% vs 基线 {:+.2}% → {}", worst, base_ret,
        if param_stable { "参数稳定 PASS (变化<30%且保持为正)" } else { "参数敏感 FAIL" });
    println!("====================================================================\n");

    let mut md = String::from("# Monte Carlo 鲁棒性检验报告\n\n");
    md.push_str(&format!(
        "- 生成时间: {} | 基线: {:+.2}% ({} 笔) | 重抽 {} 次 (seed {})\n\n",
        Utc::now().format("%Y-%m-%d %H:%M UTC"), base_ret, trades.len(), sims, seed
    ));
    md.push_str("## 1. Bootstrap 权益分布\n\n");
    md.push_str("| 指标 | p5 | p50 | p95 |\n|---|---|---|---|\n");
    md.push_str(&format!(
        "| 期末净值 | {:.2} | {:.2} | {:.2} |\n| 最大回撤% | {:.2} | {:.2} | {:.2} |\n\n",
        pick(&finals, 0.05), pick(&finals, 0.50), pick(&finals, 0.95),
        pick(&dds, 0.05), pick(&dds, 0.50), pick(&dds, 0.95)
    ));
    md.push_str(&format!(
        "- P(正收益) = {:.1}%\n- p值 = {:.4}\n- Bonferroni (已测 {} 变体, 门槛 {:.5}): **{}**\n\n",
        p_positive, p_value, n_tested, bonferroni_threshold,
        if significant { "统计显著" } else { "未达显著" }
    ));
    md.push_str("## 2. 成本压力\n\n| 倍数 | 期末净值 | 判定 |\n|---|---|---|\n");
    for (m, eq) in &cost_results {
        md.push_str(&format!(
            "| ×{:.1} | {:.2} | {} |\n",
            m, eq,
            if *eq > initial { "PASS" } else { "FAIL" }
        ));
    }
    md.push_str("\n## 3. 参数扰动 ±20%\n\n| 参数 | 方向 | 收益% |\n|---|---|---|\n");
    for (name, dir, r) in &perturb_rows {
        md.push_str(&format!("| {} | {} | {:+.2} |\n", name, dir, r));
    }
    md.push_str(&format!(
        "\n最差扰动 {:+.2}% vs 基线 {:+.2}% → **{}**\n",
        worst, base_ret,
        if param_stable { "参数稳定 PASS" } else { "参数敏感 FAIL" }
    ));
    std::fs::write(report_path, md)?;
    info!("[MonteCarlo] 报告已写入 {}", report_path);
    Ok(())
}
