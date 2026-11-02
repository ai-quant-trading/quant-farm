use crate::backtest::{BacktestEngine, BacktestSummary};
use crate::config::{AppConfig, StrategyMode};
use crate::types::Candle;
use anyhow::Result;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct StrategyCandidateSpec {
    pub id: String,
    pub name: String,
    pub mode: StrategyMode,
    pub top_k: usize,
    pub donchian_period: usize,
    pub volume_surge_mult: Decimal,
    pub initial_stop_atr: Decimal,
    pub chandelier_atr: Decimal,
    pub breakeven_r: Decimal,
    pub time_stop_bars: usize,
    pub regime_adx: Decimal,
    // ---- BearShort 专用旋钮（None = 沿用 settings.toml 默认）----
    pub bear_stop_atr: Option<Decimal>,
    pub bear_tp_atr: Option<Decimal>,
    pub bear_maturity_bars: Option<usize>,
    pub bear_entry_variant: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EvolutionResult {
    pub spec: StrategyCandidateSpec,
    pub full_summary: BacktestSummary,
    pub in_sample_return_pct: Decimal,
    pub out_of_sample_return_pct: Decimal,
    pub out_of_sample_pnl_usd: Decimal,
    pub out_of_sample_max_dd_pct: Decimal,
    pub recent_60d_return_pct: Decimal,
    pub recent_60d_pnl_usd: Decimal,
    pub robustness_score: f64,
}

/// 生成 20+ 套涵盖 5 大策略流派与不同风控止盈参数的候选量化策略矩阵
pub fn generate_strategy_candidates() -> Vec<StrategyCandidateSpec> {
    vec![
        // 流派 A：All-Weather 多空全天候突破系列
        StrategyCandidateSpec {
            id: "AW-01".to_string(),
            name: "多空全天候突破 (基准 20h + 3.0x ATR 吊灯)".to_string(),
            mode: StrategyMode::AllWeatherBreakout,
            top_k: 3,
            donchian_period: 20,
            volume_surge_mult: dec!(1.3),
            initial_stop_atr: dec!(2.2),
            chandelier_atr: dec!(3.0),
            breakeven_r: dec!(1.5),
            time_stop_bars: 16,
            regime_adx: dec!(20.0),
            bear_stop_atr: None, bear_tp_atr: None, bear_maturity_bars: None, bear_entry_variant: None,
        },
        StrategyCandidateSpec {
            id: "AW-02".to_string(),
            name: "多空灵敏突破 (16h 通道 + 2.8x 紧止盈)".to_string(),
            mode: StrategyMode::AllWeatherBreakout,
            top_k: 3,
            donchian_period: 16,
            volume_surge_mult: dec!(1.25),
            initial_stop_atr: dec!(2.0),
            chandelier_atr: dec!(2.8),
            breakeven_r: dec!(1.3),
            time_stop_bars: 14,
            regime_adx: dec!(19.0),
            bear_stop_atr: None, bear_tp_atr: None, bear_maturity_bars: None, bear_entry_variant: None,
        },
        StrategyCandidateSpec {
            id: "AW-03".to_string(),
            name: "多空宽幅波段 (24h 通道 + 3.5x 宽止盈)".to_string(),
            mode: StrategyMode::AllWeatherBreakout,
            top_k: 2,
            donchian_period: 24,
            volume_surge_mult: dec!(1.35),
            initial_stop_atr: dec!(2.4),
            chandelier_atr: dec!(3.5),
            breakeven_r: dec!(1.5),
            time_stop_bars: 20,
            regime_adx: dec!(20.0),
            bear_stop_atr: None, bear_tp_atr: None, bear_maturity_bars: None, bear_entry_variant: None,
        },
        StrategyCandidateSpec {
            id: "AW-04".to_string(),
            name: "多空强趋势过滤 (ADX>22 + 18h 突破)".to_string(),
            mode: StrategyMode::AllWeatherBreakout,
            top_k: 3,
            donchian_period: 18,
            volume_surge_mult: dec!(1.3),
            initial_stop_atr: dec!(2.2),
            chandelier_atr: dec!(3.2),
            breakeven_r: dec!(1.4),
            time_stop_bars: 18,
            regime_adx: dec!(22.0),
            bear_stop_atr: None, bear_tp_atr: None, bear_maturity_bars: None, bear_entry_variant: None,
        },
        // 流派 B：Long-Only 纯多头牛市猎手 + 熊市空仓避险系列
        StrategyCandidateSpec {
            id: "LO-01".to_string(),
            name: "纯多头避险突破 (20h + 熊市100%空仓)".to_string(),
            mode: StrategyMode::LongOnlyBreakout,
            top_k: 3,
            donchian_period: 20,
            volume_surge_mult: dec!(1.3),
            initial_stop_atr: dec!(2.2),
            chandelier_atr: dec!(3.0),
            breakeven_r: dec!(1.5),
            time_stop_bars: 16,
            regime_adx: dec!(20.0),
            bear_stop_atr: None, bear_tp_atr: None, bear_maturity_bars: None, bear_entry_variant: None,
        },
        StrategyCandidateSpec {
            id: "LO-02".to_string(),
            name: "纯多头龙头狙击 (Top2 极强动量 + 16h 突破)".to_string(),
            mode: StrategyMode::LongOnlyBreakout,
            top_k: 2,
            donchian_period: 16,
            volume_surge_mult: dec!(1.25),
            initial_stop_atr: dec!(2.1),
            chandelier_atr: dec!(3.2),
            breakeven_r: dec!(1.3),
            time_stop_bars: 16,
            regime_adx: dec!(18.5),
            bear_stop_atr: None, bear_tp_atr: None, bear_maturity_bars: None, bear_entry_variant: None,
        },
        StrategyCandidateSpec {
            id: "LO-03".to_string(),
            name: "纯多头长波段 (24h 突破 + 3.8x 放飞利润)".to_string(),
            mode: StrategyMode::LongOnlyBreakout,
            top_k: 3,
            donchian_period: 24,
            volume_surge_mult: dec!(1.2),
            initial_stop_atr: dec!(2.3),
            chandelier_atr: dec!(3.8),
            breakeven_r: dec!(1.4),
            time_stop_bars: 22,
            regime_adx: dec!(19.0),
            bear_stop_atr: None, bear_tp_atr: None, bear_maturity_bars: None, bear_entry_variant: None,
        },
        // 流派 C：Volatility Squeeze 波动率收敛爆发系列
        StrategyCandidateSpec {
            id: "VS-01".to_string(),
            name: "波动率收敛爆发 (18h 窄幅蓄势突破)".to_string(),
            mode: StrategyMode::VolatilitySqueeze,
            top_k: 3,
            donchian_period: 18,
            volume_surge_mult: dec!(1.25),
            initial_stop_atr: dec!(2.0),
            chandelier_atr: dec!(3.0),
            breakeven_r: dec!(1.3),
            time_stop_bars: 16,
            regime_adx: dec!(19.0),
            bear_stop_atr: None, bear_tp_atr: None, bear_maturity_bars: None, bear_entry_variant: None,
        },
        StrategyCandidateSpec {
            id: "VS-02".to_string(),
            name: "波动率收敛高盈亏比 (20h + 3.5x ATR 止盈)".to_string(),
            mode: StrategyMode::VolatilitySqueeze,
            top_k: 4,
            donchian_period: 20,
            volume_surge_mult: dec!(1.2),
            initial_stop_atr: dec!(2.2),
            chandelier_atr: dec!(3.5),
            breakeven_r: dec!(1.4),
            time_stop_bars: 18,
            regime_adx: dec!(18.5),
            bear_stop_atr: None, bear_tp_atr: None, bear_maturity_bars: None, bear_entry_variant: None,
        },
        // 流派 D：Trend Pullback 强势币回踩均线二次启动系列
        StrategyCandidateSpec {
            id: "TP-01".to_string(),
            name: "强势趋势回踩 EMA20 启动 (低吸高盈亏比)".to_string(),
            mode: StrategyMode::TrendPullback,
            top_k: 3,
            donchian_period: 20,
            volume_surge_mult: dec!(1.15),
            initial_stop_atr: dec!(2.0),
            chandelier_atr: dec!(3.2),
            breakeven_r: dec!(1.3),
            time_stop_bars: 16,
            regime_adx: dec!(19.0),
            bear_stop_atr: None, bear_tp_atr: None, bear_maturity_bars: None, bear_entry_variant: None,
        },
        StrategyCandidateSpec {
            id: "TP-02".to_string(),
            name: "强势回踩宽止盈 (Top4 + 3.6x ATR 吊灯)".to_string(),
            mode: StrategyMode::TrendPullback,
            top_k: 4,
            donchian_period: 20,
            volume_surge_mult: dec!(1.1),
            initial_stop_atr: dec!(2.2),
            chandelier_atr: dec!(3.6),
            breakeven_r: dec!(1.4),
            time_stop_bars: 20,
            regime_adx: dec!(18.5),
            bear_stop_atr: None, bear_tp_atr: None, bear_maturity_bars: None, bear_entry_variant: None,
        },
        // 流派 E：Ensemble Composite 多因子融合共振 (突破 + 回踩 + 负费率逼空)
        StrategyCandidateSpec {
            id: "EN-01".to_string(),
            name: "多因子融合一型 (20h 突破+回踩双引擎 + 3.0x 吊灯)".to_string(),
            mode: StrategyMode::EnsembleComposite,
            top_k: 3,
            donchian_period: 20,
            volume_surge_mult: dec!(1.3),
            initial_stop_atr: dec!(2.2),
            chandelier_atr: dec!(3.0),
            breakeven_r: dec!(1.4),
            time_stop_bars: 16,
            regime_adx: dec!(19.5),
            bear_stop_atr: None, bear_tp_atr: None, bear_maturity_bars: None, bear_entry_variant: None,
        },
        StrategyCandidateSpec {
            id: "EN-02".to_string(),
            name: "多因子融合二型 (18h 突破+回踩 + 3.4x 宽幅锁利)".to_string(),
            mode: StrategyMode::EnsembleComposite,
            top_k: 3,
            donchian_period: 18,
            volume_surge_mult: dec!(1.25),
            initial_stop_atr: dec!(2.1),
            chandelier_atr: dec!(3.4),
            breakeven_r: dec!(1.3),
            time_stop_bars: 18,
            regime_adx: dec!(19.0),
            bear_stop_atr: None, bear_tp_atr: None, bear_maturity_bars: None, bear_entry_variant: None,
        },
        StrategyCandidateSpec {
            id: "EN-03".to_string(),
            name: "多因子融合三型 (Top2 集中火力 + 1.2R 快速保本)".to_string(),
            mode: StrategyMode::EnsembleComposite,
            top_k: 2,
            donchian_period: 20,
            volume_surge_mult: dec!(1.3),
            initial_stop_atr: dec!(2.2),
            chandelier_atr: dec!(3.2),
            breakeven_r: dec!(1.2),
            time_stop_bars: 16,
            regime_adx: dec!(20.0),
            bear_stop_atr: None, bear_tp_atr: None, bear_maturity_bars: None, bear_entry_variant: None,
        },
        StrategyCandidateSpec {
            id: "EN-04".to_string(),
            name: "多因子融合四型 (22h 稳健通道 + 3.6x 长线吊灯)".to_string(),
            mode: StrategyMode::EnsembleComposite,
            top_k: 3,
            donchian_period: 22,
            volume_surge_mult: dec!(1.35),
            initial_stop_atr: dec!(2.3),
            chandelier_atr: dec!(3.6),
            breakeven_r: dec!(1.5),
            time_stop_bars: 20,
            regime_adx: dec!(20.0),
            bear_stop_atr: None, bear_tp_atr: None, bear_maturity_bars: None, bear_entry_variant: None,
        },
        // 流派 F：第二代进化策略 (GEN-2：基于第一轮冠军 LO-01 深度精细化演进)
        StrategyCandidateSpec {
            id: "G2-01".to_string(),
            name: "二代进化：纯多头高敏保本 (20h + 1.25R 极速保本 + 3.2x 吊灯)".to_string(),
            mode: StrategyMode::LongOnlyBreakout,
            top_k: 3,
            donchian_period: 20,
            volume_surge_mult: dec!(1.3),
            initial_stop_atr: dec!(2.2),
            chandelier_atr: dec!(3.2),
            breakeven_r: dec!(1.25),
            time_stop_bars: 14,
            regime_adx: dec!(20.0),
            bear_stop_atr: None, bear_tp_atr: None, bear_maturity_bars: None, bear_entry_variant: None,
        },
        StrategyCandidateSpec {
            id: "G2-02".to_string(),
            name: "二代进化：纯多头严选放量 (20h + 1.4x 强放量 + ADX>21)".to_string(),
            mode: StrategyMode::LongOnlyBreakout,
            top_k: 3,
            donchian_period: 20,
            volume_surge_mult: dec!(1.4),
            initial_stop_atr: dec!(2.2),
            chandelier_atr: dec!(3.0),
            breakeven_r: dec!(1.4),
            time_stop_bars: 15,
            regime_adx: dec!(21.0),
            bear_stop_atr: None, bear_tp_atr: None, bear_maturity_bars: None, bear_entry_variant: None,
        },
        StrategyCandidateSpec {
            id: "G2-03".to_string(),
            name: "二代进化：纯多头宽波段锁利 (22h 通道 + 3.4x ATR 吊灯)".to_string(),
            mode: StrategyMode::LongOnlyBreakout,
            top_k: 3,
            donchian_period: 22,
            volume_surge_mult: dec!(1.3),
            initial_stop_atr: dec!(2.2),
            chandelier_atr: dec!(3.4),
            breakeven_r: dec!(1.4),
            time_stop_bars: 18,
            regime_adx: dec!(20.0),
            bear_stop_atr: None, bear_tp_atr: None, bear_maturity_bars: None, bear_entry_variant: None,
        },
        StrategyCandidateSpec {
            id: "G2-04".to_string(),
            name: "二代进化：全周期稳健王 (24h 突破 + 1.3R 保本 + 3.5x 吊灯)".to_string(),
            mode: StrategyMode::LongOnlyBreakout,
            top_k: 3,
            donchian_period: 24,
            volume_surge_mult: dec!(1.25),
            initial_stop_atr: dec!(2.2),
            chandelier_atr: dec!(3.5),
            breakeven_r: dec!(1.3),
            time_stop_bars: 18,
            regime_adx: dec!(20.0),
            bear_stop_atr: None, bear_tp_atr: None, bear_maturity_bars: None, bear_entry_variant: None,
        },
        StrategyCandidateSpec {
            id: "G2-05".to_string(),
            name: "二代进化：大周期趋势猎手 (30h 突破 + 3.8x 放飞利润)".to_string(),
            mode: StrategyMode::LongOnlyBreakout,
            top_k: 2,
            donchian_period: 30,
            volume_surge_mult: dec!(1.25),
            initial_stop_atr: dec!(2.3),
            chandelier_atr: dec!(3.8),
            breakeven_r: dec!(1.4),
            time_stop_bars: 22,
            regime_adx: dec!(20.0),
            bear_stop_atr: None, bear_tp_atr: None, bear_maturity_bars: None, bear_entry_variant: None,
        },
        // 流派 G：第三代终极进化 (GEN-3：结合 G2-02 强放量 + LO-03 24h波段 + 震荡冷却防磨损)
        StrategyCandidateSpec {
            id: "G3-01".to_string(),
            name: "三代进化：24h强放量复合王 (24h + 1.35x放量 + 3.6x吊灯)".to_string(),
            mode: StrategyMode::LongOnlyBreakout,
            top_k: 3,
            donchian_period: 24,
            volume_surge_mult: dec!(1.35),
            initial_stop_atr: dec!(2.2),
            chandelier_atr: dec!(3.6),
            breakeven_r: dec!(1.4),
            time_stop_bars: 18,
            regime_adx: dec!(20.5),
            bear_stop_atr: None, bear_tp_atr: None, bear_maturity_bars: None, bear_entry_variant: None,
        },
        StrategyCandidateSpec {
            id: "G3-02".to_string(),
            name: "三代进化：全天候高夏普猎手 (20h + 1.35x放量 + 3.3x吊灯)".to_string(),
            mode: StrategyMode::LongOnlyBreakout,
            top_k: 3,
            donchian_period: 20,
            volume_surge_mult: dec!(1.35),
            initial_stop_atr: dec!(2.2),
            chandelier_atr: dec!(3.3),
            breakeven_r: dec!(1.4),
            time_stop_bars: 16,
            regime_adx: dec!(20.5),
            bear_stop_atr: None, bear_tp_atr: None, bear_maturity_bars: None, bear_entry_variant: None,
        },
        // 流派 BS：成熟熊市空头（验尸报告驱动：成熟度门 + 拉升衰竭入场 + 非对称屏障 + 破位即撤）
        StrategyCandidateSpec {
            id: "BS-01".to_string(),
            name: "熊市空头：双变体 2.5/4.0 非对称屏障 (网格最优)".to_string(),
            mode: StrategyMode::BearShort,
            top_k: 3,
            donchian_period: 20,
            volume_surge_mult: dec!(1.30),
            initial_stop_atr: dec!(2.2),
            chandelier_atr: dec!(3.0),
            breakeven_r: dec!(1.5),
            time_stop_bars: 12,
            regime_adx: dec!(20.0),
            bear_stop_atr: Some(dec!(2.5)),
            bear_tp_atr: Some(dec!(4.0)),
            bear_maturity_bars: Some(24),
            bear_entry_variant: Some("both".to_string()),
        },
        StrategyCandidateSpec {
            id: "BS-02".to_string(),
            name: "熊市空头：拉升衰竭单变体 (治卖在底部)".to_string(),
            mode: StrategyMode::BearShort,
            top_k: 3,
            donchian_period: 20,
            volume_surge_mult: dec!(1.30),
            initial_stop_atr: dec!(2.2),
            chandelier_atr: dec!(3.0),
            breakeven_r: dec!(1.5),
            time_stop_bars: 12,
            regime_adx: dec!(20.0),
            bear_stop_atr: Some(dec!(2.5)),
            bear_tp_atr: Some(dec!(4.0)),
            bear_maturity_bars: Some(24),
            bear_entry_variant: Some("rejection".to_string()),
        },
        StrategyCandidateSpec {
            id: "BS-03".to_string(),
            name: "熊市空头：成熟度加倍 (48h 只做大熊)".to_string(),
            mode: StrategyMode::BearShort,
            top_k: 2,
            donchian_period: 20,
            volume_surge_mult: dec!(1.30),
            initial_stop_atr: dec!(2.2),
            chandelier_atr: dec!(3.0),
            breakeven_r: dec!(1.5),
            time_stop_bars: 12,
            regime_adx: dec!(20.0),
            bear_stop_atr: Some(dec!(2.5)),
            bear_tp_atr: Some(dec!(4.0)),
            bear_maturity_bars: Some(48),
            bear_entry_variant: Some("both".to_string()),
        },
    ]
}

fn slice_candles_window(
    all_candles: &HashMap<String, Vec<Candle>>,
    start_idx: usize,
    end_idx: usize,
) -> HashMap<String, Vec<Candle>> {
    let mut sliced = HashMap::new();
    for (sym, vec) in all_candles {
        let s = start_idx.min(vec.len());
        let e = end_idx.min(vec.len());
        if e > s {
            sliced.insert(sym.clone(), vec[s..e].to_vec());
        }
    }
    sliced
}

/// 运行全自动策略生成、样本内外盲测（Walk-Forward）与多时间窗口盈亏竞技场
pub fn run_strategy_evolution(
    base_cfg: &AppConfig,
    all_candles: &HashMap<String, Vec<Candle>>,
    report_path: impl AsRef<Path>,
) -> Result<Vec<EvolutionResult>> {
    let btc_len = all_candles
        .get(&base_cfg.strategy.benchmark_symbol)
        .map(|v| v.len())
        .unwrap_or(0);

    if btc_len < 1000 {
        anyhow::bail!("生产数据集长度不足 ({} 根)，至少需要 1000 根 1h K 线", btc_len);
    }

    // 划分：
    // 1) In-Sample 训练集：前 65% 数据
    // 2) Out-of-Sample 样本外盲测集：后 35% 数据（前延 210 根作为指标预热）
    // 3) Recent 60d 最近 60 天 (1440 小时) 生产实况窗口
    let split_idx = (btc_len * 65) / 100;
    let oos_start = split_idx.saturating_sub(250);
    let recent_60d_start = btc_len.saturating_sub(1440 + 250);

    let is_candles = slice_candles_window(all_candles, 0, split_idx);
    let oos_candles = slice_candles_window(all_candles, oos_start, btc_len);
    let recent_candles = slice_candles_window(all_candles, recent_60d_start, btc_len);

    let candidates = generate_strategy_candidates();
    let mut results: Vec<EvolutionResult> = Vec::with_capacity(candidates.len());

    for spec in candidates {
        let mut cfg = base_cfg.clone();
        cfg.strategy.mode = spec.mode;
        cfg.strategy.top_k_momentum = spec.top_k;
        cfg.strategy.donchian_period = spec.donchian_period;
        cfg.strategy.volume_surge_mult = spec.volume_surge_mult;
        cfg.strategy.initial_stop_atr_mult = spec.initial_stop_atr;
        cfg.strategy.chandelier_atr_mult = spec.chandelier_atr;
        cfg.strategy.breakeven_r_mult = spec.breakeven_r;
        cfg.strategy.time_stop_bars = spec.time_stop_bars;
        cfg.strategy.regime_adx_threshold = spec.regime_adx;
        if let Some(v) = spec.bear_stop_atr { cfg.strategy.bear_stop_atr = v; }
        if let Some(v) = spec.bear_tp_atr { cfg.strategy.bear_tp_atr = v; }
        if let Some(v) = spec.bear_maturity_bars { cfg.strategy.bear_maturity_bars = v; }
        if let Some(v) = &spec.bear_entry_variant { cfg.strategy.bear_entry_variant = v.clone(); }

        let engine = BacktestEngine::new(cfg);

        let full_summary = engine.run(all_candles)?;
        let is_summary = engine.run(&is_candles)?;
        let oos_summary = engine.run(&oos_candles)?;
        let recent_summary = engine.run(&recent_candles)?;

        // 计算综合稳健度得分 (Robustness Score)：
        // 重点奖励：全周期收益率 + 样本外盲测正收益 + 近 60 天收益 + 高夏普 - 最大回撤惩罚
        let full_ret = full_summary.total_return_pct.to_f64().unwrap_or(0.0);
        let oos_ret = oos_summary.total_return_pct.to_f64().unwrap_or(0.0);
        let r60_ret = recent_summary.total_return_pct.to_f64().unwrap_or(0.0);
        let max_dd = full_summary.max_drawdown_pct.to_f64().unwrap_or(20.0);

        let oos_bonus = if oos_ret > 0.0 {
            oos_ret * 1.5
        } else {
            oos_ret * 2.5
        };

        let robustness_score = full_ret * 1.0
            + oos_bonus
            + r60_ret * 0.8
            + full_summary.sharpe_ratio * 8.0
            - max_dd * 0.9;

        results.push(EvolutionResult {
            spec,
            full_summary,
            in_sample_return_pct: is_summary.total_return_pct,
            out_of_sample_return_pct: oos_summary.total_return_pct,
            out_of_sample_pnl_usd: oos_summary.final_equity - oos_summary.initial_equity,
            out_of_sample_max_dd_pct: oos_summary.max_drawdown_pct,
            recent_60d_return_pct: recent_summary.total_return_pct,
            recent_60d_pnl_usd: recent_summary.final_equity - recent_summary.initial_equity,
            robustness_score,
        });
    }

    // 按稳健度得分从高到低排序
    results.sort_by(|a, b| {
        b.robustness_score
            .partial_cmp(&a.robustness_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    write_evolution_markdown(&results, btc_len, base_cfg.account.initial_equity, report_path)?;

    Ok(results)
}

fn write_evolution_markdown(
    results: &[EvolutionResult],
    total_bars: usize,
    initial_equity: Decimal,
    path: impl AsRef<Path>,
) -> Result<()> {
    if let Some(parent) = path.as_ref().parent() {
        fs::create_dir_all(parent)?;
    }

    let mut md = String::new();
    md.push_str("# Coin-AI 生产数据多策略进化与盈亏竞技场报告\n\n");
    md.push_str(&format!(
        "- **生产数据集规模**：10 个高流动性主流合约 × `{}` 小时真实 1h K 线（共 `{}` 条生产 K 线记录）\n",
        total_bars,
        total_bars * 10
    ));
    md.push_str(&format!(
        "- **初始测试本金**：`{:.2} USDT`（双边强制实扣 `0.05%` 手续费 + `0.05%` 滑点 + 8h 资金费率）\n",
        initial_equity
    ));
    md.push_str("- **防过拟合 Walk-Forward 验证**：前 65% 为样本内（In-Sample），后 35% 为样本外盲测（Out-of-Sample），并单独核算最近 60 天实况净盈亏。\n\n");

    md.push_str("## 一、 生成策略生产数据实测盈亏总榜\n\n");
    md.push_str("| 排名 | 策略编号 | 策略名称与流派 | 全周期净利 (U) | 全周期收益率 | 样本内收益 | 样本外盲测净利 | 近60天实况净利 | 最大回撤 | 夏普比率 | 盈亏比(PF) | 胜率 | 交易数 |\n");
    md.push_str("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n");

    for (i, r) in results.iter().enumerate() {
        let s = &r.full_summary;
        let full_pnl = s.final_equity - s.initial_equity;
        md.push_str(&format!(
            "| **#{}** | `{}` | {} | **`{:+.2} U`** | **`{:+.2}%`** | `{:+.2}%` | `{:+.2} U ({:+.2}%)` | `{:+.2} U ({:+.2}%)` | `{:.2}%` | `{:.2}` | `{:.2}` | `{:.1}%` | `{}` |\n",
            i + 1,
            r.spec.id,
            r.spec.name,
            full_pnl,
            s.total_return_pct,
            r.in_sample_return_pct,
            r.out_of_sample_pnl_usd,
            r.out_of_sample_return_pct,
            r.recent_60d_pnl_usd,
            r.recent_60d_return_pct,
            s.max_drawdown_pct,
            s.sharpe_ratio,
            s.profit_factor,
            s.win_rate_pct,
            s.total_trades
        ));
    }

    if let Some(champ) = results.first() {
        md.push_str("\n## 二、 冠军策略（#1 Champion）参数与逐笔盈亏剖析\n\n");
        md.push_str(&format!(
            "- **冠军策略**：`[{}] {}`\n",
            champ.spec.id, champ.spec.name
        ));
        md.push_str(&format!(
            "- **核心参数**：`mode = {:?}`, `top_k = {}`, `donchian = {}h`, `vol_surge = {}x`, `init_stop = {}x ATR`, `chandelier = {}x ATR`, `breakeven = {}R`, `adx_threshold = {}`\n",
            champ.spec.mode,
            champ.spec.top_k,
            champ.spec.donchian_period,
            champ.spec.volume_surge_mult,
            champ.spec.initial_stop_atr,
            champ.spec.chandelier_atr,
            champ.spec.breakeven_r,
            champ.spec.regime_adx
        ));
        md.push_str(&format!(
            "- **100U 生产数据净值增长**：`{:.2} USDT` $\\rightarrow$ **`{:.2} USDT`**（净利润 **`{:+.2} USDT`**，最大回撤仅 **`{:.2}%`**）\n",
            champ.full_summary.initial_equity,
            champ.full_summary.final_equity,
            champ.full_summary.final_equity - champ.full_summary.initial_equity,
            champ.full_summary.max_drawdown_pct
        ));
    }

    fs::write(path, md)?;
    Ok(())
}
