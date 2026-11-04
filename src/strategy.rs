use crate::config::{StrategyConfig, StrategyMode};
use crate::indicators::{
    atr, atr_percentile, atr_series, avg_volume, donchian_high, donchian_low, ema,
};
use crate::types::{Candle, ExitReason, MarketRegime, Position, Side, SymbolSpec};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct SignalCandidate {
    pub symbol: String,
    pub side: Side,
    pub score: Decimal,
    pub signal_close: Decimal,
    pub proposed_stop: Decimal,
    pub atr_val: Decimal,
}

/// 第二层漏斗：计算单币种相对于基准币种 (BTC) 的截面动量与放量得分
pub fn compute_cross_sectional_score(
    coin_candles: &[Candle],
    btc_candles: &[Candle],
) -> Option<Decimal> {
    if coin_candles.len() < 75 || btc_candles.len() < 75 {
        return None;
    }

    let c_now = coin_candles.last()?.close;
    let c_24h = coin_candles[coin_candles.len() - 25].close;
    let c_72h = coin_candles[coin_candles.len() - 73].close;

    let b_now = btc_candles.last()?.close;
    let b_24h = btc_candles[btc_candles.len() - 25].close;
    let b_72h = btc_candles[btc_candles.len() - 73].close;

    if c_24h <= Decimal::ZERO
        || c_72h <= Decimal::ZERO
        || b_24h <= Decimal::ZERO
        || b_72h <= Decimal::ZERO
    {
        return None;
    }

    // 相对于 BTC 的超额收益率 (Alpha Return)
    let r_24h = ((c_now - c_24h) / c_24h) - ((b_now - b_24h) / b_24h);
    let r_72h = ((c_now - c_72h) / c_72h) - ((b_now - b_72h) / b_72h);

    // 近期 6 小时成交量相对于 48 小时均量的放大比率
    let recent_vol = avg_volume(coin_candles, 6).unwrap_or(Decimal::ONE);
    let base_vol = avg_volume(&coin_candles[..coin_candles.len() - 6], 48).unwrap_or(Decimal::ONE);
    let vol_ratio = if base_vol > Decimal::ZERO {
        ((recent_vol / base_vol) - Decimal::ONE).min(dec!(2.0))
    } else {
        Decimal::ZERO
    };

    let score = dec!(0.45) * r_24h + dec!(0.35) * r_72h + dec!(0.20) * (vol_ratio * dec!(0.1));
    Some(score)
}

/// 检测波动率是否经历收敛后爆发 (Volatility Squeeze)
fn is_volatility_squeeze_breakout(candles: &[Candle], atr_period: usize) -> bool {
    if candles.len() < atr_period + 30 {
        return false;
    }
    let cur_atr = match atr(candles, atr_period) {
        Some(a) if a > Decimal::ZERO => a,
        _ => return false,
    };
    let older_slice = &candles[..candles.len() - 10];
    let older_atr = match atr(older_slice, atr_period) {
        Some(a) if a > Decimal::ZERO => a,
        _ => return false,
    };
    // 近期 ATR 相比 10 小时前处于收敛或温和扩张区间 (< 1.25x)，且当根 K 线实体明显大于平均 ATR
    let last = candles.last().unwrap();
    let body = (last.close - last.open).abs();
    (cur_atr <= older_atr * dec!(1.25)) && (body >= cur_atr * dec!(0.75))
}

/// 检测强势多头趋势中的缩量回踩 EMA20 二次启动信号 (Trend Pullback Continuation)
fn is_bull_pullback_continuation(candles: &[Candle], ema20: Decimal, ema50: Decimal, cur_atr: Decimal) -> bool {
    if candles.len() < 10 || ema20 <= ema50 {
        return false;
    }
    let last = candles.last().unwrap();
    let prev = &candles[candles.len() - 2];

    // 前 1~3 根 K 线最低价曾回踩触及 EMA20 附近 (1.0 * ATR 以内)，且当前 K 线收阳并重新站稳 EMA20 之上
    let touched_ema20 = (prev.low - ema20).abs() <= cur_atr * dec!(0.9)
        || (last.low - ema20).abs() <= cur_atr * dec!(0.8);
    let bullish_rebound = last.close > last.open
        && last.close > prev.high
        && last.close > ema20
        && (last.close - last.open) >= cur_atr * dec!(0.45);

    touched_ema20 && bullish_rebound
}

/// 第三层与第四层漏斗：支持 5 种策略模式生成信号
/// 各模式的初始止损 ATR 倍数（MR 用紧凑止损，趋势模式沿用 initial_stop_atr_mult）
pub fn stop_atr_mult(cfg: &StrategyConfig) -> Decimal {
    match cfg.mode {
        StrategyMode::ChopMeanReversion => cfg.chop_mr_stop_atr,
        StrategyMode::BearShort => cfg.bear_stop_atr,
        _ => cfg.initial_stop_atr_mult,
    }
}

/// 固定止盈 ATR 倍数（仅 MR/BearShort 有；其余模式 ZERO = 禁用，维持纯移动止损口径与历史回测一致）
pub fn fixed_tp_atr_mult(cfg: &StrategyConfig) -> Decimal {
    match cfg.mode {
        StrategyMode::ChopMeanReversion => cfg.chop_mr_tp_atr,
        StrategyMode::BearShort => cfg.bear_tp_atr,
        _ => Decimal::ZERO,
    }
}

/// 实盘/paper 引擎的挂单止盈倍数（MR 用 chop_mr_tp_atr；其余模式沿用 chandelier_atr_mult 不变）
pub fn live_tp_atr_mult(cfg: &StrategyConfig) -> Decimal {
    match cfg.mode {
        StrategyMode::ChopMeanReversion => cfg.chop_mr_tp_atr,
        StrategyMode::BearShort => cfg.bear_tp_atr,
        _ => cfg.chandelier_atr_mult,
    }
}

/// 各模式的风险缩放系数：BearShort 未经验证期按半风险（沿袭危机空单模板审慎原则），其余 1.0
pub fn risk_scale_factor(cfg: &StrategyConfig) -> Decimal {
    match cfg.mode {
        StrategyMode::BearShort => dec!(0.5),
        _ => Decimal::ONE,
    }
}

/// 该持仓是否走「成熟熊市空头」参数组。
/// `RegimeRouter` 模式下短单自动归入熊市臂，长单归入趋势臂。
pub fn is_bear_arm(cfg: &StrategyConfig, side: Side) -> bool {
    match cfg.mode {
        StrategyMode::BearShort => true,
        StrategyMode::RegimeRouter => side == Side::Short,
        _ => false,
    }
}

pub fn stop_atr_mult_for(cfg: &StrategyConfig, side: Side) -> Decimal {
    if is_bear_arm(cfg, side) {
        cfg.bear_stop_atr
    } else {
        stop_atr_mult(cfg)
    }
}

pub fn fixed_tp_atr_mult_for(cfg: &StrategyConfig, side: Side) -> Decimal {
    if is_bear_arm(cfg, side) {
        cfg.bear_tp_atr
    } else {
        fixed_tp_atr_mult(cfg)
    }
}

pub fn live_tp_atr_mult_for(cfg: &StrategyConfig, side: Side) -> Decimal {
    if is_bear_arm(cfg, side) {
        cfg.bear_tp_atr
    } else {
        live_tp_atr_mult(cfg)
    }
}

pub fn risk_scale_factor_for(cfg: &StrategyConfig, side: Side) -> Decimal {
    if is_bear_arm(cfg, side) {
        dec!(0.5)
    } else {
        risk_scale_factor(cfg)
    }
}

pub fn time_stop_bars_for(cfg: &StrategyConfig, side: Side) -> usize {
    if is_bear_arm(cfg, side) {
        cfg.bear_time_stop_bars
    } else if cfg.mode == StrategyMode::ChopMeanReversion {
        cfg.chop_mr_time_stop_bars
    } else {
        cfg.time_stop_bars
    }
}

/// Chop 均值回归扫描：区间边缘 + 均值拉伸 + 缩量（放量=突破能量，MR 不接飞刀）。
/// 与趋势扫描完全独立：不用截面动量排名、不用乖离闸门（拉伸本身就是入场理由）。
fn scan_chop_mr_signals(
    universe_candles: &HashMap<String, &[Candle]>,
    cfg: &StrategyConfig,
) -> Vec<SignalCandidate> {
    let mut cands: Vec<(String, Side, Decimal, Decimal)> = Vec::new(); // (symbol, side, stretch_z, atr)

    for symbol in &cfg.universe {
        if cfg.execution_blacklist.iter().any(|b| b == symbol) {
            continue;
        }
        let candles = match universe_candles.get(symbol) {
            Some(&c) if c.len() > cfg.donchian_period + 55 => c,
            _ => continue,
        };
        let last_candle = match candles.last() {
            Some(c) => c,
            None => continue,
        };
        let prev_candles = &candles[..candles.len() - 1];

        let cur_atr = match atr(candles, cfg.atr_period) {
            Some(a) if a > Decimal::ZERO => a,
            _ => continue,
        };
        let avg_vol = match avg_volume(prev_candles, cfg.donchian_period) {
            Some(v) if v > Decimal::ZERO => v,
            _ => continue,
        };
        let closes: Vec<Decimal> = candles.iter().map(|c| c.close).collect();
        let ema20 = match ema(&closes, 20) {
            Some(e) if e > Decimal::ZERO => e,
            _ => continue,
        };
        let dc_high = match donchian_high(prev_candles, cfg.donchian_period) {
            Some(v) => v,
            None => continue,
        };
        let dc_low = match donchian_low(prev_candles, cfg.donchian_period) {
            Some(v) => v,
            None => continue,
        };

        // 区间有效性：Donchian 宽度占价格比例必须在 [min, max] —— 过窄是死水，过宽已是趋势
        let range_width = (dc_high - dc_low) / last_candle.close;
        if range_width < cfg.chop_mr_range_min_pct || range_width > cfg.chop_mr_range_max_pct {
            continue;
        }

        // 均值拉伸 z 值（以 ATR 为单位）：负 = 超卖偏离，正 = 超买偏离
        if ema20 <= Decimal::ZERO || cur_atr <= Decimal::ZERO {
            continue;
        }
        let stretch = (last_candle.close - ema20) / cur_atr;

        // 缩量确认：放量意味着区间即将被突破（无论是谁的能量），MR 此时不接飞刀
        let quiet_volume = last_candle.volume < avg_vol * cfg.volume_surge_mult;

        // 贴近区间下轨 + 深度超卖拉伸 + 缩量 → 做多博回归
        let near_low = last_candle.close <= dc_low + cur_atr * dec!(0.15);
        if quiet_volume && near_low && stretch <= -cfg.chop_mr_stretch_atr {
            cands.push((symbol.clone(), Side::Long, -stretch, cur_atr));
            continue;
        }

        // 贴近区间上轨 + 深度超买拉伸 + 缩量 → 做空博回归
        let near_high = last_candle.close >= dc_high - cur_atr * dec!(0.15);
        if quiet_volume && near_high && stretch >= cfg.chop_mr_stretch_atr {
            cands.push((symbol.clone(), Side::Short, stretch, cur_atr));
        }
    }

    // 拉伸最深者优先；每边最多 top_k_momentum 个（多空各自计数）
    cands.sort_by(|a, b| b.2.cmp(&a.2));
    let mut long_taken = 0usize;
    let mut short_taken = 0usize;
    let mut signals = Vec::new();
    for (symbol, side, stretch_z, cur_atr) in cands {
        let slot_ok = if side == Side::Long {
            long_taken < cfg.top_k_momentum
        } else {
            short_taken < cfg.top_k_momentum
        };
        if !slot_ok {
            continue;
        }
        let candles = universe_candles.get(&symbol).copied().unwrap_or(&[]);
        let close = match candles.last() {
            Some(c) => c.close,
            None => continue,
        };
        let spec = SymbolSpec::default_for(&symbol);
        let stop_mult = stop_atr_mult(cfg);
        let proposed_stop = match side {
            Side::Long => spec.round_price(close - cur_atr * stop_mult),
            Side::Short => spec.round_price(close + cur_atr * stop_mult),
        };
        let stop_valid = match side {
            Side::Long => proposed_stop > Decimal::ZERO && proposed_stop < close,
            Side::Short => proposed_stop > close,
        };
        if stop_valid {
            if side == Side::Long {
                long_taken += 1;
            } else {
                short_taken += 1;
            }
            signals.push(SignalCandidate {
                symbol,
                side,
                score: stretch_z,
                signal_close: close,
                proposed_stop,
                atr_val: cur_atr,
            });
        }
    }
    signals
}

/// 计算BTC尾部连续空头排列（EMA50<EMA150 且 close<EMA150）的小时数——熊市成熟度。
/// 治"9 小时噪音"病：只在确立的大熊市做空， episodic 短熊腿不接。
/// 增量 EMA 一次线性扫出，O(n)，回测每根调用也轻量。
fn btc_bear_maturity_bars(btc_candles: &[Candle]) -> usize {
    let closes: Vec<Decimal> = btc_candles.iter().map(|c| c.close).collect();
    if closes.len() < 150 {
        return 0;
    }
    let k50 = dec!(2) / Decimal::from(51u32);
    let k150 = dec!(2) / Decimal::from(151u32);
    let mut e50 = closes[..50].iter().copied().sum::<Decimal>() / Decimal::from(50u32);
    let mut e150 = closes[..150].iter().copied().sum::<Decimal>() / Decimal::from(150u32);
    // 对齐状态序列：i 处为「截至第 i 根收盘」的空头排列判定（i 从 149 起）
    let mut bear_flags: Vec<bool> = Vec::with_capacity(closes.len());
    bear_flags.push(e50 < e150 && closes[149] < e150);
    for i in 150..closes.len() {
        e50 = closes[i] * k50 + e50 * (Decimal::ONE - k50);
        e150 = closes[i] * k150 + e150 * (Decimal::ONE - k150);
        bear_flags.push(e50 < e150 && closes[i] < e150);
    }
    // 从尾部数连续 true
    let mut run = 0usize;
    for f in bear_flags.iter().rev() {
        if *f {
            run += 1;
        } else {
            break;
        }
    }
    run
}

/// 成熟熊市空头扫描：熊市成熟度门 + 危机护栏 (ATR 分位) + 两种入场变体。
/// V1 顺势压破 (breakdown)：破 Donchian 下轨 + 放量 + 弱势币（截面动量最弱）。
/// V2 拉升衰竭 (rejection)：反弹至 EMA20 附近 + 阴线拒绝 + 缩量（无人接盘的弱反弹）——
/// 治验尸报告的核心死因「在放量破位高潮后卖在局部底」。
fn scan_bear_short_signals(
    universe_candles: &HashMap<String, &[Candle]>,
    btc_candles: &[Candle],
    cfg: &StrategyConfig,
) -> Vec<SignalCandidate> {
    // 门 1: 熊市成熟度——BTC 已连续空头排列 >= bear_maturity_bars 小时
        if btc_bear_maturity_bars(btc_candles) < cfg.bear_maturity_bars {
        return Vec::new();
    }
    // 门 2: 危机护栏——BTC ATR 分位过高（波动爆炸=轧空风险）时禁止新空 (书: Bear≠Crisis)
    if cfg.bear_max_atr_percentile > Decimal::ZERO {
        let series = atr_series(btc_candles, cfg.atr_period);
        if let Some(idx) = btc_candles.len().checked_sub(1) {
            if let Some(p) =
                atr_percentile(btc_candles, &series, idx, cfg.atr_percentile_lookback)
            {
                if p >= cfg.bear_max_atr_percentile {
                    return Vec::new();
                }
            }
        }
    }

    let variant = cfg.bear_entry_variant.as_str();
    let allow_breakdown = variant == "breakdown" || variant == "both";
    let allow_rejection = variant == "rejection" || variant == "both";

    let mut cands: Vec<(String, Decimal, Decimal)> = Vec::new(); // (symbol, |score|, atr)
    for symbol in &cfg.universe {
        if cfg.execution_blacklist.iter().any(|b| b == symbol) {
            continue;
        }
        let candles = match universe_candles.get(symbol) {
            Some(&c) if c.len() > cfg.donchian_period + 55 => c,
            _ => continue,
        };
        let last_candle = match candles.last() {
            Some(c) => c,
            None => continue,
        };
        let prev_candles = &candles[..candles.len() - 1];
        let cur_atr = match atr(candles, cfg.atr_period) {
            Some(a) if a > Decimal::ZERO => a,
            _ => continue,
        };
        let avg_vol = match avg_volume(prev_candles, cfg.donchian_period) {
            Some(v) if v > Decimal::ZERO => v,
            _ => continue,
        };
        let closes: Vec<Decimal> = candles.iter().map(|c| c.close).collect();
        let ema20 = match ema(&closes, 20) {
            Some(e) if e > Decimal::ZERO => e,
            _ => continue,
        };
        let ema60 = match ema(&closes, 60) {
            Some(e) if e > Decimal::ZERO => e,
            _ => continue,
        };
        // 标的自身必须深陷空头结构：均线空头排列 + 价格在 EMA20 下方（顺势，不接高潮）
        if ema20 >= ema60 || last_candle.close >= ema20 {
            continue;
        }
        // 乖离闸门：不允许在暴跌末端追空（离均线太远的拉伸）
        let bias = (last_candle.close - ema20).abs() / ema20;
        if bias > cfg.max_bias_pct {
            continue;
        }

        let triggered = if allow_breakdown {
            // V1 顺势压破：破 20 期低点 + 放量确认（1.3×，砍掉旧臂反常的 1.69×）
            if let Some(prev_low) = donchian_low(prev_candles, cfg.donchian_period) {
                if last_candle.close < prev_low
                    && last_candle.volume >= avg_vol * cfg.volume_surge_mult
                {
                    true
                } else {
                    allow_rejection && bear_rejection_setup(last_candle, &ema20, cur_atr, avg_vol, cfg)
                }
            } else {
                allow_rejection && bear_rejection_setup(last_candle, &ema20, cur_atr, avg_vol, cfg)
            }
        } else {
            allow_rejection && bear_rejection_setup(last_candle, &ema20, cur_atr, avg_vol, cfg)
        };
        if !triggered {
            continue;
        }

        // 弱势优先：截面动量最弱（对 BTC 超额收益最负）的币优先做空
        let score = compute_cross_sectional_score(candles, btc_candles)
            .unwrap_or(Decimal::ZERO);
        if score >= Decimal::ZERO {
            continue; // 做空只选跑输大盘的标的
        }
        cands.push((symbol.clone(), score.abs(), cur_atr));
    }

    // 弱势排序（score.abs() 最大 = 超跌最多 = 最弱）取 top_k
    cands.sort_by(|a, b| b.1.cmp(&a.1));
    let mut signals = Vec::new();
    for (symbol, weak, cur_atr) in cands.into_iter().take(cfg.top_k_momentum) {
        let candles = match universe_candles.get(&symbol) {
            Some(&c) => c,
            None => continue,
        };
        let close = match candles.last() {
            Some(c) => c.close,
            None => continue,
        };
        let spec = SymbolSpec::default_for(&symbol);
        let stop_mult = stop_atr_mult(cfg);
        let proposed_stop = spec.round_price(close + cur_atr * stop_mult);
        if proposed_stop > close {
            signals.push(SignalCandidate {
                symbol,
                side: Side::Short,
                score: weak,
                signal_close: close,
                proposed_stop,
                atr_val: cur_atr,
            });
        }
    }
    signals
}

/// V2 拉升衰竭形态：价格反弹至 EMA20 附近 (±0.5×ATR) + 当根阴线拒绝 + 缩量
fn bear_rejection_setup(
    last: &Candle,
    ema20: &Decimal,
    cur_atr: Decimal,
    avg_vol: Decimal,
    cfg: &StrategyConfig,
) -> bool {
    let near_ema = (last.close - ema20).abs() <= cur_atr * dec!(0.5);
    let red_candle = last.close < last.open;
    let quiet_volume = last.volume < avg_vol * cfg.volume_surge_mult;
    near_ema && red_candle && quiet_volume
}

pub fn scan_entry_signals(
    regime: MarketRegime,
    universe_candles: &HashMap<String, &[Candle]>,
    btc_candles: &[Candle],
    cfg: &StrategyConfig,
) -> Vec<SignalCandidate> {
    // 大盘状态路由模式：Bull 用已验证的突破多头臂，Bear 用已验证的成熟熊市空头臂，Chop 空仓。
    // 两条臂各自独立回测过，组合只共享账户与风控，不共享参数。
    if cfg.mode == StrategyMode::RegimeRouter {
        return match regime {
            MarketRegime::Bull => scan_entry_signals(
                MarketRegime::Bull,
                universe_candles,
                btc_candles,
                &StrategyConfig {
                    mode: StrategyMode::LongOnlyBreakout,
                    ..cfg.clone()
                },
            ),
            MarketRegime::Bear => scan_bear_short_signals(universe_candles, btc_candles, cfg),
            MarketRegime::Chop => Vec::new(),
        };
    }

    // 震荡均值回归模式：Chop 状态下走独立的区间边缘扫描（不穿越趋势排名/乖离闸门）
    if cfg.mode == StrategyMode::ChopMeanReversion {
        if regime == MarketRegime::Chop {
            return scan_chop_mr_signals(universe_candles, cfg);
        }
        // Bull/Bear 是趋势的领地，MR 绝不出手（区间已破位）
        return Vec::new();
    }
    // 成熟熊市空头模式：只在 Bear 状态出手，走独立扫描（成熟度门 + 危机护栏）
    if cfg.mode == StrategyMode::BearShort {
        if regime == MarketRegime::Bear {
            return scan_bear_short_signals(universe_candles, btc_candles, cfg);
        }
        return Vec::new();
    }
    if regime == MarketRegime::Chop {
        return Vec::new();
    }
    if cfg.mode == StrategyMode::LongOnlyBreakout && regime != MarketRegime::Bull {
        return Vec::new();
    }

    // 1. 计算全市场候选币种的截面动量排名
    let mut scored_coins: Vec<(String, Decimal)> = Vec::new();
    for symbol in &cfg.universe {
        if let Some(&candles) = universe_candles.get(symbol) {
            if let Some(score) = compute_cross_sectional_score(candles, btc_candles) {
                scored_coins.push((symbol.clone(), score));
            }
        }
    }

    match regime {
        MarketRegime::Bull => {
            scored_coins.sort_by(|a, b| b.1.cmp(&a.1));
        }
        MarketRegime::Bear => {
            scored_coins.sort_by(|a, b| a.1.cmp(&b.1));
        }
        MarketRegime::Chop => return Vec::new(),
    }

    let top_candidates = scored_coins.into_iter().take(cfg.top_k_momentum);
    let mut signals = Vec::new();

    for (symbol, score) in top_candidates {
        // 若该币种在数据库点差/胜率黑名单中，仅作为截面排名标尺，绝不实际开仓下单！
        if cfg.execution_blacklist.iter().any(|b| b == &symbol) {
            continue;
        }
        let candles = match universe_candles.get(&symbol) {
            Some(&c) if c.len() > cfg.donchian_period + 55 => c,
            _ => continue,
        };

        let last_candle = match candles.last() {
            Some(c) => c,
            None => continue,
        };
        let prev_candles = &candles[..candles.len() - 1];

        let cur_atr = match atr(candles, cfg.atr_period) {
            Some(a) if a > Decimal::ZERO => a,
            _ => continue,
        };

        // 波动率过滤器（默认关闭）：只在「当前 ATR 分位 >= 阈值」的高波动环境开仓。
        // 依据 `regime-report` 的全样本回测与前后半段交叉验证结论启用，避免低波动假突破磨损。
        if cfg.min_atr_percentile > Decimal::ZERO {
            let series = atr_series(candles, cfg.atr_period);
            let idx = candles.len() - 1;
            match atr_percentile(candles, &series, idx, cfg.atr_percentile_lookback) {
                Some(p) if p >= cfg.min_atr_percentile => {}
                _ => continue,
            }
        }

        let avg_vol = match avg_volume(prev_candles, cfg.donchian_period) {
            Some(v) if v > Decimal::ZERO => v,
            _ => continue,
        };

        let closes: Vec<Decimal> = candles.iter().map(|c| c.close).collect();
        let ema20 = match ema(&closes, 20) {
            Some(e) if e > Decimal::ZERO => e,
            _ => continue,
        };
        let ema60 = match ema(&closes, 60) {
            Some(e) if e > Decimal::ZERO => e,
            _ => continue,
        };

        // 乖离率检查：防止在暴涨/暴跌末端追单
        let bias = (last_candle.close - ema20).abs() / ema20;
        if bias > cfg.max_bias_pct {
            continue;
        }

        let spec = SymbolSpec::default_for(&symbol);

        match regime {
            MarketRegime::Bull => {
                // 要求具备真实截面超额动量 (score >= 0.012) 且当根突破 K 线为阳线 (收盘 > 开盘 > EMA20)
                if score < dec!(0.012)
                    || last_candle.close <= ema20
                    || last_candle.close <= last_candle.open
                {
                    continue;
                }
                let prev_high = match donchian_high(prev_candles, cfg.donchian_period) {
                    Some(h) => h,
                    None => continue,
                };
                let is_donchian = last_candle.close > prev_high
                    && last_candle.volume >= avg_vol * cfg.volume_surge_mult;
                let is_squeeze = is_donchian
                    && is_volatility_squeeze_breakout(candles, cfg.atr_period);
                let is_pullback =
                    is_bull_pullback_continuation(candles, ema20, ema60, cur_atr)
                        && last_candle.volume >= avg_vol * dec!(1.1);
                let is_funding_squeeze = last_candle.funding_rate <= dec!(-0.0020)
                    && last_candle.close > ema20;

                let triggered = match cfg.mode {
                    StrategyMode::AllWeatherBreakout | StrategyMode::LongOnlyBreakout => {
                        is_donchian || is_funding_squeeze
                    }
                    StrategyMode::VolatilitySqueeze => is_squeeze || is_funding_squeeze,
                    StrategyMode::TrendPullback => is_pullback,
                    StrategyMode::EnsembleComposite => {
                        is_donchian || is_pullback || is_funding_squeeze
                    }
                    // MR 模式在函数入口已分流到 scan_chop_mr_signals，趋势臂不可达
                    StrategyMode::ChopMeanReversion => false,
                    // BearShort 模式在函数入口已分流到 scan_bear_short_signals，趋势臂不可达
                    StrategyMode::BearShort => false,
                    // RegimeRouter 在函数入口已按大盘状态分流，不会走到趋势臂的 mode 判断
                    StrategyMode::RegimeRouter => false,
                };

                if triggered {
                    let raw_stop = last_candle.close - (cur_atr * cfg.initial_stop_atr_mult);
                    let proposed_stop = spec.round_price(raw_stop);
                    if proposed_stop > Decimal::ZERO && proposed_stop < last_candle.close {
                        signals.push(SignalCandidate {
                            symbol,
                            side: Side::Long,
                            score,
                            signal_close: last_candle.close,
                            proposed_stop,
                            atr_val: cur_atr,
                        });
                    }
                }
            }
            MarketRegime::Bear => {
                if cfg.mode == StrategyMode::LongOnlyBreakout {
                    continue;
                }
                if score >= dec!(-0.01) || score < dec!(-0.08) || ema20 >= ema60 {
                    continue;
                }
                let prev_low = match donchian_low(prev_candles, cfg.donchian_period) {
                    Some(l) => l,
                    None => continue,
                };
                let is_breakdown = last_candle.close < prev_low
                    && last_candle.volume >= avg_vol * (cfg.volume_surge_mult * dec!(1.3));
                let is_squeeze_short = is_breakdown
                    && is_volatility_squeeze_breakout(candles, cfg.atr_period);

                let triggered = match cfg.mode {
                    StrategyMode::VolatilitySqueeze => is_squeeze_short,
                    _ => is_breakdown,
                };

                if triggered {
                    let raw_stop = last_candle.close + (cur_atr * cfg.initial_stop_atr_mult);
                    let proposed_stop = spec.round_price(raw_stop);
                    if proposed_stop > last_candle.close {
                        signals.push(SignalCandidate {
                            symbol,
                            side: Side::Short,
                            score: score.abs(),
                            signal_close: last_candle.close,
                            proposed_stop,
                            atr_val: cur_atr,
                        });
                    }
                }
            }
            MarketRegime::Chop => {}
        }
    }

    signals
}

/// 持仓收盘后更新：更新移动止盈线 (Chandelier Exit)、保本移位 (Breakeven) 与时间止损检查
pub fn update_position_on_bar_close(
    pos: &mut Position,
    candle: &Candle,
    current_atr: Decimal,
    regime: MarketRegime,
    cfg: &StrategyConfig,
    round_trip_fee_pct: Decimal,
) -> Option<ExitReason> {
    pos.bars_held += 1;
    let spec = SymbolSpec::default_for(&pos.symbol);

    // 1. 计算当前 R 倍数 (浮盈距离 / 初始止损距离)
    let price_diff = match pos.side {
        Side::Long => candle.close - pos.entry_price,
        Side::Short => pos.entry_price - candle.close,
    };
    let current_r = if pos.initial_risk_per_unit > Decimal::ZERO {
        price_diff / pos.initial_risk_per_unit
    } else {
        Decimal::ZERO
    };

    // 2. 大盘状态逆转平仓保护
    //    MR 仓位属于震荡：大盘一旦离开 Chop（趋势启动=区间破位），无论盈亏立即离场防压路机
    if cfg.mode == StrategyMode::ChopMeanReversion {
        if regime != MarketRegime::Chop {
            return Some(ExitReason::RegimeFlip);
        }
    } else if is_bear_arm(cfg, pos.side) {
        // 空头仓位属于熊市：大盘离开 Bear（含转 Chop——验尸报告确认 Bear→Chop 不解救是旧臂死因之一）
        // 且浮盈不足 0.5R 时立即撤退，不吃熊市反弹的逆势回撤
        if pos.side == Side::Short && regime != MarketRegime::Bear && current_r < dec!(0.5) {
            return Some(ExitReason::RegimeFlip);
        }
    } else if (pos.side == Side::Long && regime == MarketRegime::Bear && current_r < dec!(0.5))
        || (pos.side == Side::Short && regime == MarketRegime::Bull && current_r < dec!(0.5))
    {
        return Some(ExitReason::RegimeFlip);
    }

    // 3. 时间止损（MR/BearShort 用各自更短的专用窗口：不成立就走）
    let time_stop_bars = time_stop_bars_for(cfg, pos.side);
    if pos.bars_held >= time_stop_bars && current_r < cfg.time_stop_min_r {
        return Some(ExitReason::TimeStop);
    }

    // 4. 保本止损移位 (Breakeven)
    if !pos.breakeven_activated && current_r >= cfg.breakeven_r_mult {
        pos.breakeven_activated = true;
        let fee_buffer = pos.entry_price * (round_trip_fee_pct + dec!(0.0005));
        match pos.side {
            Side::Long => {
                let be_stop = spec.round_price(pos.entry_price + fee_buffer);
                if be_stop > pos.active_stop {
                    pos.active_stop = be_stop;
                }
            }
            Side::Short => {
                let be_stop = spec.round_price(pos.entry_price - fee_buffer);
                if be_stop < pos.active_stop {
                    pos.active_stop = be_stop;
                }
            }
        }
    }

    // 5. 吊灯移动止盈 (Chandelier Trailing Stop)
    match pos.side {
        Side::Long => {
            if candle.close > pos.highest_close {
                pos.highest_close = candle.close;
            }
            if pos.breakeven_activated && current_atr > Decimal::ZERO {
                let chandelier_stop = spec
                    .round_price(pos.highest_close - current_atr * cfg.chandelier_atr_mult);
                if chandelier_stop > pos.active_stop {
                    pos.active_stop = chandelier_stop;
                }
            }
        }
        Side::Short => {
            if candle.close < pos.lowest_close {
                pos.lowest_close = candle.close;
            }
            if pos.breakeven_activated && current_atr > Decimal::ZERO {
                let chandelier_stop = spec
                    .round_price(pos.lowest_close + current_atr * cfg.chandelier_atr_mult);
                if chandelier_stop < pos.active_stop {
                    pos.active_stop = chandelier_stop;
                }
            }
        }
    }

    None
}