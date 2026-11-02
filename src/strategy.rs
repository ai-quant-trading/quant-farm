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
        _ => cfg.initial_stop_atr_mult,
    }
}

/// 固定止盈 ATR 倍数（仅 MR 有；其余模式 ZERO = 禁用，维持纯移动止损口径与历史回测一致）
pub fn fixed_tp_atr_mult(cfg: &StrategyConfig) -> Decimal {
    match cfg.mode {
        StrategyMode::ChopMeanReversion => cfg.chop_mr_tp_atr,
        _ => Decimal::ZERO,
    }
}

/// 实盘/paper 引擎的挂单止盈倍数（MR 用 chop_mr_tp_atr；其余模式沿用 chandelier_atr_mult 不变）
pub fn live_tp_atr_mult(cfg: &StrategyConfig) -> Decimal {
    match cfg.mode {
        StrategyMode::ChopMeanReversion => cfg.chop_mr_tp_atr,
        _ => cfg.chandelier_atr_mult,
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

pub fn scan_entry_signals(
    regime: MarketRegime,
    universe_candles: &HashMap<String, &[Candle]>,
    btc_candles: &[Candle],
    cfg: &StrategyConfig,
) -> Vec<SignalCandidate> {
    // 震荡均值回归模式：Chop 状态下走独立的区间边缘扫描（不穿越趋势排名/乖离闸门）
    if cfg.mode == StrategyMode::ChopMeanReversion {
        if regime == MarketRegime::Chop {
            return scan_chop_mr_signals(universe_candles, cfg);
        }
        // Bull/Bear 是趋势的领地，MR 绝不出手（区间已破位）
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
    } else if (pos.side == Side::Long && regime == MarketRegime::Bear && current_r < dec!(0.5))
        || (pos.side == Side::Short && regime == MarketRegime::Bull && current_r < dec!(0.5))
    {
        return Some(ExitReason::RegimeFlip);
    }

    // 3. 时间止损（MR 用更短的专用窗口：均值回归不成立就走）
    let time_stop_bars = if cfg.mode == StrategyMode::ChopMeanReversion {
        cfg.chop_mr_time_stop_bars
    } else {
        cfg.time_stop_bars
    };
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
