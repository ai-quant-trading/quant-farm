use crate::types::{Candle, MarketRegime};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// 计算指数移动平均线 (EMA)
pub fn ema(values: &[Decimal], period: usize) -> Option<Decimal> {
    if values.len() < period || period == 0 {
        return None;
    }
    let k = dec!(2) / Decimal::from(period + 1);
    let one_minus_k = Decimal::ONE - k;

    // 初始值使用前 period 个样本的简单均值 (SMA)
    let sum: Decimal = values[..period].iter().copied().sum();
    let mut current_ema = sum / Decimal::from(period);

    for &val in &values[period..] {
        current_ema = val * k + current_ema * one_minus_k;
    }
    Some(current_ema)
}

/// 计算平均真实波幅 (ATR)
/// 增量 EMA 序列（index i 处 = 截至第 i 个值的 EMA），前 period-1 个为 None
pub fn ema_series(values: &[Decimal], period: usize) -> Vec<Option<Decimal>> {
    let n = values.len();
    let mut out = vec![None; n];
    if period == 0 || n < period {
        return out;
    }
    let k = dec!(2) / Decimal::from(period + 1);
    let mut e = values[..period].iter().copied().sum::<Decimal>() / Decimal::from(period);
    out[period - 1] = Some(e);
    for i in period..n {
        e = values[i] * k + e * (Decimal::ONE - k);
        out[i] = Some(e);
    }
    out
}

pub fn atr(candles: &[Candle], period: usize) -> Option<Decimal> {
    if candles.len() < period + 1 || period == 0 {
        return None;
    }

    let mut trs = Vec::with_capacity(candles.len() - 1);
    for i in 1..candles.len() {
        let cur = &candles[i];
        let prev_close = candles[i - 1].close;

        let hl = cur.high - cur.low;
        let hc = (cur.high - prev_close).abs();
        let lc = (cur.low - prev_close).abs();

        let tr = hl.max(hc).max(lc);
        trs.push(tr);
    }

    if trs.len() < period {
        return None;
    }

    // Wilder 平滑 ATR
    let p_dec = Decimal::from(period);
    let p_minus_one = Decimal::from(period - 1);
    let init_sum: Decimal = trs[..period].iter().copied().sum();
    let mut current_atr = init_sum / p_dec;

    for &tr in &trs[period..] {
        current_atr = (current_atr * p_minus_one + tr) / p_dec;
    }
    Some(current_atr)
}

/// 一次性预计算整条 ATR 序列 (Wilder 平滑)，复杂度 O(n)。
/// 返回 `Vec<Option<Decimal>>`，只有 index >= period 的位置有值。
/// 用于计算「当前 ATR 在历史窗口中的分位」，避免逐根重复平滑。
pub fn atr_series(candles: &[Candle], period: usize) -> Vec<Option<Decimal>> {
    let n = candles.len();
    let mut out = vec![None; n];
    if n < period + 1 || period == 0 {
        return out;
    }

    let mut trs = vec![Decimal::ZERO; n];
    for i in 1..n {
        let cur = &candles[i];
        let prev_close = candles[i - 1].close;
        let hl = cur.high - cur.low;
        let hc = (cur.high - prev_close).abs();
        let lc = (cur.low - prev_close).abs();
        trs[i] = hl.max(hc).max(lc);
    }

    let init_sum: Decimal = trs[1..=period].iter().copied().sum();
    let p_dec = Decimal::from(period);
    let p_minus_one = Decimal::from(period - 1);
    out[period] = Some(init_sum / p_dec);
    for i in (period + 1)..n {
        let prev = out[i - 1].unwrap();
        out[i] = Some((prev * p_minus_one + trs[i]) / p_dec);
    }
    out
}

/// 计算 `idx` 位置 ATR/收盘价 在过去 `lookback` 根 K 线中的分位 (0-100)。
/// 分位越高代表当前波动率相对历史越极端。
pub fn atr_percentile(
    candles: &[Candle],
    series: &[Option<Decimal>],
    idx: usize,
    lookback: usize,
) -> Option<Decimal> {
    let cur_atr = series.get(idx).copied().flatten()?;
    let cur_px = candles.get(idx)?.close;
    if cur_px <= Decimal::ZERO {
        return None;
    }
    let cur = cur_atr / cur_px;

    let start = idx.saturating_sub(lookback);
    let mut vals = Vec::with_capacity(idx - start);
    for j in start..idx {
        if let (Some(a), Some(c)) = (series.get(j).copied().flatten(), candles.get(j)) {
            if c.close > Decimal::ZERO {
                vals.push(a / c.close);
            }
        }
    }
    if vals.len() < 30 {
        return None;
    }
    let below = vals.iter().filter(|&&v| v < cur).count();
    Some(Decimal::from(below) / Decimal::from(vals.len()) * dec!(100))
}

/// 计算简化版 ADX (平均趋向指数)，用于识别大盘是单边趋势还是横盘震荡
pub fn adx(candles: &[Candle], period: usize) -> Option<Decimal> {
    if candles.len() < period * 2 + 1 || period == 0 {
        return None;
    }

    let mut plus_dm_vec = Vec::with_capacity(candles.len() - 1);
    let mut minus_dm_vec = Vec::with_capacity(candles.len() - 1);
    let mut tr_vec = Vec::with_capacity(candles.len() - 1);

    for i in 1..candles.len() {
        let cur = &candles[i];
        let prev = &candles[i - 1];

        let up_move = cur.high - prev.high;
        let down_move = prev.low - cur.low;

        let plus_dm = if up_move > down_move && up_move > Decimal::ZERO {
            up_move
        } else {
            Decimal::ZERO
        };

        let minus_dm = if down_move > up_move && down_move > Decimal::ZERO {
            down_move
        } else {
            Decimal::ZERO
        };

        let hl = cur.high - cur.low;
        let hc = (cur.high - prev.close).abs();
        let lc = (cur.low - prev.close).abs();
        let tr = hl.max(hc).max(lc);

        plus_dm_vec.push(plus_dm);
        minus_dm_vec.push(minus_dm);
        tr_vec.push(tr);
    }

    let p_dec = Decimal::from(period);
    let p_minus_one = Decimal::from(period - 1);

    let mut smooth_plus_dm: Decimal = plus_dm_vec[..period].iter().copied().sum();
    let mut smooth_minus_dm: Decimal = minus_dm_vec[..period].iter().copied().sum();
    let mut smooth_tr: Decimal = tr_vec[..period].iter().copied().sum();

    let mut dx_values = Vec::new();

    for i in period..tr_vec.len() {
        smooth_plus_dm = smooth_plus_dm - (smooth_plus_dm / p_dec) + plus_dm_vec[i];
        smooth_minus_dm = smooth_minus_dm - (smooth_minus_dm / p_dec) + minus_dm_vec[i];
        smooth_tr = smooth_tr - (smooth_tr / p_dec) + tr_vec[i];

        if smooth_tr <= Decimal::ZERO {
            continue;
        }

        let plus_di = (smooth_plus_dm / smooth_tr) * dec!(100);
        let minus_di = (smooth_minus_dm / smooth_tr) * dec!(100);
        let di_sum = plus_di + minus_di;
        if di_sum > Decimal::ZERO {
            let dx = ((plus_di - minus_di).abs() / di_sum) * dec!(100);
            dx_values.push(dx);
        }
    }

    if dx_values.len() < period {
        return None;
    }

    let init_adx_sum: Decimal = dx_values[..period].iter().copied().sum();
    let mut current_adx = init_adx_sum / p_dec;
    for &dx in &dx_values[period..] {
        current_adx = (current_adx * p_minus_one + dx) / p_dec;
    }
    Some(current_adx)
}

/// 计算过去 period 根 K 线的最高价 (唐奇安通道上轨，不含当前 K 线)
pub fn donchian_high(candles: &[Candle], period: usize) -> Option<Decimal> {
    if candles.len() < period || period == 0 {
        return None;
    }
    let slice = &candles[candles.len() - period..];
    slice.iter().map(|c| c.high).max()
}

/// 计算过去 period 根 K 线的最低价 (唐奇安通道下轨，不含当前 K 线)
pub fn donchian_low(candles: &[Candle], period: usize) -> Option<Decimal> {
    if candles.len() < period || period == 0 {
        return None;
    }
    let slice = &candles[candles.len() - period..];
    slice.iter().map(|c| c.low).min()
}

/// 计算过去 period 根 K 线的平均成交量
pub fn avg_volume(candles: &[Candle], period: usize) -> Option<Decimal> {
    if candles.len() < period || period == 0 {
        return None;
    }
    let slice = &candles[candles.len() - period..];
    let sum: Decimal = slice.iter().map(|c| c.volume).sum();
    Some(sum / Decimal::from(period))
}

/// 第一层漏斗：基于基准币种 (BTCUSDT 1h K线) 判定当前大盘状态
pub fn detect_market_regime(btc_candles: &[Candle], adx_threshold: Decimal) -> MarketRegime {
    if btc_candles.len() < 205 {
        return MarketRegime::Chop;
    }
    let closes: Vec<Decimal> = btc_candles.iter().map(|c| c.close).collect();
    let last_close = match closes.last() {
        Some(&c) => c,
        None => return MarketRegime::Chop,
    };

    let ema_fast = match ema(&closes, 50) {
        Some(v) => v,
        None => return MarketRegime::Chop,
    };
    let ema_slow = match ema(&closes, 150) {
        Some(v) => v,
        None => return MarketRegime::Chop,
    };
    let current_adx = adx(btc_candles, 14).unwrap_or(Decimal::ZERO);

    if current_adx < adx_threshold {
        return MarketRegime::Chop;
    }

    if last_close > ema_slow && ema_fast > ema_slow {
        MarketRegime::Bull
    } else if last_close < ema_slow && ema_fast < ema_slow {
        MarketRegime::Bear
    } else {
        MarketRegime::Chop
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ema_and_donchian() {
        let vals = vec![
            dec!(10.0),
            dec!(11.0),
            dec!(12.0),
            dec!(13.0),
            dec!(14.0),
            dec!(15.0),
        ];
        let e = ema(&vals, 3).unwrap();
        assert!(e > dec!(13.0) && e < dec!(15.0));
    }
}
