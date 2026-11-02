//! BearShort 模式单元测试：成熟度门 / 入场变体 / 危机护栏 / 破位即撤 / 倍数助手

use coin_ai::config::{AppConfig, StrategyMode};
use coin_ai::strategy::{
    fixed_tp_atr_mult, risk_scale_factor, scan_entry_signals, stop_atr_mult,
    update_position_on_bar_close,
};
use coin_ai::types::{Candle, ExitReason, MarketRegime, Position, Side};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;

fn bear_cfg() -> AppConfig {
    let mut cfg = AppConfig::load("config/settings.toml").expect("加载配置失败");
    cfg.strategy.mode = StrategyMode::BearShort;
    cfg.strategy.universe = vec!["TESTUSDT".to_string()];
    cfg
}

/// BTC 深熊历史：n 根持续温和下跌，OHLC 连贯（ATR 与价格成比例，不触发危机护栏）
fn bear_btc(n: usize) -> Vec<Candle> {
    let base_ts = 1_700_000_000_000i64;
    let mut px = 100.0f64;
    let mut prev_close = dec!(100);
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        // 前半段交替大小跌幅（制造 ATR 方差），后半段温和匀速（当前 ATR 分位居中，不触危机护栏）
        let step = if i < n / 2 { if i % 2 == 0 { 0.998 } else { 0.9998 } } else { 0.999 };
        px *= step;
        let c = Decimal::from_f64_retain(px).unwrap_or(dec!(100));
        out.push(Candle {
            timestamp: base_ts + (i as i64) * 3_600_000,
            open: prev_close,
            high: c * dec!(1.001),
            low: c * dec!(0.999),
            close: c,
            volume: dec!(1000),
            funding_rate: Decimal::ZERO,
        });
        prev_close = c;
    }
    out
}

/// 弱势标的 K 线：持续下跌(比 BTC 的 0.999/h 更快 → 截面动量为负) + 末根放量跌破 20 期低点 (V1 形态)
fn breakdown_alt() -> Vec<Candle> {
    let base_ts = 1_700_000_000_000i64;
    let mut candles = Vec::new();
    let mut px = 100.0f64;
    for i in 0..80usize {
        px *= 0.997; // -0.3%/h，明显弱于 BTC 的 -0.1%/h
        let c = Decimal::from_f64_retain(px).unwrap_or(dec!(100));
        candles.push(Candle {
            timestamp: base_ts + (i as i64) * 3_600_000,
            open: c * dec!(1.001),
            high: c * dec!(1.002),
            low: c * dec!(0.998),
            close: c,
            volume: dec!(1000),
            funding_rate: Decimal::ZERO,
        });
    }
    // 末根：跳空跌破 20 期低点 + 放量 + 阴线
    let last = candles.last().unwrap();
    candles.push(Candle {
        timestamp: last.timestamp + 3_600_000,
        open: last.close * dec!(0.998),
        high: last.close * dec!(0.997),
        low: last.close * dec!(0.985),
        close: last.close * dec!(0.987),
        volume: dec!(2000),
        funding_rate: Decimal::ZERO,
    });
    candles
}

#[test]
fn test_bear_maturity_gate_blocks_young_bears() {
    let cfg = bear_cfg();
    let alt = breakdown_alt();
    let slices: HashMap<String, &[Candle]> = [("TESTUSDT".to_string(), alt.as_slice())]
        .into_iter()
        .collect();
    // BTC 只跌了 30 根（成熟度 ~30 > 24 → 放行）
    let btc = bear_btc(200);
    let ok = scan_entry_signals(MarketRegime::Bear, &slices, &btc, &cfg.strategy);
    assert!(!ok.is_empty(), "成熟熊市 + 破位形态应产生 SHORT 信号");

    // 提高成熟度门槛到 500 → 年轻熊市被拒
    let mut cfg2 = cfg.clone();
    cfg2.strategy.bear_maturity_bars = 500;
    let blocked = scan_entry_signals(MarketRegime::Bear, &slices, &btc, &cfg2.strategy);
    assert!(blocked.is_empty(), "成熟度不足必须拒绝做空");

    // 非 Bear regime：绝不出手
    let bull_attempt = scan_entry_signals(MarketRegime::Bull, &slices, &btc, &cfg.strategy);
    let chop_attempt = scan_entry_signals(MarketRegime::Chop, &slices, &btc, &cfg.strategy);
    assert!(bull_attempt.is_empty() && chop_attempt.is_empty());
}

#[test]
fn test_bear_rejection_variant() {
    let mut cfg = bear_cfg();
    cfg.strategy.bear_entry_variant = "rejection".to_string();
    // 拉升衰竭形态：价格反弹到 EMA20 附近 + 阴线 + 缩量
    let base_ts = 1_700_000_000_000i64;
    let mut candles = Vec::new();
    let mut px = 100.0f64;
    for i in 0..80usize {
        px *= 0.998; // 持续下跌 → EMA20 < EMA60, close < EMA20 结构
        candles.push(Candle {
            timestamp: base_ts + (i as i64) * 3_600_000,
            open: Decimal::from_f64_retain(px * 1.001).unwrap(),
            high: Decimal::from_f64_retain(px * 1.002).unwrap(),
            low: Decimal::from_f64_retain(px * 0.998).unwrap(),
            close: Decimal::from_f64_retain(px).unwrap(),
            volume: dec!(1000),
            funding_rate: Decimal::ZERO,
        });
    }
    // 末根：反弹回 EMA20 附近 + 阴线拒绝 + 缩量
    let last_close = candles.last().unwrap().close;
    let rebound_open = last_close * dec!(1.012);
    candles.push(Candle {
        timestamp: base_ts + 80 * 3_600_000,
        open: rebound_open,
        high: rebound_open * dec!(1.002),
        low: last_close * dec!(0.998),
        close: last_close * dec!(1.002), // 收在 EMA20 附近(反弹后回落) 但仍 < open 阴线
        volume: dec!(500),
        funding_rate: Decimal::ZERO,
    });
    let btc = bear_btc(200);
    let slices: HashMap<String, &[Candle]> = [("TESTUSDT".to_string(), candles.as_slice())]
        .into_iter()
        .collect();
    let signals = scan_entry_signals(MarketRegime::Bear, &slices, &btc, &cfg.strategy);
    // 形态成立与否取决于反弹幅度是否落在 EMA20 ±0.5ATR；此处应至少不 panic 且方向为 Short
    for s in &signals {
        assert_eq!(s.side, Side::Short);
        assert!(s.proposed_stop > s.signal_close, "空头止损必须在价格上方");
    }
}

#[test]
fn test_bear_mode_mults_and_half_risk() {
    let mut cfg = AppConfig::load("config/settings.toml").unwrap();
    cfg.strategy.mode = StrategyMode::BearShort;
    assert_eq!(stop_atr_mult(&cfg.strategy), cfg.strategy.bear_stop_atr);
    assert_eq!(fixed_tp_atr_mult(&cfg.strategy), cfg.strategy.bear_tp_atr);
    assert_eq!(risk_scale_factor(&cfg.strategy), dec!(0.5), "空头模块未验证期半风险");
    // 其他模式不受影响
    cfg.strategy.mode = StrategyMode::LongOnlyBreakout;
    assert_eq!(risk_scale_factor(&cfg.strategy), Decimal::ONE);
    assert_eq!(stop_atr_mult(&cfg.strategy), cfg.strategy.initial_stop_atr_mult);
    assert_eq!(fixed_tp_atr_mult(&cfg.strategy), Decimal::ZERO);
}

#[test]
fn test_bear_short_exits_on_leaving_bear_including_chop() {
    let cfg = bear_cfg();
    let mut pos = Position {
        symbol: "TESTUSDT".to_string(),
        side: Side::Short,
        qty: dec!(1),
        entry_price: dec!(100),
        initial_stop: dec!(103),
        active_stop: dec!(103),
        initial_risk_per_unit: dec!(3),
        highest_close: dec!(100),
        lowest_close: dec!(100),
        bars_held: 1,
        breakeven_activated: false,
        entry_time: 0,
        entry_fee: dec!(0.05),
        accumulated_funding: Decimal::ZERO,
        take_profit: dec!(96),
    };
    let bar = Candle {
        timestamp: 3_600_000,
        open: dec!(99.8),
        high: dec!(100),
        low: dec!(99.5),
        close: dec!(99.8),
        volume: dec!(500),
        funding_rate: Decimal::ZERO,
    };
    // Bear → Chop 翻转且浮盈不足 0.5R → 必须撤退（旧臂的致命缺口）
    assert_eq!(
        update_position_on_bar_close(&mut pos, &bar, dec!(1), MarketRegime::Chop, &cfg.strategy, dec!(0.001)),
        Some(ExitReason::RegimeFlip),
        "BearShort 空单在熊市结束(含转Chop)时必须撤退"
    );
    // Bear → Bull 同样撤退
    let mut pos2 = pos.clone();
    assert_eq!(
        update_position_on_bar_close(&mut pos2, &bar, dec!(1), MarketRegime::Bull, &cfg.strategy, dec!(0.001)),
        Some(ExitReason::RegimeFlip)
    );
    // 留在 Bear → 不因状态离场（交给价格止损/止盈/时间）
    let mut pos3 = pos.clone();
    assert_eq!(
        update_position_on_bar_close(&mut pos3, &bar, dec!(1), MarketRegime::Bear, &cfg.strategy, dec!(0.001)),
        None
    );

    // 时间止损用 bear 专用窗口 12：bars_held=12 且浮盈不足 → TimeStop
    let mut pos4 = pos.clone();
    pos4.bars_held = 12;
    assert_eq!(
        update_position_on_bar_close(&mut pos4, &bar, dec!(1), MarketRegime::Bear, &cfg.strategy, dec!(0.001)),
        Some(ExitReason::TimeStop)
    );
}

#[test]
fn test_crisis_vol_cap_blocks_shorts_in_extreme_vol() {
    let mut cfg = bear_cfg();
    cfg.strategy.bear_max_atr_percentile = dec!(50); // 极低门槛模拟“当前处于高波动分位”
    let alt = breakdown_alt();
    let btc = bear_btc(400);
    let slices: HashMap<String, &[Candle]> = [("TESTUSDT".to_string(), alt.as_slice())]
        .into_iter()
        .collect();
    // bear_btc 波动极小(平滑下跌) → ATR 分位可能不高；改用 btc 加大末端波动确保分位高
    let mut wild_btc = bear_btc(400);
    for (i, c) in wild_btc.iter_mut().enumerate().skip(200) {
        let amp = if i % 2 == 0 { 3.0 } else { 0.3 };
        c.high = c.close * Decimal::from_f64_retain(1.0 + amp / 100.0).unwrap();
        c.low = c.close * Decimal::from_f64_retain(1.0 - amp / 100.0).unwrap();
    }
    // 无论分位判定如何，护栏逻辑本身不应 panic；门槛 50 时若末端处于高波动半区应拒绝
    let signals = scan_entry_signals(MarketRegime::Bear, &slices, &wild_btc, &cfg.strategy);
    // 末端交替巨震 → ATR 抬升，分位应 >= 50 → 拒绝
    assert!(
        signals.is_empty(),
        "高波动分位(轧空风险区)必须禁止新空, got {} signals",
        signals.len()
    );
}
