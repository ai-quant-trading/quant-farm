//! ChopMeanReversion 模式单元测试：入场条件 / 模式化倍数 / 破位离场

use coin_ai::config::{AppConfig, StrategyConfig, StrategyMode};
use coin_ai::strategy::{fixed_tp_atr_mult, live_tp_atr_mult, scan_entry_signals, stop_atr_mult, update_position_on_bar_close};
use coin_ai::types::{Candle, ExitReason, MarketRegime, Position, Side};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;

fn mr_cfg() -> AppConfig {
    let mut cfg = AppConfig::load("config/settings.toml").expect("加载配置失败");
    cfg.strategy.mode = StrategyMode::ChopMeanReversion;
    cfg.strategy.universe = vec!["TESTUSDT".to_string()];
    cfg
}

/// 构造一段震荡 K 线：价格在 [98, 102] 区间来回，末根贴下轨且远离 EMA20
fn chop_candles(final_stretch: bool, surge_volume: bool) -> Vec<Candle> {
    let mut candles = Vec::new();
    let base_ts = 1_700_000_000_000i64;
    // 80 根在 100 附近来回摆动的 K 线（构成有效区间 + 平滑 EMA20，且满足 MR 扫描的最少根数门槛 75+）
    for i in 0..80usize {
        let px = match i % 4 {
            0 => dec!(100.0),
            1 => dec!(101.8),
            2 => dec!(100.0),
            _ => dec!(98.4),
        };
        candles.push(Candle {
            timestamp: base_ts + (i as i64) * 3_600_000,
            open: px,
            high: px + dec!(0.2),
            low: px - dec!(0.2),
            close: px + dec!(0.1),
            volume: dec!(1000),
            funding_rate: Decimal::ZERO,
        });
    }
    // 末根：可选贴下轨深拉伸（≈-1.75 ATR）+ 可选放量
    let (o, c, v) = if final_stretch {
        (dec!(97.8), dec!(96.5), if surge_volume { dec!(2000) } else { dec!(800) })
    } else {
        (dec!(100.2), dec!(100.1), dec!(900))
    };
    candles.push(Candle {
        timestamp: base_ts + 80 * 3_600_000,
        open: o,
        high: o + dec!(0.2),
        low: c - dec!(0.2),
        close: c,
        volume: v,
        funding_rate: Decimal::ZERO,
    });
    candles
}

#[test]
fn test_mr_entry_at_lower_rail_with_quiet_volume() {
    let cfg = mr_cfg();
    let candles = chop_candles(true, false);
    let slices: HashMap<String, &[Candle]> = [("TESTUSDT".to_string(), candles.as_slice())]
        .into_iter()
        .collect();
    let signals = scan_entry_signals(MarketRegime::Chop, &slices, &candles, &cfg.strategy);
    assert!(!signals.is_empty(), "贴下轨+拉伸+缩量应产生 LONG 信号");
    assert_eq!(signals[0].side, Side::Long);
    // 紧凑止损: close - 1.2*ATR
    assert!(signals[0].proposed_stop < signals[0].signal_close);
}

#[test]
fn test_mr_entry_rejected_on_surge_volume() {
    let cfg = mr_cfg();
    let candles = chop_candles(true, true); // 放量 → 不接飞刀
    let slices: HashMap<String, &[Candle]> = [("TESTUSDT".to_string(), candles.as_slice())]
        .into_iter()
        .collect();
    let signals = scan_entry_signals(MarketRegime::Chop, &slices, &candles, &cfg.strategy);
    assert!(signals.is_empty(), "放量时必须拒绝 MR 入场（区间将被突破）");
}

#[test]
fn test_mr_no_signal_in_mid_range_or_trend_regime() {
    let cfg = mr_cfg();
    // 区间中部：无拉伸 → 无信号
    let mid = chop_candles(false, false);
    let slices: HashMap<String, &[Candle]> = [("TESTUSDT".to_string(), mid.as_slice())]
        .into_iter()
        .collect();
    assert!(scan_entry_signals(MarketRegime::Chop, &slices, &mid, &cfg.strategy).is_empty());

    // Bull/Bear（趋势领地）：MR 绝不出手
    let stretched = chop_candles(true, false);
    let slices2: HashMap<String, &[Candle]> = [("TESTUSDT".to_string(), stretched.as_slice())]
        .into_iter()
        .collect();
    assert!(scan_entry_signals(MarketRegime::Bull, &slices2, &stretched, &cfg.strategy).is_empty());
    assert!(scan_entry_signals(MarketRegime::Bear, &slices2, &stretched, &cfg.strategy).is_empty());
}

#[test]
fn test_mode_mult_helpers() {
    let mut cfg = AppConfig::load("config/settings.toml").unwrap();
    // 趋势模式：倍数不变
    cfg.strategy.mode = StrategyMode::LongOnlyBreakout;
    assert_eq!(stop_atr_mult(&cfg.strategy), cfg.strategy.initial_stop_atr_mult);
    assert_eq!(fixed_tp_atr_mult(&cfg.strategy), Decimal::ZERO, "趋势模式回测无固定 TP（历史口径）");
    assert_eq!(live_tp_atr_mult(&cfg.strategy), cfg.strategy.chandelier_atr_mult);
    // MR 模式：紧凑止损 + 均值目标
    cfg.strategy.mode = StrategyMode::ChopMeanReversion;
    assert_eq!(stop_atr_mult(&cfg.strategy), cfg.strategy.chop_mr_stop_atr);
    assert_eq!(fixed_tp_atr_mult(&cfg.strategy), cfg.strategy.chop_mr_tp_atr);
    assert_eq!(live_tp_atr_mult(&cfg.strategy), cfg.strategy.chop_mr_tp_atr);
}

#[test]
fn test_mr_regime_flip_exit_and_short_time_stop() {
    let cfg = mr_cfg();
    // MR 多头仓位，大盘转为 Bull（离开 Chop）→ 无条件离场
    let mut pos = Position {
        symbol: "TESTUSDT".to_string(),
        side: Side::Long,
        qty: dec!(1),
        entry_price: dec!(100),
        initial_stop: dec!(98.8),
        active_stop: dec!(98.8),
        initial_risk_per_unit: dec!(1.2),
        highest_close: dec!(100),
        lowest_close: dec!(100),
        bars_held: 1,
        breakeven_activated: false,
        entry_time: 0,
        entry_fee: dec!(0.05),
        accumulated_funding: Decimal::ZERO,
        take_profit: dec!(101),
    };
    let bar = Candle {
        timestamp: 3_600_000,
        open: dec!(100.5),
        high: dec!(100.8),
        low: dec!(100.2),
        close: dec!(100.5),
        volume: dec!(500),
        funding_rate: Decimal::ZERO,
    };
    assert_eq!(
        update_position_on_bar_close(&mut pos, &bar, dec!(1), MarketRegime::Bull, &cfg.strategy, dec!(0.001)),
        Some(ExitReason::RegimeFlip),
        "MR 仓位在大盘离开 Chop 时必须立即离场"
    );

    // 时间止损用 MR 专用短窗口 (8 根)：bars_held=8 且浮盈不足 → TimeStop
    let mut pos2 = pos.clone();
    pos2.bars_held = 8;
    let weak_bar = Candle {
        timestamp: 7_200_000,
        open: dec!(100.0),
        high: dec!(100.1),
        low: dec!(99.9),
        close: dec!(100.0),
        volume: dec!(500),
        funding_rate: Decimal::ZERO,
    };
    assert_eq!(
        update_position_on_bar_close(&mut pos2, &weak_bar, dec!(1), MarketRegime::Chop, &cfg.strategy, dec!(0.001)),
        Some(ExitReason::TimeStop)
    );
}
