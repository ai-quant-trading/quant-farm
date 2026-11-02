use coin_ai::ai::AiRiskGuard;
use coin_ai::config::AppConfig;
use coin_ai::exchange::{BinanceFuturesGateway, PersistentLedger};
use coin_ai::risk::{CircuitBreakerState, RiskManager};
use coin_ai::strategy::{ update_position_on_bar_close, SignalCandidate};
use coin_ai::types::{Candle, ExitReason, MarketRegime, Position, Side, SymbolSpec};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// 端到端集成测试 1：配置安全自检拦截（防止误配超高风险或超高杠杆）
#[test]
fn test_e2e_config_safety_guards() {
    let mut cfg = AppConfig::load("config/settings.toml").expect("加载默认配置应成功");
    assert!(cfg.validate().is_ok());

    // 故意将单笔风险调到 10% (超过 3% 红线)，必须被拦截
    cfg.risk.risk_per_trade_pct = dec!(0.10);
    assert!(cfg.validate().is_err(), "超过 3% 的单笔风险配置必须被拒绝启动");

    // 恢复单笔风险，故意将账户总杠杆调到 5.0x (超过 3.0x 红线)，必须被拦截
    cfg.risk.risk_per_trade_pct = dec!(0.02);
    cfg.risk.max_account_leverage = dec!(5.0);
    assert!(cfg.validate().is_err(), "超过 3x 的总有效杠杆配置必须被拒绝启动");
}

/// 端到端集成测试 2：100U 多仓位组合与总杠杆上限、最小下单额、精度对齐测试
#[test]
fn test_e2e_multi_position_and_leverage_caps() {
    let cfg = AppConfig::load("config/settings.toml").unwrap();
    let rm = RiskManager::new(dec!(100.0), cfg.risk.clone(), cfg.costs.clone());

    // 第 1 仓：SOLUSDT @ 100.0, 止损 @ 98.0 (止损距离 2% + 0.2% 磨损 = 2.2%)
    // 理论仓位 = 2.0U / 0.022 = 90.9U，但受限于单仓 65% 上限 (65.0U)，应被截断为 0.65 SOL (65.0U)
    let sol_spec = SymbolSpec::default_for("SOLUSDT");
    let size1 = rm
        .calculate_position_size(dec!(100.0), dec!(100.0), dec!(98.0), &sol_spec, &[])
        .expect("第 1 仓应成功计算");
    assert_eq!(size1.approved_qty, dec!(0.65));
    assert_eq!(size1.notional_usd, dec!(65.0));

    let pos1 = Position {
        symbol: "SOLUSDT".to_string(),
        side: Side::Long,
        qty: size1.approved_qty,
        entry_price: dec!(100.0),
        initial_stop: dec!(98.0),
        active_stop: dec!(98.0),
        initial_risk_per_unit: dec!(2.0),
        highest_close: dec!(100.0),
        lowest_close: dec!(100.0),
        bars_held: 0,
        breakeven_activated: false,
        entry_time: 1000,
        entry_fee: dec!(0.0325),
        accumulated_funding: Decimal::ZERO,
    };

    // 第 2 仓：XRPUSDT @ 1.00, 止损 @ 0.98
    // 同样受单仓 65% 上限限制为 65U (总持仓 65 + 65 = 130U <= 150U 1.5x 上限)
    let xrp_spec = SymbolSpec::default_for("XRPUSDT");
    let size2 = rm
        .calculate_position_size(
            dec!(100.0),
            dec!(1.00),
            dec!(0.98),
            &xrp_spec,
            std::slice::from_ref(&pos1),
        )
        .expect("第 2 仓应成功计算");
    assert_eq!(size2.notional_usd, dec!(65.0));

    let pos2 = Position {
        symbol: "XRPUSDT".to_string(),
        side: Side::Long,
        qty: size2.approved_qty,
        entry_price: dec!(1.00),
        initial_stop: dec!(0.98),
        active_stop: dec!(0.98),
        initial_risk_per_unit: dec!(0.02),
        highest_close: dec!(1.00),
        lowest_close: dec!(1.00),
        bars_held: 0,
        breakeven_activated: false,
        entry_time: 1000,
        entry_fee: dec!(0.0325),
        accumulated_funding: Decimal::ZERO,
    };

    // 第 3 仓：已持有 2 个仓位，达到 100U 模式 max_concurrent_positions=2 上限，必须返回 None 拒绝开仓！
    let ada_spec = SymbolSpec::default_for("ADAUSDT");
    let size3 = rm.calculate_position_size(
        dec!(100.0),
        dec!(0.50),
        dec!(0.48),
        &ada_spec,
        &[pos1, pos2],
    );
    assert!(
        size3.is_none(),
        "超过最大同时持仓数 (2 个) 时必须被风控网关拦截"
    );

    // 测试 BTCUSDT 100U 最小下单名义价值拦截 (BTC 最小门槛 100U > 65U 单仓上限)
    let btc_spec = SymbolSpec::default_for("BTCUSDT");
    let btc_size = rm.calculate_position_size(
        dec!(100.0),
        dec!(80000.0),
        dec!(78000.0),
        &btc_spec,
        &[],
    );
    assert!(
        btc_size.is_none(),
        "100U 模式下 BTCUSDT 因最小下单门槛 (100U) 超过单仓上限 (65U) 必须被自动过滤"
    );
}

/// 端到端集成测试 3：AI 风控一票否决（拦截天地针异常波动与极端多头拥挤费率）
#[tokio::test]
async fn test_e2e_ai_risk_veto_scenarios() {
    let ai_guard = AiRiskGuard::from_env();
    let sig = SignalCandidate {
        symbol: "DOGEUSDT".to_string(),
        side: Side::Long,
        score: dec!(0.04),
        signal_close: dec!(0.10),
        proposed_stop: dec!(0.095),
        atr_val: dec!(0.002),
    };

    // 场景 A：当根 K 线振幅高达 18% (>12% 阈值，疑似天地针插针行情)
    let flash_crash_candles = vec![Candle {
        timestamp: 1000,
        open: dec!(0.10),
        high: dec!(0.118),
        low: dec!(0.10),
        close: dec!(0.105),
        volume: dec!(100000),
        funding_rate: dec!(0.0001),
    }];
    let v1 = ai_guard
        .evaluate_signal(&sig, &flash_crash_candles, None)
        .await;
    assert!(!v1.approved, "天地针异常波动必须被 AI 风控一票否决");
    assert!(v1.risk_score >= 80);

    // 场景 B：多头资金费率高达 +0.25% (> +0.15% 阈值，散户极度拥挤追多)
    let crowded_funding_candles = vec![Candle {
        timestamp: 1000,
        open: dec!(0.10),
        high: dec!(0.103),
        low: dec!(0.099),
        close: dec!(0.102),
        volume: dec!(100000),
        funding_rate: dec!(0.0025),
    }];
    let v2 = ai_guard
        .evaluate_signal(&sig, &crowded_funding_candles, None)
        .await;
    assert!(!v2.approved, "极端正资金费率下追多必须被 AI 风控一票否决");
}

/// 端到端集成测试 4：持仓全状态机流转（1.5R 保本移位 -> 吊灯止盈棘轮上移 -> 时间止损 -> 大盘反转平仓）
#[test]
fn test_e2e_position_lifecycle_transitions() {
    let cfg = AppConfig::load("config/settings.toml").unwrap();
    let round_trip_wear = dec!(0.002);

    let mut pos = Position {
        symbol: "SOLUSDT".to_string(),
        side: Side::Long,
        qty: dec!(0.50),
        entry_price: dec!(100.0),
        initial_stop: dec!(96.0),
        active_stop: dec!(96.0),
        initial_risk_per_unit: dec!(4.0), // 1R = 4.0 USDT price move
        highest_close: dec!(100.0),
        lowest_close: dec!(100.0),
        bars_held: 0,
        breakeven_activated: false,
        entry_time: 1000,
        entry_fee: dec!(0.025),
        accumulated_funding: Decimal::ZERO,
    };

    // 1. 价格涨至 106.5 (+1.625R >= 1.5R) -> 应激活保本并启动吊灯移动止盈
    let bar1 = Candle {
        timestamp: 2000,
        open: dec!(100.0),
        high: dec!(107.0),
        low: dec!(100.0),
        close: dec!(106.5),
        volume: dec!(1000),
        funding_rate: Decimal::ZERO,
    };
    let reason1 = update_position_on_bar_close(
        &mut pos,
        &bar1,
        dec!(1.8), // chandelier = 106.5 - 3.0 * 1.8 = 101.10
        MarketRegime::Bull,
        &cfg.strategy,
        round_trip_wear,
    );
    assert!(reason1.is_none());
    assert!(pos.breakeven_activated);
    assert_eq!(pos.active_stop, dec!(101.10));

    // 2. 下一根 K 线小幅回调至 104.0 -> 止损线绝不能下移 (棘轮单向保护)
    let bar2 = Candle {
        timestamp: 3000,
        open: dec!(106.5),
        high: dec!(106.5),
        low: dec!(103.5),
        close: dec!(104.0),
        volume: dec!(1000),
        funding_rate: Decimal::ZERO,
    };
    let _ = update_position_on_bar_close(
        &mut pos,
        &bar2,
        dec!(1.8),
        MarketRegime::Bull,
        &cfg.strategy,
        round_trip_wear,
    );
    assert_eq!(
        pos.active_stop,
        dec!(101.10),
        "价格回调时移动止损绝不能下移"
    );

    // 3. 测试时间止损：新建一个横盘 16 小时涨幅不足 0.4R 的僵尸仓位 -> 必须触发 TimeStop
    let mut zombie_pos = pos.clone();
    zombie_pos.bars_held = 15;
    zombie_pos.breakeven_activated = false;
    let stagnant_bar = Candle {
        timestamp: 4000,
        open: dec!(100.0),
        high: dec!(101.0),
        low: dec!(99.5),
        close: dec!(100.5), // +0.125R < 0.4R
        volume: dec!(500),
        funding_rate: Decimal::ZERO,
    };
    let reason_time = update_position_on_bar_close(
        &mut zombie_pos,
        &stagnant_bar,
        dec!(1.8),
        MarketRegime::Bull,
        &cfg.strategy,
        round_trip_wear,
    );
    assert_eq!(reason_time, Some(ExitReason::TimeStop));

    // 4. 测试大盘转势平仓：持有多单且未脱离成本区，大盘突然转为 Bear -> 必须触发 RegimeFlip
    let mut flip_pos = pos.clone();
    flip_pos.bars_held = 2;
    let reason_flip = update_position_on_bar_close(
        &mut flip_pos,
        &stagnant_bar,
        dec!(1.8),
        MarketRegime::Bear,
        &cfg.strategy,
        round_trip_wear,
    );
    assert_eq!(reason_flip, Some(ExitReason::RegimeFlip));
}

/// 端到端集成测试 5：实盘网关未配 Key 拒绝启动保护 & 账本崩溃恢复一致性
#[tokio::test]
async fn test_e2e_live_gateway_safety_and_ledger_persistence() {
    // 未设置 BINANCE_API_KEY 时尝试启动 Live 模式必须报错拦截
    std::env::remove_var("BINANCE_API_KEY");
    let live_gw = BinanceFuturesGateway::new(false, "data/test_e2e_ledger.json");
    assert!(
        live_gw.is_err(),
        "未配置有效 API Key 时严禁启动 Live 真金实盘网关"
    );

    // 测试持久化账本写入后重新加载 100% 恢复
    let test_path = "data/test_e2e_persistence.json";
    let _ = std::fs::remove_file(test_path);
    {
        let mut ledger =
            PersistentLedger::load_or_init(test_path, "DRY-RUN", dec!(100.0)).unwrap();
        ledger.cash_balance = dec!(98.45);
        ledger.consecutive_losses = 2;
        ledger
            .stop_order_ids
            .insert("AVAXUSDT".to_string(), "STOP_999".to_string());
        ledger.save(test_path).unwrap();
    }
    let reloaded = PersistentLedger::load_or_init(test_path, "DRY-RUN", dec!(100.0)).unwrap();
    assert_eq!(reloaded.cash_balance, dec!(98.45));
    assert_eq!(reloaded.consecutive_losses, 2);
    assert_eq!(
        reloaded.stop_order_ids.get("AVAXUSDT").map(|s| s.as_str()),
        Some("STOP_999")
    );
    let _ = std::fs::remove_file(test_path);
}

/// 端到端集成测试 6：二级熔断风险减半与三级熔断锁死测试
#[test]
fn test_e2e_circuit_breaker_halving_and_halt() {
    let cfg = AppConfig::load("config/settings.toml").unwrap();
    let mut rm = RiskManager::new(dec!(100.0), cfg.risk.clone(), cfg.costs.clone());
    let spec = SymbolSpec::default_for("SOLUSDT");

    // 正常状态下 (100U, 2.0% 风险 = 2.0U)，入场 100 止损 95 (5% + 0.2% = 5.2% 止损距离)
    // 名义仓位 = 2.0 / 0.052 = 38.46U -> 0.38 SOL
    let normal_size = rm
        .calculate_position_size(dec!(100.0), dec!(100.0), dec!(95.0), &spec, &[])
        .unwrap();
    assert_eq!(normal_size.approved_qty, dec!(0.38));

    // 连续止损 5 次 -> 触发二级熔断 ReducedRisk (单笔风险从 2.0% 减半至 1.0%)
    for _ in 0..5 {
        rm.record_trade_outcome(dec!(-1.5));
    }
    rm.update_equity(1_700_000_000_000, dec!(92.5));
    assert_eq!(rm.state, CircuitBreakerState::ReducedRisk);

    // 二级熔断下同样入场 100 止损 95，仓位必须自动减半 (92.5U * 1.0% / 0.052 = 17.78U -> 0.17 SOL)
    let reduced_size = rm
        .calculate_position_size(dec!(92.5), dec!(100.0), dec!(95.0), &spec, &[])
        .unwrap();
    assert_eq!(reduced_size.approved_qty, dec!(0.17));

    // 净值跌破 80.0U (回撤 >= 20%) -> 触发三级熔断 Halted，完全禁止任何开仓！
    rm.update_equity(1_700_000_100_000, dec!(79.9));
    assert_eq!(rm.state, CircuitBreakerState::Halted);
    let halted_size =
        rm.calculate_position_size(dec!(79.9), dec!(100.0), dec!(95.0), &spec, &[]);
    assert!(halted_size.is_none(), "三级硬熔断后必须拒绝一切新开仓请求");
}
