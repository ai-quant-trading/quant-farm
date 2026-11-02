//! 回放压测 Harness 集成测试：
//! 1. 延迟漂移方向性（多空开平仓的"不利"定义恒正确）
//! 2. 压力窗口扫描（四类窗口、范围裁剪、去重叠）
//! 3. 合成情景生成器确定性（同 seed 严格同序列）
//! 4. 决策函数提取后的关键行为（止损/止盈/时间止损/黑天鹅紧急清仓）
//! 5. 端到端合成回放冒烟（不联网）

use coin_ai::config::AppConfig;
use coin_ai::paper_engine::{LiveExitDecision, SqlitePaperEngine};
use coin_ai::sim::{
    drift_bps, generate_cascade_scenario, replay_window_with_latency, scan_stress_windows,
};
use coin_ai::tick_db::{TickDatabase, TickRow};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

fn make_quote(bid: Decimal, ask: Decimal) -> coin_ai::db::ApiQuoteRow {
    let mid = (bid + ask) / Decimal::from(2);
    let spread_bps = if mid > Decimal::ZERO {
        (ask - bid) / mid * dec!(10000)
    } else {
        Decimal::ZERO
    };
    coin_ai::db::ApiQuoteRow {
        symbol: "BTCUSDT".to_string(),
        bid_price: bid,
        ask_price: ask,
        last_price: mid,
        spread_bps,
        source_api: "test".to_string(),
        fetched_at: "2026-11-02T00:00:00+00:00".to_string(),
    }
}

fn make_position(side: &str, entry: Decimal, qty: Decimal, stop: Decimal, tp: Decimal, opened_ms: i64) -> coin_ai::db::DbPositionRow {
    coin_ai::db::DbPositionRow {
        symbol: "BTCUSDT".to_string(),
        side: side.to_string(),
        qty,
        entry_price: entry,
        entry_order_id: "T_OPEN".to_string(),
        stop_order_id: "T_STOP".to_string(),
        initial_stop: stop,
        active_stop: stop,
        take_profit: tp,
        highest_mark: entry,
        lowest_mark: entry,
        last_mark_price: entry,
        unrealized_pnl: Decimal::ZERO,
        entry_fee: dec!(0.05),
        bars_held: 0,
        opened_at: chrono::DateTime::from_timestamp_millis(opened_ms)
            .unwrap()
            .to_rfc3339(),
        updated_at: String::new(),
    }
}

fn load_cfg() -> AppConfig {
    AppConfig::load("config/settings.toml").expect("加载配置失败")
}

#[test]
fn test_drift_bps_directionality() {
    // 四个方向的"不利"定义：买贵/卖便宜 > 0，反向 < 0
    assert_eq!(drift_bps("OPEN", "LONG", dec!(100), dec!(101)), dec!(100)); // 买贵 1% → +100bps
    assert_eq!(drift_bps("CLOSE", "LONG", dec!(100), dec!(99)), dec!(100)); // 卖便宜 1% → +100bps
    assert_eq!(drift_bps("OPEN", "SHORT", dec!(100), dec!(99)), dec!(100)); // 空开卖便宜 → +100bps
    assert_eq!(drift_bps("CLOSE", "SHORT", dec!(100), dec!(101)), dec!(100)); // 空平买贵 → +100bps
    // 有利方向为负
    assert_eq!(drift_bps("OPEN", "LONG", dec!(100), dec!(99)), dec!(-100));
    assert_eq!(drift_bps("OPEN", "LONG", dec!(100), dec!(100)), dec!(0));
}

#[test]
fn test_extracted_exit_decision_stop_and_tp() {
    let cfg = load_cfg();
    let now = 1_800_000_000_000i64;
    // LONG: 开仓 100, 硬止损 95 → 盘口 bid 跌破 95 触发硬止损
    let p = make_position("LONG", dec!(100), dec!(1), dec!(95), dec!(110), now);
    let q = make_quote(dec!(94), dec!(94.02));
    match SqlitePaperEngine::check_position_exit(
        p,
        &q,
        coin_ai::types::MarketRegime::Bull,
        Some(dec!(2)),
        false,
        now + 3_600_000,
        &cfg.strategy,
    ) {
        LiveExitDecision::Exit { reason, .. } => assert!(reason.contains("STOP_MARKET"), "{}", reason),
        LiveExitDecision::Hold(_) => panic!("止损应触发"),
    }

    // LONG: 盘口 bid 涨过止盈 110 → TAKE_PROFIT
    let p = make_position("LONG", dec!(100), dec!(1), dec!(95), dec!(110), now);
    let q = make_quote(dec!(111), dec!(111.02));
    match SqlitePaperEngine::check_position_exit(
        p,
        &q,
        coin_ai::types::MarketRegime::Bull,
        Some(dec!(2)),
        false,
        now + 3_600_000,
        &cfg.strategy,
    ) {
        LiveExitDecision::Exit { reason, .. } => assert!(reason.contains("TAKE_PROFIT"), "{}", reason),
        LiveExitDecision::Hold(_) => panic!("止盈应触发"),
    }

    // LONG: 价格安全区 → 持仓保留
    let p = make_position("LONG", dec!(100), dec!(1), dec!(95), dec!(110), now);
    let q = make_quote(dec!(102), dec!(102.02));
    match SqlitePaperEngine::check_position_exit(
        p,
        &q,
        coin_ai::types::MarketRegime::Bull,
        Some(dec!(2)),
        false,
        now + 3_600_000,
        &cfg.strategy,
    ) {
        LiveExitDecision::Hold(p2) => assert!(p2.unrealized_pnl > Decimal::ZERO),
        LiveExitDecision::Exit { .. } => panic!("安全区不应平仓"),
    }
}

#[test]
fn test_extracted_exit_time_stop_and_evacuation() {
    let cfg = load_cfg();
    let now = 1_800_000_000_000i64;
    // 持仓超过 time_stop_bars(16h) 且浮盈不足 min_r(0.4R) → TIME_STOP
    let opened = now - 20 * 3_600_000; // 20 小时前
    let p = make_position("LONG", dec!(100), dec!(1), dec!(95), dec!(110), opened);
    let q = make_quote(dec!(100.2), dec!(100.22)); // 浮盈 0.2U << 0.4R(2U)
    match SqlitePaperEngine::check_position_exit(
        p,
        &q,
        coin_ai::types::MarketRegime::Bull,
        Some(dec!(2)),
        false,
        now,
        &cfg.strategy,
    ) {
        LiveExitDecision::Exit { reason, .. } => assert!(reason.contains("TIME_STOP"), "{}", reason),
        LiveExitDecision::Hold(_) => panic!("时间止损应触发"),
    }

    // 黑天鹅激活 + LONG → 无论盈亏立即紧急清仓
    let p = make_position("LONG", dec!(100), dec!(1), dec!(95), dec!(110), now);
    let q = make_quote(dec!(103), dec!(103.02));
    match SqlitePaperEngine::check_position_exit(
        p,
        &q,
        coin_ai::types::MarketRegime::Bull,
        Some(dec!(2)),
        true,
        now + 600_000,
        &cfg.strategy,
    ) {
        LiveExitDecision::Exit { reason, .. } => {
            assert!(reason.contains("BLACK_SWAN"), "{}", reason)
        }
        LiveExitDecision::Hold(_) => panic!("黑天鹅应紧急清仓"),
    }
}

#[test]
fn test_scan_stress_windows_kinds_and_bounds() {
    // 构造 20 天 1h K线：第 200 根暴跌 8%，第 300 根暴涨 7%，其余平静
    let hour = 3_600_000i64;
    let t0 = 1_700_000_000_000i64;
    let bars: Vec<coin_ai::types::Candle> = (0..480)
        .map(|i| {
            let open = dec!(100);
            let (high, low) = if i == 200 {
                (dec!(100.5), dec!(92))
            } else if i == 300 {
                (dec!(107), dec!(99.5))
            } else {
                (dec!(100.6), dec!(99.4))
            };
            coin_ai::types::Candle {
                timestamp: t0 + i * hour,
                open,
                high,
                low,
                close: dec!(100),
                volume: dec!(100),
                funding_rate: Decimal::ZERO,
            }
        })
        .collect();
    let tick_min = t0 + 100 * hour;
    let tick_max = t0 + 470 * hour;
    let windows = scan_stress_windows(&bars, tick_min, tick_max, 4, 24);
    assert!(!windows.is_empty());
    for w in &windows {
        // 窗口必须在 tick 覆盖范围内
        assert!(w.start_ms >= tick_min);
        assert!(w.end_ms <= tick_max);
        // 窗口互不重叠
        for other in &windows {
            if w.label != other.label {
                assert!(w.start_ms >= other.end_ms || w.end_ms <= other.start_ms);
            }
        }
    }
    // 暴跌/暴涨事件应被选中（在 tick 范围内）
    assert!(windows
        .iter()
        .any(|w| w.kind == coin_ai::sim::WindowKind::Crash && w.event_ts == t0 + 200 * hour));
    assert!(windows
        .iter()
        .any(|w| w.kind == coin_ai::sim::WindowKind::Pump && w.event_ts == t0 + 300 * hour));
}

#[test]
fn test_synthetic_generator_deterministic() {
    let a = generate_cascade_scenario(42);
    let b = generate_cascade_scenario(42);
    assert_eq!(a.ticks.len(), b.ticks.len());
    assert_eq!(a.ticks.first(), b.ticks.first());
    assert_eq!(a.ticks.last(), b.ticks.last());
    assert_eq!(a.candles_1h.len(), b.candles_1h.len());
    // 不同 seed 必须不同
    let c = generate_cascade_scenario(43);
    assert_ne!(a.ticks.last(), c.ticks.last());
}

#[test]
fn test_volatility_gate_blocks_entry_during_violence() {
    // 波动闸门：最近价格振幅超阈值时，check_new_entries 必须拒绝开仓（只用过去数据）
    let cfg = load_cfg();
    let risk_mgr = coin_ai::risk::RiskManager::new(
        dec!(300),
        cfg.risk.clone(),
        cfg.costs.clone(),
    );
    let entries = vec![("BTCUSDT".to_string(), coin_ai::types::Side::Long, dec!(2))];
    let mut quotes = std::collections::HashMap::new();
    quotes.insert(
        "BTCUSDT".to_string(),
        make_quote(dec!(100.00), dec!(100.02)), // 点差 2bps < 3.5bps 门槛
    );
    let mut recent_move = std::collections::HashMap::new();
    recent_move.insert("BTCUSDT".to_string(), dec!(50)); // 最近 10s 振幅 50bps > 闸门 30bps

    let blocked = SqlitePaperEngine::check_new_entries(
        &entries,
        &quotes,
        &[],
        dec!(300),
        &risk_mgr,
        &cfg.strategy,
        |_| false,
        &recent_move,
    );
    assert!(blocked.is_empty(), "波动中必须拒绝开仓");

    // 波动平静 → 放行
    recent_move.insert("BTCUSDT".to_string(), dec!(5));
    let allowed = SqlitePaperEngine::check_new_entries(
        &entries,
        &quotes,
        &[],
        dec!(300),
        &risk_mgr,
        &cfg.strategy,
        |_| false,
        &recent_move,
    );
    assert!(!allowed.is_empty(), "平静时应允许开仓");
}

#[test]
fn test_collar_abandons_runaway_fill() {
    // 护圈 A/B 核心（用户「下一秒翻倍」场景）：+30% 轧空开始时刻 LONG 入场，
    // 无护圈按火箭价吃单；有护圈（只拦不利方向）必须放弃
    let cfg = load_cfg();
    let mut sim_cfg = cfg.clone();
    sim_cfg.strategy.universe = vec!["BTCUSDT".to_string()];
    sim_cfg.account.initial_equity = dec!(300);

    let scenario = coin_ai::sim::generate_pump_scenario(11);
    let dir = std::env::temp_dir().join("coin_ai_replay_test_collar");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut db = TickDatabase::open(dir.join("collar.db")).unwrap();
    db.upsert_ticks("BTCUSDT", &scenario.ticks).unwrap();

    let engine = SqlitePaperEngine::new(sim_cfg.clone(), "data/replay_sim.db").unwrap();
    let window = scenario.window.clone(); // kind=Pump → LONG 种子, seed_at_event=true
    // 轧空开始时刻入场 + 5 秒落地延迟（每秒约 +0.118% → 不利偏离约 60bps > 30bps 护圈）
    let no_collar =
        replay_window_with_latency(&engine, &sim_cfg, &db, &scenario.candles_1h, &window, 5000, 10, dec!(0.5), true, true, Decimal::ZERO)
            .unwrap();
    let with_collar =
        replay_window_with_latency(&engine, &sim_cfg, &db, &scenario.candles_1h, &window, 5000, 10, dec!(0.5), true, true, dec!(30))
            .unwrap();

    // 无护圈：按火箭价开仓（灾难成交存在）
    assert!(no_collar.abandoned_count == 0);
    assert!(no_collar.open_count > 0, "无护圈时会照常吃单");
    // 有护圈：不利偏离 > 30bps，必须放弃
    assert!(with_collar.abandoned_count > 0, "护圈应放弃跑飞的开仓");
    assert!(
        with_collar
            .fills
            .iter()
            .any(|f| f.action == "OPEN_ABANDONED"),
        "必须有 OPEN_ABANDONED 记录"
    );
    // 护圈保护钱包：不在火箭顶追入，净值不应更差
    assert!(with_collar.final_equity >= no_collar.final_equity);
}

#[test]
fn test_collar_never_blocks_favorable_fill() {
    // 有利偏离（下跌中买入更便宜）必须照常成交——护圈只拦不利方向，不错杀好价格
    let cfg = load_cfg();
    let mut sim_cfg = cfg.clone();
    sim_cfg.strategy.universe = vec!["BTCUSDT".to_string()];
    sim_cfg.account.initial_equity = dec!(300);

    let scenario = generate_cascade_scenario(11); // -30% 级联
    let dir = std::env::temp_dir().join("coin_ai_replay_test_fav");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut db = TickDatabase::open(dir.join("fav.db")).unwrap();
    db.upsert_ticks("BTCUSDT", &scenario.ticks).unwrap();

    let engine = SqlitePaperEngine::new(sim_cfg.clone(), "data/replay_sim.db").unwrap();
    let mut long_window = scenario.window.clone();
    long_window.kind = coin_ai::sim::WindowKind::Pump; // LONG 种子
    let r =
        replay_window_with_latency(&engine, &sim_cfg, &db, &scenario.candles_1h, &long_window, 5000, 10, dec!(0.5), true, true, dec!(30))
            .unwrap();

    // 级联下跌中 LONG 落地价 < 决策价 = 有利 → 护圈放弃数必须为 0
    assert_eq!(r.abandoned_count, 0, "有利成交不允许被护圈错杀");
    assert!(r.open_count > 0);
}

#[test]
fn test_synthetic_e2e_replay_smoke() {
    // 端到端冒烟：合成级联情景回放（不联网），多头种子必须因黑天鹅紧急清仓离场
    let cfg = load_cfg();
    let mut sim_cfg = cfg.clone();
    sim_cfg.strategy.universe = vec!["BTCUSDT".to_string()];
    sim_cfg.account.initial_equity = dec!(300);

    let scenario = generate_cascade_scenario(7);
    let dir = std::env::temp_dir().join("coin_ai_replay_test_smoke");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut db = TickDatabase::open(dir.join("smoke.db")).unwrap();
    db.upsert_ticks("BTCUSDT", &scenario.ticks).unwrap();

    let engine = SqlitePaperEngine::new(sim_cfg.clone(), "data/replay_sim.db").unwrap();
    let mut long_window = scenario.window.clone();
    long_window.kind = coin_ai::sim::WindowKind::Pump; // LONG 种子
    let result =
        replay_window_with_latency(&engine, &sim_cfg, &db, &scenario.candles_1h, &long_window, 0, 10, dec!(0.5), true, false, dec!(30))
            .unwrap();

    // 黑天鹅探测器必须在级联段触发
    assert!(result.detector_first_fire_secs.is_some(), "合成级联必须触发黑天鹅探测器");
    // LONG 种子必须因级联离场：硬止损(~-0.8%)比探测器(-2%阈值)更早触发，
    // 分层防御顺序 = 硬止损先保护、黑天鹅哨兵随后冻结新多单（合理生产行为）
    let exit_reasons: Vec<&str> = result
        .fills
        .iter()
        .filter(|f| f.action == "CLOSE")
        .map(|f| f.reason.as_str())
        .collect();
    assert!(
        exit_reasons.iter().any(|r| r.contains("STOP_MARKET") || r.contains("BLACK_SWAN")),
        "LONG 种子应被止损或紧急清仓保护离场, 实际: {:?}",
        exit_reasons
    );
    // 成交记录完整
    assert!(!result.fills.is_empty());
}
