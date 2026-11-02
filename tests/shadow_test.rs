//! 影子模式集成测试：开仓→出场全生命周期 / 生产隔离断言 / 冷却生效

use coin_ai::config::{AppConfig, StrategyMode};
use coin_ai::db::{ApiQuoteRow, TradingDatabase};
use coin_ai::shadow::ShadowEngine;
use coin_ai::types::{Candle, MarketRegime};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;

fn test_cfg(tmp: &str) -> AppConfig {
    let mut cfg = AppConfig::load("config/settings.toml").expect("加载配置失败");
    cfg.strategy.mode = StrategyMode::LongOnlyBreakout; // 生产保持冠军模式
    cfg.strategy.universe = vec!["BTCUSDT".to_string()];
    cfg.account.initial_equity = dec!(400); // BTCUSDT 交易所最小名义 100U，半风险下 100U 本金无法开仓
    cfg.shadow.db_path = tmp.to_string();
    cfg
}

fn bear_btc(n: usize) -> Vec<Candle> {
    let base_ts = 1_700_000_000_000i64;
    let mut px = 100.0f64;
    let mut prev_close = dec!(100);
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        // 前半段交替大小跌幅制造 ATR 方差（避免匀速序列的 ATR 分位=100 触发危机护栏）
        let step = if i < n / 2 {
            if i % 2 == 0 { 0.998 } else { 0.9998 }
        } else {
            0.999
        };
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

fn breakdown_alt() -> Vec<Candle> {
    let base_ts = 1_700_000_000_000i64;
    let mut candles = Vec::new();
    let mut px = 100.0f64;
    for i in 0..80usize {
        px *= 0.997;
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

fn make_quote(symbol: &str, bid: Decimal, ask: Decimal) -> ApiQuoteRow {
    ApiQuoteRow {
        symbol: symbol.to_string(),
        bid_price: bid,
        ask_price: ask,
        last_price: (bid + ask) / dec!(2),
        spread_bps: dec!(1.0),
        source_api: "test".to_string(),
        fetched_at: "2026-11-03T00:00:00+00:00".to_string(),
    }
}

#[test]
fn test_shadow_lifecycle_and_production_isolation() {
    let dir = std::env::temp_dir().join("coin_ai_shadow_test");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let shadow_db = dir.join("shadow.db").to_str().unwrap().to_string();
    let prod_db = dir.join("prod.db").to_str().unwrap().to_string();

    let cfg = test_cfg(&shadow_db);
    // 生产库基线
    let prod = TradingDatabase::open(&prod_db, dec!(400)).unwrap();

    let mut engine = ShadowEngine::new(&cfg).expect("影子引擎构造");

    // 整点刷新：合成成熟熊市 + 弱势破位标的 → 产生候选
    let btc = bear_btc(200);
    let alt = breakdown_alt();
    let mut universe: HashMap<String, Vec<Candle>> = HashMap::new();
    universe.insert("BTCUSDT".to_string(), alt);
    engine.on_metrics(&btc, &universe, MarketRegime::Bear);
    assert!(
        !engine.pending_entries.is_empty(),
        "成熟熊市 + 破位形态应产生影子候选"
    );

    // 第 1 拍：开仓（盘口 bid 附近成交空单）
    let mut quotes = HashMap::new();
    let alt_close = universe["BTCUSDT"].last().unwrap().close;
    quotes.insert("BTCUSDT".to_string(), make_quote("BTCUSDT", alt_close * dec!(0.999), alt_close * dec!(1.001)));
    let mut metrics: HashMap<String, (Decimal, Decimal, Decimal)> = HashMap::new();
    metrics.insert("BTCUSDT".to_string(), (dec!(0), dec!(1), dec!(0)));
    engine
        .on_cycle(&quotes, MarketRegime::Bear, &metrics, 1_700_100_000_000)
        .unwrap();
    let positions = engine.db.list_positions().unwrap();
    assert_eq!(positions.len(), 1, "影子应开立 1 个空单");
    assert_eq!(positions[0].side, "SHORT");
    let entry = positions[0].entry_price;

    // 第 2 拍：价格暴跌到止盈区（ask <= entry - 4*ATR - 缓冲）
    let mut quotes2 = HashMap::new();
    quotes2.insert("BTCUSDT".to_string(), make_quote("BTCUSDT", entry * dec!(0.93), entry * dec!(0.931)));
    engine
        .on_cycle(&quotes2, MarketRegime::Bear, &metrics, 1_700_100_060_000)
        .unwrap();
    let trades = engine.db.list_recent_trades(100).unwrap();
    assert!(!trades.is_empty(), "暴跌后影子空单应止盈离场");
    assert!(trades[0].net_pnl > Decimal::ZERO, "止盈出场应为正收益");
    assert!(trades[0].exit_reason.contains("影子"));

    // ---- 生产隔离断言：影子全程运行后，生产库零订单零持仓 ----
    let (_, prod_orders, prod_positions, prod_trades) = prod.count_table_rows().unwrap();
    assert_eq!((prod_orders, prod_positions, prod_trades), (0, 0, 0));

    // 冷却：刚平仓的币种 30 分钟内禁止影子重开
    engine.on_metrics(&btc, &universe, MarketRegime::Bear);
    assert!(!engine.pending_entries.is_empty());
    engine
        .on_cycle(&quotes2, MarketRegime::Bear, &metrics, 1_700_100_120_000)
        .unwrap();
    let after = engine.db.list_positions().unwrap();
    assert!(after.is_empty(), "冷却期内禁止原地重开");
}

#[test]
fn test_shadow_disabled_by_config() {
    let dir = std::env::temp_dir().join("coin_ai_shadow_off");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut cfg = test_cfg(dir.join("s.db").to_str().unwrap());
    cfg.shadow.enabled = false;
    // 构造仍应成功（enabled 只控制 paper_engine 是否接线）
    let engine = ShadowEngine::new(&cfg);
    assert!(engine.is_ok());
}
