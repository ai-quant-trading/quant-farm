//! 策略农场测试：参数白名单 / spec 解析 / Book 生命周期 / 验证门 / 注册表恢复

use coin_ai::config::AppConfig;
use coin_ai::farm::{spec_to_config, Book, Farm, StrategySpec};
use coin_ai::types::{Candle, MarketRegime};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::json;
use std::collections::HashMap;


fn jmap(v: serde_json::Value) -> HashMap<String, serde_json::Value> {
    v.as_object().unwrap().iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

fn base_cfg() -> AppConfig {
    AppConfig::load("config/settings.toml").expect("加载配置失败")
}

#[test]
fn test_spec_allowlist_accepts_valid_params() {
    let cfg = base_cfg();
    let spec = StrategySpec {
        name: "my_bear_v2".to_string(),
        author: "tester".to_string(),
        mode: "bear_short".to_string(),
        params: jmap(json!({
            "bear_stop_atr": "2.5",
            "bear_tp_atr": 4.0,
            "bear_maturity_bars": 24,
            "donchian_period": 20
        })),
        universe: Some(vec!["SOLUSDT".to_string(), "ETHUSDT".to_string()]),
        initial_equity: dec!(100),
        combo: None,
    };
    let out = spec_to_config(&cfg, &spec).expect("合法 spec 应通过");
    assert_eq!(out.strategy.bear_stop_atr, dec!(2.5));
    assert_eq!(out.strategy.bear_tp_atr, dec!(4.0)); // 数字型 JSON 也接受
    assert_eq!(out.strategy.universe.len(), 2);
}

#[test]
fn test_spec_rejects_unknown_param_and_bad_universe() {
    let cfg = base_cfg();
    let mut spec = StrategySpec {
        name: "bad1".to_string(),
        author: "t".to_string(),
        mode: "bear_short".to_string(),
        params: jmap(json!({ "risk_per_trade_pct": "0.5" })),
        universe: None,
        initial_equity: dec!(100),
        combo: None,
    };
    // 风控参数不在白名单 → 拒（所有账本共享生产风控纪律）
    assert!(spec_to_config(&cfg, &spec).is_err());

    spec.params = jmap(json!({ "unknown_knob": 1 }));
    let err = spec_to_config(&cfg, &spec).unwrap_err().to_string();
    assert!(err.contains("白名单"), "错误信息应含白名单提示: {}", err);

    spec.params = HashMap::new();
    spec.universe = Some(vec!["NOTACOIN".to_string()]);
    assert!(spec_to_config(&cfg, &spec).is_err(), "非生产池币种应拒");

    spec.universe = None;
    spec.mode = "magic_crystal_ball".to_string();
    assert!(spec_to_config(&cfg, &spec).is_err(), "未知模式应拒");
}

#[test]
fn test_book_lifecycle_offline() {
    let dir = std::env::temp_dir().join("coin_ai_farm_test_book");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let cfg = base_cfg();
    let book = Book::new(
        "t1",
        cfg,
        dir.join("t1.db").to_str().unwrap(),
    )
    .expect("Book 构造");
    assert_eq!(book.status().name, "t1");
    assert_eq!(book.status().trades, 0);
}

#[test]
fn test_farm_registry_persistence() {
    let dir = std::env::temp_dir().join("coin_ai_farm_test_reg");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let cfg = base_cfg();

    // Farm::load 空目录 → 空农场；注册一条 rejected 记录（不开账本、不跑回测——手动构造）
    let reg = r#"[{
        "spec": {"name":"junk","author":"t","mode":"bear_short","params":{},"initial_equity":"100"},
        "status": "rejected",
        "validation": null,
        "submitted_at": "2026-11-04T00:00:00+00:00"
    }]"#;
    std::fs::write(dir.join("registry.json"), reg).unwrap();
    let data_mgr = coin_ai::data::DataManager::new("data").unwrap();
    let farm = Farm::load(&cfg, &data_mgr, dir.to_str().unwrap()).expect("加载");
    assert_eq!(farm.registry.len(), 1);
    assert!(farm.books.is_empty(), "rejected 不开账本");
    let board = farm.leaderboard();
    assert_eq!(board.len(), 1);
    assert_eq!(board[0]["status"], "rejected");
}

#[test]
fn test_book_metrics_respects_custom_universe() {
    // 构造只含 BTCUSDT 的 universe 的 Book：on_metrics 用非池内标的喂入 → 无候选
    let dir = std::env::temp_dir().join("coin_ai_farm_test_uni");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut cfg = base_cfg();
    cfg.strategy.universe = vec!["ETHUSDT".to_string()];
    let mut book = Book::new("t2", cfg, dir.join("t2.db").to_str().unwrap()).unwrap();
    // SOLUSDT 不在池内 → 即便给了 K 线也不产生候选
    let candles: HashMap<String, Vec<Candle>> =
        [("SOLUSDT".to_string(), vec![Candle::default_candle(1000); 130])]
            .into_iter()
            .collect();
    book.on_metrics(&vec![Candle::default_candle(1000); 300], &candles, MarketRegime::Bull);
    assert!(book.pending_entries.is_empty());
}

/// 测试辅助：默认 K 线
trait DefaultCandle {
    fn default_candle(ts: i64) -> Candle;
}
impl DefaultCandle for Candle {
    fn default_candle(ts: i64) -> Candle {
        Candle {
            timestamp: ts,
            open: dec!(100),
            high: dec!(101),
            low: dec!(99),
            close: dec!(100),
            volume: dec!(1000),
            funding_rate: Decimal::ZERO,
        }
    }
}
