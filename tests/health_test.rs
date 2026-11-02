//! 健康监测单元测试：分级判定 / 窗口切分 / 状态聚合

use coin_ai::health::{Level, MonitorResult};

#[test]
fn test_level_worst_aggregation() {
    use Level::*;
    assert_eq!(Level::worst(Green, Red), Red);
    assert_eq!(Level::worst(Red, Green), Red);
    assert_eq!(Level::worst(Green, Yellow), Yellow);
    assert_eq!(Level::worst(Green, Green), Green);
    // 灰色(样本不足)不稀释其他状态
    assert_eq!(Level::worst(Insufficient, Green), Green);
    assert_eq!(Level::worst(Insufficient, Insufficient), Insufficient);
}

#[test]
fn test_monitor_labels() {
    assert_eq!(Level::Red.label(), "RED");
    assert_eq!(Level::Insufficient.label(), "GRAY(样本不足)");
}

// window_stats 是私有函数，通过集成口径验证：直接构造报告结构确认渲染不 panic
#[test]
fn test_render_report_smoke() {
    let report = coin_ai::health::HealthReport {
        monitors: vec![
            MonitorResult {
                name: "M1 边际衰减",
                level: Level::Insufficient,
                detail: "闭环样本 0 笔 < 最少 20 笔".to_string(),
            },
            MonitorResult {
                name: "M3 回撤语境",
                level: Level::Green,
                detail: "当前回撤 0.00%".to_string(),
            },
        ],
        overall: Level::Insufficient,
        baseline: None,
        window_trades: Default::default(),
        window_days: Default::default(),
    };
    let dir = std::env::temp_dir().join("coin_ai_health_test");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("health.md");
    coin_ai::health::render_report(&report, path.to_str().unwrap()).unwrap();
    let content = std::fs::read_to_string(&path).unwrap();
    assert!(content.contains("M1 边际衰减"));
    assert!(content.contains("GRAY(样本不足)"));
}
