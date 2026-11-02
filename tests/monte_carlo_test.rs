//! Monte Carlo 模块单元测试：bootstrap 确定性 / 成本压力数学

use coin_ai::monte_carlo::{bootstrap_equity, cost_stress_totals};
use coin_ai::types::{ExitReason, Side, TradeRecord};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

fn make_trade(net: Decimal, fee: Decimal) -> TradeRecord {
    TradeRecord {
        symbol: "TESTUSDT".to_string(),
        side: Side::Long,
        entry_time: 0,
        exit_time: 1,
        qty: dec!(1),
        entry_price: dec!(100),
        exit_price: dec!(101),
        gross_pnl: net + fee,
        total_fees: fee,
        funding_pnl: Decimal::ZERO,
        net_pnl: net,
        r_multiple: net,
        bars_held: 1,
        exit_reason: ExitReason::TakeProfit,
        equity_after: Decimal::ZERO,
    }
}

#[test]
fn test_bootstrap_deterministic_and_sane() {
    let trades: Vec<TradeRecord> = (0..50)
        .map(|i| make_trade(dec!(1) + Decimal::from(i % 3), dec!(0.1)))
        .collect();
    let a = bootstrap_equity(&trades, dec!(100), 500, 42);
    let b = bootstrap_equity(&trades, dec!(100), 500, 42);
    assert_eq!(a.len(), 500);
    // 同 seed 完全一致
    for (x, y) in a.iter().zip(b.iter()) {
        assert_eq!(x.final_equity, y.final_equity);
    }
    // 全赢交易 → 所有重抽样期末都 > 本金
    assert!(a.iter().all(|o| o.final_equity > 100.0));
    // 回撤非负
    assert!(a.iter().all(|o| o.max_dd_pct >= 0.0));
}

#[test]
fn test_bootstrap_losing_trades() {
    let trades: Vec<TradeRecord> = (0..30).map(|_| make_trade(dec!(-1), dec!(0.1))).collect();
    let out = bootstrap_equity(&trades, dec!(100), 200, 7);
    assert!(out.iter().all(|o| o.final_equity < 100.0));
}

#[test]
fn test_cost_stress_math() {
    // 10 笔各净赚 1U、费用 0.2U → 基础净利 10U，总费用 2U
    let trades: Vec<TradeRecord> = (0..10).map(|_| make_trade(dec!(1), dec!(0.2))).collect();
    let results = cost_stress_totals(&trades, dec!(100), &[dec!(1.0), dec!(1.5), dec!(0.5)]);
    // ×1.0 → 110
    assert_eq!(results[0].1, dec!(110));
    // ×1.5 → 110 - 2*0.5 = 109
    assert_eq!(results[1].1, dec!(109));
    // ×0.5 → 110 + 2*0.5 = 111
    assert_eq!(results[2].1, dec!(111));
}
