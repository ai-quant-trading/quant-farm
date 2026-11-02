use crate::config::{CostsConfig, RiskConfig};
use crate::types::{Position, SymbolSpec};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CircuitBreakerState {
    /// 正常运行 (单笔风险 1.5% ~ 2.0%)
    Normal,
    /// 一级熔断：单日亏损触及上限，冷却 24 小时暂停新开仓
    DailyCooldown { until_timestamp: i64 },
    /// 二级熔断：周回撤 >= 10% 或连亏 >= 5 次，单笔风险减半
    ReducedRisk,
    /// 三级熔断：总回撤 >= 20%，全系统硬停机并锁死
    Halted,
}

impl std::fmt::Display for CircuitBreakerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitBreakerState::Normal => write!(f, "NORMAL (正常全额风控)"),
            CircuitBreakerState::DailyCooldown { .. } => {
                write!(f, "TIER-1 COOLDOWN (日内亏损冷却 24h)")
            }
            CircuitBreakerState::ReducedRisk => write!(f, "TIER-2 REDUCED (防御降仓模式: 风险减半)"),
            CircuitBreakerState::Halted => write!(f, "TIER-3 HALTED (触及 20% 止损红线硬停机)"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PositionSizeResult {
    pub approved_qty: Decimal,
    pub notional_usd: Decimal,
    pub max_loss_usd: Decimal,
    pub stop_distance_pct: Decimal,
    pub risk_pct_used: Decimal,
}

#[derive(Debug, Clone)]
pub struct RiskManager {
    pub initial_equity: Decimal,
    pub peak_equity: Decimal,
    pub daily_start_equity: Decimal,
    pub current_day_utc: i64,
    pub consecutive_losses: usize,
    pub recent_7d_peak_equity: Decimal,
    pub equity_history_7d: VecDeque<(i64, Decimal)>,
    pub state: CircuitBreakerState,
    pub risk_cfg: RiskConfig,
    pub costs_cfg: CostsConfig,
}

impl RiskManager {
    pub fn new(initial_equity: Decimal, risk_cfg: RiskConfig, costs_cfg: CostsConfig) -> Self {
        Self {
            initial_equity,
            peak_equity: initial_equity,
            daily_start_equity: initial_equity,
            current_day_utc: 0,
            consecutive_losses: 0,
            recent_7d_peak_equity: initial_equity,
            equity_history_7d: VecDeque::new(),
            state: CircuitBreakerState::Normal,
            risk_cfg,
            costs_cfg,
        }
    }

    /// 每根 K 线更新账户净值并检查三级熔断状态
    pub fn update_equity(&mut self, timestamp_ms: i64, current_equity: Decimal) {
        if matches!(self.state, CircuitBreakerState::Halted) {
            return;
        }

        if current_equity > self.peak_equity {
            self.peak_equity = current_equity;
        }

        // 跨日重置日初净值 (UTC 00:00)
        let day_index = timestamp_ms / 86_400_000;
        if day_index != self.current_day_utc {
            self.current_day_utc = day_index;
            self.daily_start_equity = current_equity;
        }

        // 维护滚动 7 天净值峰值
        let seven_days_ms = 7 * 86_400_000;
        self.equity_history_7d.push_back((timestamp_ms, current_equity));
        while let Some(&(ts, _)) = self.equity_history_7d.front() {
            if timestamp_ms - ts > seven_days_ms {
                self.equity_history_7d.pop_front();
            } else {
                break;
            }
        }
        self.recent_7d_peak_equity = self
            .equity_history_7d
            .iter()
            .map(|(_, eq)| *eq)
            .max()
            .unwrap_or(current_equity);

        // 1. 检查三级硬熔断：相对于初始本金或最高净值回撤超过 20%
        let halt_floor = self.initial_equity * (Decimal::ONE - self.risk_cfg.max_drawdown_halt_pct);
        let peak_drawdown = if self.peak_equity > Decimal::ZERO {
            (self.peak_equity - current_equity) / self.peak_equity
        } else {
            Decimal::ZERO
        };

        if current_equity <= halt_floor || peak_drawdown >= self.risk_cfg.max_drawdown_halt_pct {
            self.state = CircuitBreakerState::Halted;
            return;
        }

        // 检查一级熔断冷却是否到期
        if let CircuitBreakerState::DailyCooldown { until_timestamp } = self.state {
            if timestamp_ms >= until_timestamp {
                self.state = CircuitBreakerState::Normal;
            } else {
                return;
            }
        }

        // 2. 检查一级熔断：日内亏损 >= 5%
        if self.daily_start_equity > Decimal::ZERO {
            let daily_loss_pct =
                (self.daily_start_equity - current_equity) / self.daily_start_equity;
            if daily_loss_pct >= self.risk_cfg.daily_loss_limit_pct {
                self.state = CircuitBreakerState::DailyCooldown {
                    until_timestamp: timestamp_ms + 86_400_000,
                };
                return;
            }
        }

        // 3. 检查二级熔断：滚动 7 日回撤 >= 10% 或连续止损 >= 5 次
        let rolling_7d_dd = if self.recent_7d_peak_equity > Decimal::ZERO {
            (self.recent_7d_peak_equity - current_equity) / self.recent_7d_peak_equity
        } else {
            Decimal::ZERO
        };

        if rolling_7d_dd >= self.risk_cfg.rolling_7d_drawdown_pct
            || self.consecutive_losses >= self.risk_cfg.max_consecutive_losses
            || self.consecutive_losses >= 2
        {
            self.state = CircuitBreakerState::ReducedRisk;
        } else if matches!(self.state, CircuitBreakerState::ReducedRisk)
            && rolling_7d_dd < (self.risk_cfg.rolling_7d_drawdown_pct / Decimal::from(2))
            && self.consecutive_losses == 0
        {
            self.state = CircuitBreakerState::Normal;
        }
    }

    pub fn record_trade_outcome(&mut self, net_pnl: Decimal) {
        if net_pnl < Decimal::ZERO {
            self.consecutive_losses += 1;
        } else {
            self.consecutive_losses = 0;
        }
    }

    /// 当发生连亏时，动态触发震荡期冷却保护 (每连续止损 3 次，自动冷却 36 小时避开假突破泥潭)
    pub fn apply_chop_cooldown_if_needed(&mut self, timestamp_ms: i64) {
        if matches!(self.state, CircuitBreakerState::Halted) {
            return;
        }
        if self.consecutive_losses > 0 && self.consecutive_losses % 3 == 0 {
            self.state = CircuitBreakerState::DailyCooldown {
                until_timestamp: timestamp_ms + 36 * 3_600_000,
            };
        }
    }

    /// 核心“以损定仓”公式实现 (Risk-Based Position Sizing)
    /// 根据账户净值、入场价、止损价和交易所精度规格，计算精确允许开仓数量
    pub fn calculate_position_size(
        &self,
        current_equity: Decimal,
        entry_price: Decimal,
        stop_price: Decimal,
        spec: &SymbolSpec,
        open_positions: &[Position],
    ) -> Option<PositionSizeResult> {
        // 熔断或冷却状态下严禁开新仓
        match self.state {
            CircuitBreakerState::Halted | CircuitBreakerState::DailyCooldown { .. } => return None,
            _ => {}
        }

        // 持仓数量上限检查
        if open_positions.len() >= self.risk_cfg.max_concurrent_positions {
            return None;
        }

        if entry_price <= Decimal::ZERO || stop_price <= Decimal::ZERO || entry_price == stop_price
        {
            return None;
        }

        // 决定当前使用的单笔风险比例 (正常 vs 二级熔断减半)
        let risk_pct = match self.state {
            CircuitBreakerState::ReducedRisk => self.risk_cfg.reduced_risk_per_trade_pct,
            _ => self.risk_cfg.risk_per_trade_pct,
        };

        let max_loss_usd = current_equity * risk_pct;

        // 止损百分比距离 + 双边交易磨损 (2 * (手续费 + 滑点))
        let raw_stop_dist_pct = (entry_price - stop_price).abs() / entry_price;
        let round_trip_wear =
            (self.costs_cfg.taker_fee_rate + self.costs_cfg.slippage_rate) * Decimal::from(2);
        let total_risk_dist_pct = raw_stop_dist_pct + round_trip_wear;

        if total_risk_dist_pct <= Decimal::ZERO {
            return None;
        }

        // 以损定仓理论名义价值: Notional = Risk_USD / d
        let mut target_notional = max_loss_usd / total_risk_dist_pct;

        // 约束 1：单币种最大名义仓位不超过总权益的 max_single_notional_pct (如 65%)
        let max_single_notional = current_equity * self.risk_cfg.max_single_notional_pct;
        if target_notional > max_single_notional {
            target_notional = max_single_notional;
        }

        // 约束 2：账户总名义持仓不超过 max_account_leverage (如 1.5x ~ 2.0x)
        let current_total_notional: Decimal = open_positions
            .iter()
            .map(|p| p.qty * p.entry_price)
            .sum();
        let max_total_notional = current_equity * self.risk_cfg.max_account_leverage;
        if current_total_notional >= max_total_notional {
            return None;
        }
        let remaining_notional_room = max_total_notional - current_total_notional;
        if target_notional > remaining_notional_room {
            target_notional = remaining_notional_room;
        }

        // 对齐交易所 step_size 数量精度
        let raw_qty = target_notional / entry_price;
        let approved_qty = spec.round_qty(raw_qty);
        let final_notional = approved_qty * entry_price;

        // 约束 3：必须满足交易所最小下单名义价值 (min_notional) 与系统最小门槛
        let min_required = spec.min_notional.max(self.risk_cfg.min_single_notional_usd);
        if final_notional < min_required || approved_qty <= Decimal::ZERO {
            return None;
        }

        let actual_max_loss = final_notional * total_risk_dist_pct;

        Some(PositionSizeResult {
            approved_qty,
            notional_usd: final_notional,
            max_loss_usd: actual_max_loss,
            stop_distance_pct: total_risk_dist_pct,
            risk_pct_used: risk_pct,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn mock_risk_manager(initial_equity: Decimal) -> RiskManager {
        let risk_cfg = RiskConfig {
            risk_per_trade_pct: dec!(0.02),
            reduced_risk_per_trade_pct: dec!(0.01),
            max_single_notional_pct: dec!(0.65),
            min_single_notional_usd: dec!(12.0),
            max_concurrent_positions: 2,
            max_account_leverage: dec!(1.5),
            daily_loss_limit_pct: dec!(0.05),
            rolling_7d_drawdown_pct: dec!(0.10),
            max_consecutive_losses: 5,
            max_drawdown_halt_pct: dec!(0.20),
        };
        let costs_cfg = CostsConfig {
            taker_fee_rate: dec!(0.0005),
            slippage_rate: dec!(0.0005),
            default_funding_rate_8h: dec!(0.0001),
        };
        RiskManager::new(initial_equity, risk_cfg, costs_cfg)
    }

    #[test]
    fn test_100u_position_sizing_exact_risk() {
        let rm = mock_risk_manager(dec!(100.0));
        let spec = SymbolSpec::default_for("SOLUSDT");
        // SOL 入场价 100.0，止损价 96.2 (3.8% 止损 + 0.2% 磨损 = 4.0% 总风险距离)
        // 100U 的 2% 风险 = 2.0 USDT -> 理论名义仓位 = 2.0 / 0.04 = 50.0 USDT (0.50 SOL)
        let res = rm
            .calculate_position_size(dec!(100.0), dec!(100.0), dec!(96.2), &spec, &[])
            .expect("应当计算出有效仓位");

        assert_eq!(res.approved_qty, dec!(0.50));
        assert_eq!(res.notional_usd, dec!(50.0));
        assert_eq!(res.max_loss_usd, dec!(2.0));
    }

    #[test]
    fn test_circuit_breaker_tiers() {
        let mut rm = mock_risk_manager(dec!(100.0));
        let t0 = 1_700_000_000_000i64;
        rm.update_equity(t0, dec!(100.0));
        assert_eq!(rm.state, CircuitBreakerState::Normal);

        // 日内跌 5.5% (剩 94.5U) -> 触发一级日内冷却
        rm.update_equity(t0 + 3600_000, dec!(94.5));
        assert!(matches!(
            rm.state,
            CircuitBreakerState::DailyCooldown { .. }
        ));

        // 24小时后解除冷却，但连亏 5 次触发二级减仓
        for _ in 0..5 {
            rm.record_trade_outcome(dec!(-1.0));
        }
        rm.update_equity(t0 + 90_000_000, dec!(94.5));
        assert_eq!(rm.state, CircuitBreakerState::ReducedRisk);

        // 跌破 80U (回撤 20%) -> 触发三级硬停机
        rm.update_equity(t0 + 95_000_000, dec!(79.8));
        assert_eq!(rm.state, CircuitBreakerState::Halted);
    }
}
