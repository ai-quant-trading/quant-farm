use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Long,
    Short,
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Side::Long => write!(f, "LONG"),
            Side::Short => write!(f, "SHORT"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarketRegime {
    Bull,
    Bear,
    Chop,
}

impl std::fmt::Display for MarketRegime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MarketRegime::Bull => write!(f, "BULL (多头趋势)"),
            MarketRegime::Bear => write!(f, "BEAR (空头趋势)"),
            MarketRegime::Chop => write!(f, "CHOP (震荡观望)"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candle {
    pub timestamp: i64, // milliseconds UTC
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    #[serde(default)]
    pub funding_rate: Decimal,
}

impl Candle {
    pub fn datetime(&self) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(self.timestamp).unwrap_or_default()
    }
}

/// 交易所交易对精度与最小下单约束，防止实盘报 LOT_SIZE / PRICE_FILTER / MIN_NOTIONAL 拒单
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolSpec {
    pub symbol: String,
    pub tick_size: Decimal,
    pub step_size: Decimal,
    pub min_notional: Decimal,
}

impl SymbolSpec {
    pub fn default_for(symbol: &str) -> Self {
        match symbol {
            "BTCUSDT" => Self {
                symbol: symbol.to_string(),
                tick_size: dec!(0.1),
                step_size: dec!(0.001),
                min_notional: dec!(100.0),
            },
            "ETHUSDT" => Self {
                symbol: symbol.to_string(),
                tick_size: dec!(0.01),
                step_size: dec!(0.001),
                min_notional: dec!(20.0),
            },
            "BNBUSDT" | "SOLUSDT" | "AVAXUSDT" => Self {
                symbol: symbol.to_string(),
                tick_size: dec!(0.01),
                step_size: dec!(0.01),
                min_notional: dec!(5.0),
            },
            "LINKUSDT" | "SUIUSDT" => Self {
                symbol: symbol.to_string(),
                tick_size: dec!(0.001),
                step_size: dec!(0.1),
                min_notional: dec!(5.0),
            },
            _ => Self {
                symbol: symbol.to_string(),
                tick_size: dec!(0.0001),
                step_size: dec!(1.0),
                min_notional: dec!(5.0),
            },
        }
    }

    /// 向下取整对齐下单数量步长 (step_size)
    pub fn round_qty(&self, raw_qty: Decimal) -> Decimal {
        if self.step_size <= Decimal::ZERO || raw_qty <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        let steps = (raw_qty / self.step_size).floor();
        steps * self.step_size
    }

    /// 对齐价格最小变动单位 (tick_size)
    pub fn round_price(&self, raw_price: Decimal) -> Decimal {
        if self.tick_size <= Decimal::ZERO || raw_price <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        let ticks = (raw_price / self.tick_size).round();
        ticks * self.tick_size
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExitReason {
    InitialStop,
    BreakevenStop,
    TrailingStop,
    TimeStop,
    RegimeFlip,
    CircuitBreaker,
    EndOfBacktest,
    /// 固定目标价止盈（均值回归模式：回到均值即离场）
    TakeProfit,
}

impl std::fmt::Display for ExitReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExitReason::InitialStop => write!(f, "初始硬止损 (Initial Stop)"),
            ExitReason::BreakevenStop => write!(f, "保本止损 (Breakeven)"),
            ExitReason::TrailingStop => write!(f, "吊灯移动止盈 (Chandelier Exit)"),
            ExitReason::TimeStop => write!(f, "时间止损 (Time Stop)"),
            ExitReason::RegimeFlip => write!(f, "大盘转势平仓 (Regime Flip)"),
            ExitReason::CircuitBreaker => write!(f, "风控熔断强平 (Circuit Breaker)"),
            ExitReason::EndOfBacktest => write!(f, "回测结束结算 (End of Test)"),
            ExitReason::TakeProfit => write!(f, "固定目标价止盈 (Take Profit)"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub symbol: String,
    pub side: Side,
    pub qty: Decimal,
    pub entry_price: Decimal,
    pub initial_stop: Decimal,
    pub active_stop: Decimal,
    pub initial_risk_per_unit: Decimal,
    pub highest_close: Decimal,
    pub lowest_close: Decimal,
    pub bars_held: usize,
    pub breakeven_activated: bool,
    pub entry_time: i64,
    pub entry_fee: Decimal,
    pub accumulated_funding: Decimal,
    /// 固定目标价（ZERO = 禁用，趋势模式维持纯移动止损口径不变）
    #[serde(default)]
    pub take_profit: Decimal,
}

impl Position {
    pub fn notional(&self, current_price: Decimal) -> Decimal {
        self.qty * current_price
    }

    pub fn unrealized_pnl(&self, current_price: Decimal) -> Decimal {
        match self.side {
            Side::Long => (current_price - self.entry_price) * self.qty,
            Side::Short => (self.entry_price - current_price) * self.qty,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub symbol: String,
    pub side: Side,
    pub entry_time: i64,
    pub exit_time: i64,
    pub qty: Decimal,
    pub entry_price: Decimal,
    pub exit_price: Decimal,
    pub gross_pnl: Decimal,
    pub total_fees: Decimal,
    pub funding_pnl: Decimal,
    pub net_pnl: Decimal,
    pub r_multiple: Decimal,
    pub bars_held: usize,
    pub exit_reason: ExitReason,
    pub equity_after: Decimal,
}
