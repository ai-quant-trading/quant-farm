use anyhow::{bail, Context, Result};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub account: AccountConfig,
    pub risk: RiskConfig,
    pub costs: CostsConfig,
    pub strategy: StrategyConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountConfig {
    pub initial_equity: Decimal,
    pub reserve_ratio: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    pub risk_per_trade_pct: Decimal,
    pub reduced_risk_per_trade_pct: Decimal,
    pub max_single_notional_pct: Decimal,
    pub min_single_notional_usd: Decimal,
    pub max_concurrent_positions: usize,
    pub max_account_leverage: Decimal,
    pub daily_loss_limit_pct: Decimal,
    pub rolling_7d_drawdown_pct: Decimal,
    pub max_consecutive_losses: usize,
    pub max_drawdown_halt_pct: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostsConfig {
    pub taker_fee_rate: Decimal,
    pub slippage_rate: Decimal,
    pub default_funding_rate_8h: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyMode {
    /// 全天候截面动量 + 唐奇安通道突破 (多空双向)
    AllWeatherBreakout,
    /// 纯多头强势突破 + 空头空仓避险 (Long-Only Bull Sniper)
    LongOnlyBreakout,
    /// 波动率收敛扩张突破 (Volatility Squeeze Breakout)
    VolatilitySqueeze,
    /// 强势趋势回踩均线二次启动 (EMA Pullback Continuation)
    TrendPullback,
    /// 多策略融合共振 (Ensemble: 突破 + 波动率收敛 + 动量回踩)
    EnsembleComposite,
    /// 震荡期区间边缘均值回归 (Chop Mean Reversion: 下轨超卖买/上轨超买卖, 回到均值即止盈)
    ChopMeanReversion,
}

impl Default for StrategyMode {
    fn default() -> Self {
        StrategyMode::AllWeatherBreakout
    }
}

impl std::fmt::Display for StrategyMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StrategyMode::AllWeatherBreakout => write!(f, "AllWeatherBreakout (多空全天候突破)"),
            StrategyMode::LongOnlyBreakout => write!(f, "LongOnlyBreakout (纯多头避险突破)"),
            StrategyMode::VolatilitySqueeze => write!(f, "VolatilitySqueeze (波动率收敛爆发)"),
            StrategyMode::TrendPullback => write!(f, "TrendPullback (强势趋势回踩启动)"),
            StrategyMode::EnsembleComposite => write!(f, "EnsembleComposite (多因子融合策略)"),
            StrategyMode::ChopMeanReversion => write!(f, "ChopMeanReversion (震荡区间均值回归)"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfig {
    #[serde(default)]
    pub mode: StrategyMode,
    pub benchmark_symbol: String,
    pub universe: Vec<String>,
    /// 数据库驱动优化：因盘口点差过大 (>3.5bps) 或假突破率过高被禁止下单的黑名单币种（仍参与全市场截面排名参考，但禁止实盘开仓）
    #[serde(default)]
    pub execution_blacklist: Vec<String>,
    pub top_k_momentum: usize,
    pub donchian_period: usize,
    pub atr_period: usize,
    pub volume_surge_mult: Decimal,
    pub max_bias_pct: Decimal,
    pub initial_stop_atr_mult: Decimal,
    pub chandelier_atr_mult: Decimal,
    pub breakeven_r_mult: Decimal,
    pub time_stop_bars: usize,
    pub time_stop_min_r: Decimal,
    pub regime_adx_threshold: Decimal,
    /// 波动率过滤器（默认 0.0 = 关闭）：仅当开仓时 ATR/价格 分位 >= 该阈值才允许开仓。
    /// 该阈值由 `regime-report` 在全样本 + 前后半段交叉验证后确定，禁止拍脑袋填写。
    #[serde(default)]
    pub min_atr_percentile: Decimal,
    /// 计算 ATR 分位时回看的 K 线根数（默认 200）
    #[serde(default = "default_atr_lookback")]
    pub atr_percentile_lookback: usize,
    /// 下单前波动闸门（默认 0 = 关闭）：最近 10 秒价格振幅超过该 bps 值时放弃开仓。
    /// 只使用决策时刻已 observable 的过去数据（无未来函数）：闪崩/插针进行中不追单。
    /// 配合实盘 IOC 限价护圈使用：闸门拦「正在剧烈波动」，护圈拦「落地瞬间跑飞」。
    #[serde(default)]
    pub max_recent_move_bps: Decimal,
    /// 波动闸门的回看窗口（秒，默认 10）
    #[serde(default = "default_recent_move_window")]
    pub recent_move_window_secs: u64,
    // ---- ChopMeanReversion 专用参数（其余模式完全忽略）----
    /// 有效区间宽度下限（Donchian 宽度/价格），过窄 = 死水无肉
    #[serde(default = "default_mr_range_min")]
    pub chop_mr_range_min_pct: Decimal,
    /// 有效区间宽度上限，过宽 = 已是趋势不是震荡
    #[serde(default = "default_mr_range_max")]
    pub chop_mr_range_max_pct: Decimal,
    /// 均值拉伸入场阈值（|close − EMA20| / ATR），默认 1.5
    #[serde(default = "default_mr_stretch")]
    pub chop_mr_stretch_atr: Decimal,
    /// 紧凑初始止损（×ATR），默认 1.2（趋势模式仍用 initial_stop_atr_mult）
    #[serde(default = "default_mr_stop")]
    pub chop_mr_stop_atr: Decimal,
    /// 固定止盈目标（×ATR，回到均值附近），默认 1.0
    #[serde(default = "default_mr_tp")]
    pub chop_mr_tp_atr: Decimal,
    /// MR 专用时间止损（K 线根数），均值回归不成立就走，默认 8
    #[serde(default = "default_mr_time_stop")]
    pub chop_mr_time_stop_bars: usize,
}

fn default_recent_move_window() -> u64 {
    10
}

fn default_mr_range_min() -> Decimal {
    dec!(0.03)
}
fn default_mr_range_max() -> Decimal {
    dec!(0.12)
}
fn default_mr_stretch() -> Decimal {
    dec!(1.5)
}
fn default_mr_stop() -> Decimal {
    dec!(1.2)
}
fn default_mr_tp() -> Decimal {
    dec!(1.0)
}
fn default_mr_time_stop() -> usize {
    8
}

fn default_atr_lookback() -> usize {
    200
}

impl AppConfig {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path.as_ref())
            .with_context(|| format!("无法读取配置文件: {}", path.as_ref().display()))?;
        let cfg: AppConfig = toml::from_str(&content).context("解析 TOML 配置文件失败")?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// 架构级安全自检：任何危险参数在启动阶段直接拒绝编译/运行
    pub fn validate(&self) -> Result<()> {
        if self.account.initial_equity < dec!(50.0) {
            bail!("初始本金不能低于 50 USDT (否则无法满足合约最小下单金额门槛)");
        }
        if self.risk.risk_per_trade_pct <= Decimal::ZERO
            || self.risk.risk_per_trade_pct > dec!(0.03)
        {
            bail!(
                "单笔风险比例 ({}) 超过安全红线 (最高允许 3.0%)",
                self.risk.risk_per_trade_pct
            );
        }
        if self.risk.max_account_leverage > dec!(3.0) {
            bail!(
                "账户最大有效杠杆 ({}) 超过安全红线 (最高允许 3.0x)",
                self.risk.max_account_leverage
            );
        }
        if self.risk.max_drawdown_halt_pct > dec!(0.25) {
            bail!("三级硬熔断回撤线不能超过 25%");
        }
        Ok(())
    }
}
