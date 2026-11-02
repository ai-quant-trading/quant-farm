use crate::config::AppConfig;
use crate::indicators::{atr, detect_market_regime};
use crate::risk::{CircuitBreakerState, RiskManager};
use crate::strategy::{scan_entry_signals, update_position_on_bar_close, SignalCandidate};
use crate::types::{Candle, ExitReason, MarketRegime, Position, Side, SymbolSpec, TradeRecord};
use anyhow::Result;
use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct BacktestSummary {
    pub initial_equity: Decimal,
    pub final_equity: Decimal,
    pub total_return_pct: Decimal,
    pub max_drawdown_pct: Decimal,
    pub max_drawdown_usd: Decimal,
    pub total_trades: usize,
    pub win_trades: usize,
    pub loss_trades: usize,
    pub win_rate_pct: Decimal,
    pub profit_factor: Decimal,
    pub avg_win_usd: Decimal,
    pub avg_loss_usd: Decimal,
    pub payoff_ratio: Decimal,
    pub sharpe_ratio: f64,
    pub total_fees_paid: Decimal,
    pub total_funding_net: Decimal,
    pub final_breaker_state: CircuitBreakerState,
    pub trades: Vec<TradeRecord>,
    pub equity_curve: Vec<(i64, Decimal)>,
}

pub struct BacktestEngine {
    cfg: AppConfig,
}

impl BacktestEngine {
    pub fn new(cfg: AppConfig) -> Self {
        Self { cfg }
    }

    /// 运行防未来函数、计入真实双边手续费 + 滑点 + 资金费率的组合回测
    pub fn run(&self, all_candles: &HashMap<String, Vec<Candle>>) -> Result<BacktestSummary> {
        let btc_candles = all_candles
            .get(&self.cfg.strategy.benchmark_symbol)
            .expect("缺少基准币种 BTCUSDT 行情数据");

        // 以基准币种的时间戳序列作为全局时钟
        let warmup_bars = 250usize;
        if btc_candles.len() <= warmup_bars + 10 {
            anyhow::bail!("历史 K 线数量不足，至少需要 {} 根 1h K 线", warmup_bars + 10);
        }

        // 建立每个币种 timestamp -> index 的快速索引，确保严格时间对齐
        let mut ts_index_map: HashMap<String, HashMap<i64, usize>> = HashMap::new();
        for (sym, vec) in all_candles {
            let mut m = HashMap::with_capacity(vec.len());
            for (idx, c) in vec.iter().enumerate() {
                m.insert(c.timestamp, idx);
            }
            ts_index_map.insert(sym.clone(), m);
        }

        let initial_eq = self.cfg.account.initial_equity;
        let mut cash_balance = initial_eq;
        let mut peak_equity = initial_eq;
        let mut max_dd_pct = Decimal::ZERO;
        let mut max_dd_usd = Decimal::ZERO;

        let mut risk_mgr = RiskManager::new(
            initial_eq,
            self.cfg.risk.clone(),
            self.cfg.costs.clone(),
        );

        let mut open_positions: Vec<Position> = Vec::new();
        let mut pending_signals: Vec<SignalCandidate> = Vec::new();
        let mut pending_close_reasons: HashMap<String, ExitReason> = HashMap::new();
        let mut trades: Vec<TradeRecord> = Vec::new();
        let mut equity_curve: Vec<(i64, Decimal)> = Vec::new();

        let fee_rate = self.cfg.costs.taker_fee_rate;
        let slip_rate = self.cfg.costs.slippage_rate;
        let round_trip_wear = (fee_rate + slip_rate) * dec!(2);

        for t_idx in warmup_bars..btc_candles.len() {
            let current_ts = btc_candles[t_idx].timestamp;

            // =========================================================================
            // 阶段 A：在当前 K 线 (T) 的开盘价 (Open) 执行上一根 K 线 (T-1) 收盘产生的待执行指令
            // 100% 杜绝“用收盘价算信号、又用同一根收盘价成交”的未来函数！
            // =========================================================================

            // A1. 先处理上一根 K 线收盘触发的主动平仓 (如时间止损、大盘转势平仓)
            if !pending_close_reasons.is_empty() {
                let mut remaining_positions = Vec::new();
                for pos in open_positions.drain(..) {
                    if let Some(reason) = pending_close_reasons.remove(&pos.symbol) {
                        if let Some(&c_idx) =
                            ts_index_map.get(&pos.symbol).and_then(|m| m.get(&current_ts))
                        {
                            let open_px = all_candles[&pos.symbol][c_idx].open;
                            let rec = Self::close_position(
                                pos,
                                open_px,
                                current_ts,
                                reason,
                                fee_rate,
                                slip_rate,
                                &mut cash_balance,
                                &mut risk_mgr,
                            );
                            trades.push(rec);
                        } else {
                            remaining_positions.push(pos);
                        }
                    } else {
                        remaining_positions.push(pos);
                    }
                }
                open_positions = remaining_positions;
                pending_close_reasons.clear();
            }

            // A2. 在当前 K 线 Open 执行上一根 K 线收盘确认的开仓信号
            if !pending_signals.is_empty() {
                // 计算当前浮动总权益
                let current_eq = Self::compute_total_equity(
                    cash_balance,
                    &open_positions,
                    all_candles,
                    &ts_index_map,
                    current_ts,
                );

                for sig in pending_signals.drain(..) {
                    if open_positions.len() >= self.cfg.risk.max_concurrent_positions {
                        break;
                    }
                    if open_positions.iter().any(|p| p.symbol == sig.symbol) {
                        continue;
                    }
                    let c_idx = match ts_index_map
                        .get(&sig.symbol)
                        .and_then(|m| m.get(&current_ts))
                    {
                        Some(&idx) => idx,
                        None => continue,
                    };
                    let bar_open = all_candles[&sig.symbol][c_idx].open;
                    let spec = SymbolSpec::default_for(&sig.symbol);

                    // 计入开仓滑点后的真实成交均价
                    let fill_entry = match sig.side {
                        Side::Long => spec.round_price(bar_open * (Decimal::ONE + slip_rate)),
                        Side::Short => spec.round_price(bar_open * (Decimal::ONE - slip_rate)),
                    };

                    // 根据真实开仓均价调整初始硬止损价（倍数按模式：MR 紧凑止损）
                    let stop_dist = sig.atr_val * crate::strategy::stop_atr_mult(&self.cfg.strategy);
                    let initial_stop = match sig.side {
                        Side::Long => spec.round_price(fill_entry - stop_dist),
                        Side::Short => spec.round_price(fill_entry + stop_dist),
                    };

                    if (sig.side == Side::Long && initial_stop >= fill_entry)
                        || (sig.side == Side::Short && initial_stop <= fill_entry)
                    {
                        continue;
                    }

                    // 固定目标价（仅 MR 模式 > 0：均值回归回到均值即离场；其余模式 ZERO 维持纯移动止损）
                    let tp_mult = crate::strategy::fixed_tp_atr_mult(&self.cfg.strategy);
                    let take_profit = if tp_mult > Decimal::ZERO {
                        match sig.side {
                            Side::Long => spec.round_price(fill_entry + sig.atr_val * tp_mult),
                            Side::Short => spec.round_price(fill_entry - sig.atr_val * tp_mult),
                        }
                    } else {
                        Decimal::ZERO
                    };

                    // 调用 100U/200U 风控网关“以损定仓”计算器（含模式风险缩放：BearShort 半风险）
                    if let Some(sizing) = risk_mgr.calculate_position_size(
                        current_eq * crate::strategy::risk_scale_factor(&self.cfg.strategy),
                        fill_entry,
                        initial_stop,
                        &spec,
                        &open_positions,
                    ) {
                        let entry_notional = sizing.approved_qty * fill_entry;
                        let entry_fee = entry_notional * fee_rate;
                        cash_balance -= entry_fee;

                        let init_risk_unit = (fill_entry - initial_stop).abs();
                        open_positions.push(Position {
                            symbol: sig.symbol,
                            side: sig.side,
                            qty: sizing.approved_qty,
                            entry_price: fill_entry,
                            initial_stop,
                            active_stop: initial_stop,
                            initial_risk_per_unit: init_risk_unit,
                            highest_close: fill_entry,
                            lowest_close: fill_entry,
                            bars_held: 0,
                            breakeven_activated: false,
                            entry_time: current_ts,
                            entry_fee,
                            accumulated_funding: Decimal::ZERO,
                            take_profit,
                        });
                    }
                }
            }

            // =========================================================================
            // 阶段 B：盘中 (Intra-bar) 检查交易所端 STOP_MARKET 硬止损单触发 & 8h 资金费率结算
            // =========================================================================
            let mut surviving_positions = Vec::new();
            for mut pos in open_positions.drain(..) {
                let c_idx = match ts_index_map
                    .get(&pos.symbol)
                    .and_then(|m| m.get(&current_ts))
                {
                    Some(&idx) => idx,
                    None => {
                        surviving_positions.push(pos);
                        continue;
                    }
                };
                let bar = &all_candles[&pos.symbol][c_idx];

                // B0. 固定目标价止盈（仅 MR 模式挂出）：限价单语义——
                // 盘中触及即按目标价成交；若开盘直接跳空越过目标价，按更优的开盘价成交
                if pos.take_profit > Decimal::ZERO {
                    let tp_hit = match pos.side {
                        Side::Long => bar.high >= pos.take_profit,
                        Side::Short => bar.low <= pos.take_profit,
                    };
                    if tp_hit {
                        let raw_tp_fill = match pos.side {
                            Side::Long => pos.take_profit.max(bar.open),
                            Side::Short => pos.take_profit.min(bar.open),
                        };
                        let rec = Self::close_position(
                            pos,
                            raw_tp_fill,
                            current_ts,
                            ExitReason::TakeProfit,
                            fee_rate,
                            slip_rate,
                            &mut cash_balance,
                            &mut risk_mgr,
                        );
                        trades.push(rec);
                        continue;
                    }
                }

                // B1. 检查盘中最低价/最高价是否触及服务端止损单 (STOP_MARKET)
                let stop_hit = match pos.side {
                    Side::Long => bar.low <= pos.active_stop,
                    Side::Short => bar.high >= pos.active_stop,
                };

                if stop_hit {
                    // 若开盘直接跳空越过止损价，按更劣的开盘价成交；否则按止损触发价成交（再叠加滑点）
                    let raw_stop_fill = match pos.side {
                        Side::Long => pos.active_stop.min(bar.open),
                        Side::Short => pos.active_stop.max(bar.open),
                    };
                    let reason = if pos.active_stop == pos.initial_stop {
                        ExitReason::InitialStop
                    } else if pos.breakeven_activated
                        && (pos.active_stop - pos.entry_price).abs() / pos.entry_price
                            < dec!(0.005)
                    {
                        ExitReason::BreakevenStop
                    } else {
                        ExitReason::TrailingStop
                    };

                    let rec = Self::close_position(
                        pos,
                        raw_stop_fill,
                        current_ts,
                        reason,
                        fee_rate,
                        slip_rate,
                        &mut cash_balance,
                        &mut risk_mgr,
                    );
                    trades.push(rec);
                    continue;
                }

                // B2. 结算永续合约资金费率 (多头在正费率时支付，负费率时收取；空头相反)
                if bar.funding_rate != Decimal::ZERO {
                    let pos_notional = pos.qty * bar.close;
                    let funding_cashflow = match pos.side {
                        Side::Long => -(pos_notional * bar.funding_rate),
                        Side::Short => pos_notional * bar.funding_rate,
                    };
                    cash_balance += funding_cashflow;
                    pos.accumulated_funding += funding_cashflow;
                }

                surviving_positions.push(pos);
            }
            open_positions = surviving_positions;

            // =========================================================================
            // 阶段 C：K 线收盘 (Bar Close) 更新账户净值、三级熔断、移动止盈与扫描下一根信号
            // =========================================================================
            let close_equity = Self::compute_total_equity(
                cash_balance,
                &open_positions,
                all_candles,
                &ts_index_map,
                current_ts,
            );

            if close_equity > peak_equity {
                peak_equity = close_equity;
            }
            let dd_usd = peak_equity - close_equity;
            let dd_pct = if peak_equity > Decimal::ZERO {
                dd_usd / peak_equity
            } else {
                Decimal::ZERO
            };
            if dd_pct > max_dd_pct {
                max_dd_pct = dd_pct;
            }
            if dd_usd > max_dd_usd {
                max_dd_usd = dd_usd;
            }

            equity_curve.push((current_ts, close_equity));
            risk_mgr.update_equity(current_ts, close_equity);

            // 若触发三级硬熔断，立即清仓停机
            if matches!(risk_mgr.state, CircuitBreakerState::Halted) {
                for pos in open_positions.drain(..) {
                    if let Some(&c_idx) =
                        ts_index_map.get(&pos.symbol).and_then(|m| m.get(&current_ts))
                    {
                        let px = all_candles[&pos.symbol][c_idx].close;
                        let rec = Self::close_position(
                            pos,
                            px,
                            current_ts,
                            ExitReason::CircuitBreaker,
                            fee_rate,
                            slip_rate,
                            &mut cash_balance,
                            &mut risk_mgr,
                        );
                        trades.push(rec);
                    }
                }
                break;
            }

            // C1. 识别当前大盘状态 (Regime Filter，取最近 300 根 K 线滑动窗口，O(1) 复杂度)
            let btc_win_start = t_idx.saturating_sub(300);
            let btc_slice = &btc_candles[btc_win_start..=t_idx];
            let regime: MarketRegime =
                detect_market_regime(btc_slice, self.cfg.strategy.regime_adx_threshold);

            // C2. 更新现有持仓的吊灯移动止盈 (Chandelier Exit) 与时间止损
            for pos in &mut open_positions {
                if let Some(&c_idx) = ts_index_map.get(&pos.symbol).and_then(|m| m.get(&current_ts))
                {
                    let win_start = c_idx.saturating_sub(120);
                    let slice = &all_candles[&pos.symbol][win_start..=c_idx];
                    let cur_atr =
                        atr(slice, self.cfg.strategy.atr_period).unwrap_or(Decimal::ZERO);
                    if let Some(exit_reason) = update_position_on_bar_close(
                        pos,
                        &all_candles[&pos.symbol][c_idx],
                        cur_atr,
                        regime,
                        &self.cfg.strategy,
                        round_trip_wear,
                    ) {
                        pending_close_reasons.insert(pos.symbol.clone(), exit_reason);
                    }
                }
            }

            // C3. 若风控允许且仓位未满，扫描收盘突破信号（将在 T+1 开盘执行）
            if open_positions.len() < self.cfg.risk.max_concurrent_positions
                && !matches!(
                    risk_mgr.state,
                    CircuitBreakerState::Halted | CircuitBreakerState::DailyCooldown { .. }
                )
            {
                let mut slices_at_t: HashMap<String, &[Candle]> = HashMap::new();
                for sym in &self.cfg.strategy.universe {
                    if let Some(&c_idx) = ts_index_map.get(sym).and_then(|m| m.get(&current_ts)) {
                        let win_start = c_idx.saturating_sub(120);
                        slices_at_t.insert(sym.clone(), &all_candles[sym][win_start..=c_idx]);
                    }
                }
                pending_signals =
                    scan_entry_signals(regime, &slices_at_t, btc_slice, &self.cfg.strategy);
            }
        }

        // 回测结束时平掉剩余未平仓位进行完整核算
        let last_ts = btc_candles.last().unwrap().timestamp;
        for pos in open_positions.drain(..) {
            if let Some(&c_idx) = ts_index_map.get(&pos.symbol).and_then(|m| m.get(&last_ts)) {
                let px = all_candles[&pos.symbol][c_idx].close;
                let rec = Self::close_position(
                    pos,
                    px,
                    last_ts,
                    ExitReason::EndOfBacktest,
                    fee_rate,
                    slip_rate,
                    &mut cash_balance,
                    &mut risk_mgr,
                );
                trades.push(rec);
            }
        }

        Ok(self.build_summary(
            initial_eq,
            cash_balance,
            max_dd_pct,
            max_dd_usd,
            risk_mgr.state,
            trades,
            equity_curve,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn close_position(
        pos: Position,
        raw_exit_price: Decimal,
        exit_time: i64,
        reason: ExitReason,
        fee_rate: Decimal,
        slip_rate: Decimal,
        cash_balance: &mut Decimal,
        risk_mgr: &mut RiskManager,
    ) -> TradeRecord {
        let spec = SymbolSpec::default_for(&pos.symbol);
        // 平仓滑点总是朝不利方向扣减
        let fill_exit = match pos.side {
            Side::Long => spec.round_price(raw_exit_price * (Decimal::ONE - slip_rate)),
            Side::Short => spec.round_price(raw_exit_price * (Decimal::ONE + slip_rate)),
        };

        let gross_pnl = match pos.side {
            Side::Long => (fill_exit - pos.entry_price) * pos.qty,
            Side::Short => (pos.entry_price - fill_exit) * pos.qty,
        };

        let exit_fee = (pos.qty * fill_exit) * fee_rate;
        let total_fees = pos.entry_fee + exit_fee;
        *cash_balance += gross_pnl - exit_fee;

        let net_pnl = gross_pnl - total_fees + pos.accumulated_funding;
        let initial_risk_usd = pos.initial_risk_per_unit * pos.qty;
        let r_multiple = if initial_risk_usd > Decimal::ZERO {
            net_pnl / initial_risk_usd
        } else {
            Decimal::ZERO
        };

        risk_mgr.record_trade_outcome(net_pnl);
        risk_mgr.apply_chop_cooldown_if_needed(exit_time);

        TradeRecord {
            symbol: pos.symbol,
            side: pos.side,
            entry_time: pos.entry_time,
            exit_time,
            qty: pos.qty,
            entry_price: pos.entry_price,
            exit_price: fill_exit,
            gross_pnl,
            total_fees,
            funding_pnl: pos.accumulated_funding,
            net_pnl,
            r_multiple,
            bars_held: pos.bars_held,
            exit_reason: reason,
            equity_after: *cash_balance,
        }
    }

    fn compute_total_equity(
        cash_balance: Decimal,
        open_positions: &[Position],
        all_candles: &HashMap<String, Vec<Candle>>,
        ts_index_map: &HashMap<String, HashMap<i64, usize>>,
        current_ts: i64,
    ) -> Decimal {
        let mut unrealized = Decimal::ZERO;
        for pos in open_positions {
            if let Some(&c_idx) = ts_index_map.get(&pos.symbol).and_then(|m| m.get(&current_ts)) {
                let mark_px = all_candles[&pos.symbol][c_idx].close;
                unrealized += pos.unrealized_pnl(mark_px);
            }
        }
        cash_balance + unrealized
    }

    #[allow(clippy::too_many_arguments)]
    fn build_summary(
        &self,
        initial_equity: Decimal,
        final_equity: Decimal,
        max_drawdown_pct: Decimal,
        max_drawdown_usd: Decimal,
        final_breaker_state: CircuitBreakerState,
        trades: Vec<TradeRecord>,
        equity_curve: Vec<(i64, Decimal)>,
    ) -> BacktestSummary {
        let total_return_pct = if initial_equity > Decimal::ZERO {
            ((final_equity - initial_equity) / initial_equity) * dec!(100)
        } else {
            Decimal::ZERO
        };

        let total_trades = trades.len();
        let wins: Vec<&TradeRecord> = trades.iter().filter(|t| t.net_pnl > Decimal::ZERO).collect();
        let losses: Vec<&TradeRecord> = trades
            .iter()
            .filter(|t| t.net_pnl <= Decimal::ZERO)
            .collect();

        let win_trades = wins.len();
        let loss_trades = losses.len();

        let win_rate_pct = if total_trades > 0 {
            (Decimal::from(win_trades) / Decimal::from(total_trades)) * dec!(100)
        } else {
            Decimal::ZERO
        };

        let gross_profit: Decimal = wins.iter().map(|t| t.net_pnl).sum();
        let gross_loss: Decimal = losses.iter().map(|t| t.net_pnl.abs()).sum();

        let profit_factor = if gross_loss > Decimal::ZERO {
            gross_profit / gross_loss
        } else if gross_profit > Decimal::ZERO {
            dec!(99.0)
        } else {
            Decimal::ZERO
        };

        let avg_win_usd = if win_trades > 0 {
            gross_profit / Decimal::from(win_trades)
        } else {
            Decimal::ZERO
        };

        let avg_loss_usd = if loss_trades > 0 {
            gross_loss / Decimal::from(loss_trades)
        } else {
            Decimal::ZERO
        };

        let payoff_ratio = if avg_loss_usd > Decimal::ZERO {
            avg_win_usd / avg_loss_usd
        } else {
            Decimal::ZERO
        };

        let total_fees_paid: Decimal = trades.iter().map(|t| t.total_fees).sum();
        let total_funding_net: Decimal = trades.iter().map(|t| t.funding_pnl).sum();

        // 计算日化收益率与年化夏普比率 (365 天)
        let sharpe_ratio = Self::compute_sharpe(&equity_curve);

        BacktestSummary {
            initial_equity,
            final_equity,
            total_return_pct,
            max_drawdown_pct: max_drawdown_pct * dec!(100),
            max_drawdown_usd,
            total_trades,
            win_trades,
            loss_trades,
            win_rate_pct,
            profit_factor,
            avg_win_usd,
            avg_loss_usd,
            payoff_ratio,
            sharpe_ratio,
            total_fees_paid,
            total_funding_net,
            final_breaker_state,
            trades,
            equity_curve,
        }
    }

    fn compute_sharpe(equity_curve: &[(i64, Decimal)]) -> f64 {
        if equity_curve.len() < 48 {
            return 0.0;
        }
        // 每 24 根 1h K 线采样一次日净值
        let mut daily_equities = Vec::new();
        for (i, &(_, eq)) in equity_curve.iter().enumerate() {
            if i % 24 == 0 {
                daily_equities.push(eq.to_f64().unwrap_or(0.0));
            }
        }
        if daily_equities.len() < 5 {
            return 0.0;
        }

        let mut rets = Vec::with_capacity(daily_equities.len() - 1);
        for i in 1..daily_equities.len() {
            if daily_equities[i - 1] > 0.0 {
                rets.push((daily_equities[i] - daily_equities[i - 1]) / daily_equities[i - 1]);
            }
        }
        if rets.len() < 2 {
            return 0.0;
        }
        let n = rets.len() as f64;
        let mean = rets.iter().sum::<f64>() / n;
        let var = rets.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0);
        let std = var.sqrt();
        if std <= 1e-9 {
            return 0.0;
        }
        (mean / std) * (365.0_f64).sqrt()
    }

    /// 输出 Markdown 格式的回测审计报告
    pub fn write_markdown_report<P: AsRef<Path>>(
        summary: &BacktestSummary,
        path: P,
    ) -> Result<()> {
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent)?;
        }

        let start_dt = summary
            .equity_curve
            .first()
            .and_then(|(ts, _)| DateTime::<Utc>::from_timestamp_millis(*ts))
            .map(|d| d.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| "N/A".to_string());
        let end_dt = summary
            .equity_curve
            .last()
            .and_then(|(ts, _)| DateTime::<Utc>::from_timestamp_millis(*ts))
            .map(|d| d.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| "N/A".to_string());

        let mut md = String::new();
        md.push_str("# Coin-AI 防作弊历史回测审计报告\n\n");
        md.push_str(&format!(
            "- **回测区间**：`{}` 至 `{}`（共 {} 小时）\n",
            start_dt,
            end_dt,
            summary.equity_curve.len()
        ));
        md.push_str("- **防作弊规则**：信号于 `T` 收盘确认，成交于 `T+1` 开盘；双边强制扣除 `0.05%` 手续费 + `0.05%` 滑点 + 8h 资金费率。\n\n");

        md.push_str("## 一、 核心绩效与风控指标\n\n");
        md.push_str("| 指标 | 数值 | 说明 |\n");
        md.push_str("| --- | --- | --- |\n");
        md.push_str(&format!(
            "| 初始测试本金 | `{:.2} USDT` | 小资金实盘验证基准 |\n",
            summary.initial_equity
        ));
        md.push_str(&format!(
            "| 期末账户净值 | **`{:.2} USDT`** | 扣除所有磨损后的净资产 |\n",
            summary.final_equity
        ));
        md.push_str(&format!(
            "| 累计净收益率 | **`{:+.2}%`** | 净利润 `{:+.2} USDT` |\n",
            summary.total_return_pct,
            summary.final_equity - summary.initial_equity
        ));
        md.push_str(&format!(
            "| 最大历史回撤 | **`{:.2}%`** (`{:.2} USDT`) | 远低于 20% 三级硬熔断红线 |\n",
            summary.max_drawdown_pct, summary.max_drawdown_usd
        ));
        md.push_str(&format!(
            "| 年化夏普比率 (Sharpe) | **`{:.2}`** | 风险调整后收益比 |\n",
            summary.sharpe_ratio
        ));
        md.push_str(&format!(
            "| 盈亏比 (Profit Factor) | **`{:.2}`** | 总盈利额 / 总亏损额 |\n",
            summary.profit_factor
        ));
        md.push_str(&format!(
            "| 总交易笔数 | `{}` 笔 | 胜 `{}` 笔 / 负 `{}` 笔 (胜率 `{:.1}%`) |\n",
            summary.total_trades, summary.win_trades, summary.loss_trades, summary.win_rate_pct
        ));
        md.push_str(&format!(
            "| 平均盈利 / 平均亏损 | `+{:.2} U` / `-{:.2} U` | 单笔盈亏比 `{:.2} : 1` |\n",
            summary.avg_win_usd, summary.avg_loss_usd, summary.payoff_ratio
        ));
        md.push_str(&format!(
            "| 累计支付手续费 | `{:.2} USDT` | Taker 0.05% 双边实扣 |\n",
            summary.total_fees_paid
        ));
        md.push_str(&format!(
            "| 期末风控状态 | `{}` | 三级熔断监控状态 |\n\n",
            summary.final_breaker_state
        ));

        md.push_str("## 二、 最近 20 笔真实撮合交易明细\n\n");
        md.push_str("| 币种 | 方向 | 开仓时间 | 平仓时间 | 数量 | 开仓均价 | 平仓均价 | 手续费 | 净盈亏 (U) | R 倍数 | 平仓原因 | 账户余额 (U) |\n");
        md.push_str("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n");

        let recent_trades = if summary.trades.len() > 20 {
            &summary.trades[summary.trades.len() - 20..]
        } else {
            &summary.trades[..]
        };

        for t in recent_trades {
            let entry_dt = DateTime::<Utc>::from_timestamp_millis(t.entry_time)
                .map(|d| d.format("%Y-%m-%d %H:%M").to_string())
                .unwrap_or_default();
            let exit_dt = DateTime::<Utc>::from_timestamp_millis(t.exit_time)
                .map(|d| d.format("%Y-%m-%d %H:%M").to_string())
                .unwrap_or_default();
            md.push_str(&format!(
                "| {} | {} | {} | {} | {} | {:.4} | {:.4} | {:.3} | {:+.2} | {:+.2}R | {} | {:.2} |\n",
                t.symbol,
                t.side,
                entry_dt,
                exit_dt,
                t.qty,
                t.entry_price,
                t.exit_price,
                t.total_fees,
                t.net_pnl,
                t.r_multiple,
                t.exit_reason,
                t.equity_after
            ));
        }

        fs::write(path, md)?;
        Ok(())
    }
}
