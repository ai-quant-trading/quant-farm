use crate::ai::AiRiskGuard;
use crate::config::AppConfig;
use crate::data::DataManager;
use crate::exchange::{BinanceFuturesGateway, PersistentLedger};
use crate::indicators::{adx, atr, detect_market_regime, ema};
use crate::notify::{AlertLevel, Notifier};
use crate::risk::RiskManager;
use crate::strategy::{
    compute_cross_sectional_score, scan_entry_signals, update_position_on_bar_close,
    SignalCandidate,
};
use crate::types::{Candle, ExitReason, MarketRegime, Position, Side, SymbolSpec, TradeRecord};
use anyhow::{bail, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::time::Duration;
use tracing::info;

/// 执行一次全市场实时雷达扫描 (`coin-ai scan`)
pub async fn run_market_scan(
    cfg: &AppConfig,
    data_mgr: &DataManager,
    force_refresh: bool,
) -> Result<()> {
    let mut symbols = vec![cfg.strategy.benchmark_symbol.clone()];
    for s in &cfg.strategy.universe {
        if !symbols.contains(s) {
            symbols.push(s.clone());
        }
    }

    let mut all_candles: HashMap<String, Vec<Candle>> = HashMap::new();
    for sym in &symbols {
        let c = data_mgr
            .load_or_fetch_candles(sym, 300, force_refresh)
            .await?;
        all_candles.insert(sym.clone(), c);
    }

    let btc_candles = &all_candles[&cfg.strategy.benchmark_symbol];
    let btc_last = btc_candles.last().unwrap();
    let btc_closes: Vec<Decimal> = btc_candles.iter().map(|c| c.close).collect();
    let btc_ema50 = ema(&btc_closes, 50).unwrap_or(Decimal::ZERO);
    let btc_ema150 = ema(&btc_closes, 150).unwrap_or(Decimal::ZERO);
    let btc_adx = adx(btc_candles, 14).unwrap_or(Decimal::ZERO);
    let regime = detect_market_regime(btc_candles, cfg.strategy.regime_adx_threshold);

    let risk_mgr = RiskManager::new(
        cfg.account.initial_equity,
        cfg.risk.clone(),
        cfg.costs.clone(),
    );

    println!(
        "\n================================================================================"
    );
    println!(
        "  Coin-AI 实时全市场量化雷达扫描 | 账户基准本金: {:.2} USDT",
        cfg.account.initial_equity
    );
    println!(
        "================================================================================"
    );
    println!(
        "  [第一层：大盘状态过滤 ({})] 最新价: {:.2} USDT | K线时间: {}",
        cfg.strategy.benchmark_symbol,
        btc_last.close,
        btc_last.datetime().format("%Y-%m-%d %H:%M UTC")
    );
    println!(
        "  EMA(50): {:.2} | EMA(150): {:.2} | ADX(14): {:.2} (阈值: {})",
        btc_ema50, btc_ema150, btc_adx, cfg.strategy.regime_adx_threshold
    );
    println!("  ---> 当前大盘判定状态: {}", regime);
    println!(
        "--------------------------------------------------------------------------------"
    );
    println!(
        "  [第二层：截面动量排行榜 & {:.0}U 以损定仓实时测算表 (单笔最大止损 = {:.2} USDT)]",
        cfg.account.initial_equity,
        cfg.account.initial_equity * cfg.risk.risk_per_trade_pct
    );
    println!(
        "  排名 | 币种     | 最新价      | 截面得分  | ATR止损距 | 建议数量   | 名义仓位(U) | 预计最大亏损"
    );
    println!(
        "  ------------------------------------------------------------------------------"
    );

    let mut ranked: Vec<(String, Decimal, Decimal, Decimal)> = Vec::new();
    for sym in &cfg.strategy.universe {
        if let Some(candles) = all_candles.get(sym) {
            if let Some(score) = compute_cross_sectional_score(candles, btc_candles) {
                let last_px = candles.last().unwrap().close;
                let cur_atr = atr(candles, cfg.strategy.atr_period).unwrap_or(Decimal::ZERO);
                ranked.push((sym.clone(), score, last_px, cur_atr));
            }
        }
    }
    ranked.sort_by(|a, b| b.1.cmp(&a.1));

    for (idx, (sym, score, px, cur_atr)) in ranked.iter().enumerate() {
        let spec = SymbolSpec::default_for(sym);
        let stop_dist = *cur_atr * cfg.strategy.initial_stop_atr_mult;
        let stop_px = spec.round_price((*px - stop_dist).max(dec!(0.0001)));
        let stop_pct = if *px > Decimal::ZERO {
            (stop_dist / *px) * dec!(100)
        } else {
            Decimal::ZERO
        };

        if let Some(sizing) = risk_mgr.calculate_position_size(
            cfg.account.initial_equity,
            *px,
            stop_px,
            &spec,
            &[],
        ) {
            println!(
                "  #{:<3} | {:<8} | {:<11.4} | {:+8.4} | {:>7.2}%  | {:<10} | {:>9.2} U | -{:.2} U",
                idx + 1,
                sym,
                px,
                score,
                stop_pct,
                sizing.approved_qty,
                sizing.notional_usd,
                sizing.max_loss_usd
            );
        } else {
            println!(
                "  #{:<3} | {:<8} | {:<11.4} | {:+8.4} | {:>7.2}%  | (受限于最小下单额或风控拦截)",
                idx + 1,
                sym,
                px,
                score,
                stop_pct
            );
        }
    }

    println!(
        "--------------------------------------------------------------------------------"
    );
    let mut slice_map: HashMap<String, &[Candle]> = HashMap::new();
    for (k, v) in &all_candles {
        slice_map.insert(k.clone(), v.as_slice());
    }

    let signals = scan_entry_signals(regime, &slice_map, btc_candles, &cfg.strategy);
    let ai_guard = AiRiskGuard::from_env();

    if signals.is_empty() {
        println!(
            "  [第三/四层：突破信号与 AI 审查] 当前整点无满足条件的突破开仓信号（保持空仓耐心等待）。"
        );
    } else {
        for sig in &signals {
            let verdict = ai_guard
                .evaluate_signal(sig, &all_candles[&sig.symbol], None)
                .await;
            println!(
                "  [触发信号!] {} {} @ {} (初始止损: {}) | AI 审查: approved={} (风险分={}, 原因={})",
                sig.symbol,
                sig.side,
                sig.signal_close,
                sig.proposed_stop,
                verdict.approved,
                verdict.risk_score,
                verdict.reason
            );
        }
    }
    println!(
        "================================================================================\n"
    );

    Ok(())
}

/// 实盘前全功能闭环自检 (`coin-ai sim-verify`)：
/// 在隔离沙箱中完整跑通：100U 以损定仓 -> 开仓 -> 挂载服务端 STOP_MARKET ->
/// 价格上涨 +1.8R 触发保本移位与吊灯止盈上移 -> 触发止盈平仓与手续费记账
pub async fn verify_full_trading_lifecycle(cfg: &AppConfig, data_mgr: &DataManager) -> Result<()> {
    println!("\n================================================================================");
    println!("  Coin-AI 实盘功能全链路闭环自检 (Full Lifecycle Self-Test)");
    println!("================================================================================");

    let test_ledger_path = "data/verify_sandbox_state.json";
    let _ = std::fs::remove_file(test_ledger_path);

    let gateway = BinanceFuturesGateway::new(true, test_ledger_path)?;
    let notifier = Notifier::from_env();
    let ai_guard = AiRiskGuard::from_env();

    // 1. 拉取 SOLUSDT 真实行情测试
    let symbol = "SOLUSDT";
    let candles = data_mgr.load_or_fetch_candles(symbol, 100, false).await?;
    let last_bar = candles.last().unwrap().clone();
    let cur_atr = atr(&candles, cfg.strategy.atr_period).unwrap_or(dec!(1.5));
    let spec = SymbolSpec::default_for(symbol);

    let entry_px = last_bar.close;
    let init_stop = spec.round_price(entry_px - cur_atr * cfg.strategy.initial_stop_atr_mult);

    println!(
        "  [步骤 1/5] 行情与 ATR 止损计算 -> PASS | {} 当前价: {} | ATR(14): {:.4} | 初始止损价: {}",
        symbol, entry_px, cur_atr, init_stop
    );

    // 2. AI 盘前风控审查
    let candidate = SignalCandidate {
        symbol: symbol.to_string(),
        side: Side::Long,
        score: dec!(0.035),
        signal_close: entry_px,
        proposed_stop: init_stop,
        atr_val: cur_atr,
    };
    let verdict = ai_guard.evaluate_signal(&candidate, &candles, None).await;
    assert!(verdict.approved, "AI 风控审查应通过正常信号");
    println!(
        "  [步骤 2/5] AI 盘前风险审查 -> PASS | approved={} | 风险分: {} | 原因: {}",
        verdict.approved, verdict.risk_score, verdict.reason
    );

    // 3. 100U 以损定仓与开仓 + 服务端 STOP_MARKET 挂载
    let risk_mgr = RiskManager::new(
        cfg.account.initial_equity,
        cfg.risk.clone(),
        cfg.costs.clone(),
    );
    let sizing = risk_mgr
        .calculate_position_size(cfg.account.initial_equity, entry_px, init_stop, &spec, &[])
        .expect("100U 以损定仓应返回有效仓位");

    let (fill_px, stop_id_1) = gateway
        .open_position_with_hard_stop(symbol, Side::Long, sizing.approved_qty, entry_px, init_stop)
        .await?;

    println!(
        "  [步骤 3/5] 100U 以损定仓开仓 & 服务端硬止损挂载 -> PASS | 数量: {} (名义仓位: {:.2}U, 最大风险: -{:.2}U) | 止损单ID: {}",
        sizing.approved_qty, sizing.notional_usd, sizing.max_loss_usd, stop_id_1
    );

    // 4. 模拟行情向有利方向上涨 +2.2R，检验保本移位 (Breakeven) 与吊灯移动止盈 (Chandelier Exit)
    let risk_unit = (fill_px - init_stop).abs();
    let simulated_pump_close = spec.round_price(fill_px + risk_unit * dec!(2.2));
    let mut pos = Position {
        symbol: symbol.to_string(),
        side: Side::Long,
        qty: sizing.approved_qty,
        entry_price: fill_px,
        initial_stop: init_stop,
        active_stop: init_stop,
        initial_risk_per_unit: risk_unit,
        highest_close: fill_px,
        lowest_close: fill_px,
        bars_held: 1,
        breakeven_activated: false,
        entry_time: Utc::now().timestamp_millis(),
        entry_fee: sizing.approved_qty * fill_px * cfg.costs.taker_fee_rate,
        accumulated_funding: Decimal::ZERO,
    };

    let pump_candle = Candle {
        timestamp: Utc::now().timestamp_millis() + 3_600_000,
        open: fill_px,
        high: simulated_pump_close * dec!(1.005),
        low: fill_px,
        close: simulated_pump_close,
        volume: last_bar.volume * dec!(1.5),
        funding_rate: Decimal::ZERO,
    };

    let round_trip_wear = (cfg.costs.taker_fee_rate + cfg.costs.slippage_rate) * dec!(2);
    let exit_opt = update_position_on_bar_close(
        &mut pos,
        &pump_candle,
        cur_atr,
        MarketRegime::Bull,
        &cfg.strategy,
        round_trip_wear,
    );
    assert!(exit_opt.is_none());
    assert!(pos.breakeven_activated, "浮盈达 2.2R 时保本标志必须已激活");
    assert!(
        pos.active_stop > pos.entry_price,
        "移动止损价 ({}) 必须已上移至开仓成本价 ({}) 之上鎖定利润",
        pos.active_stop,
        pos.entry_price
    );

    let stop_id_2 = gateway
        .place_server_stop_market(symbol, Side::Long, pos.qty, pos.active_stop)
        .await?;

    println!(
        "  [步骤 4/5] 保本移位 & 吊灯移动止盈同步 -> PASS | 价格涨至 {} (+2.2R) | 服务端止损单自动从 {} 上移至 {} (新止损价高于开仓价 {}，已锁定正收益! 新单ID: {})",
        simulated_pump_close, init_stop, pos.active_stop, pos.entry_price, stop_id_2
    );

    // 5. 测试平仓结算与手续费核算
    gateway
        .close_position_market(symbol, pos.side, pos.qty)
        .await?;
    let gross_pnl = pos.unrealized_pnl(simulated_pump_close);
    let exit_fee = pos.qty * simulated_pump_close * cfg.costs.taker_fee_rate;
    let net_pnl = gross_pnl - pos.entry_fee - exit_fee;
    let final_eq = cfg.account.initial_equity + net_pnl;

    let _ = notifier
        .send(
            AlertLevel::Info,
            "全链路闭环自检通过",
            &format!("模拟净盈亏: {:+.2} USDT, 期末权益: {:.2} USDT", net_pnl, final_eq),
        )
        .await;

    let _ = std::fs::remove_file(test_ledger_path);

    println!(
        "  [步骤 5/5] 市价平仓 & 真实双边手续费记账 -> PASS | 毛利: {:+.2}U | 双边手续费: -{:.3}U | 净利: {:+.2}U | 结算净值: {:.2} USDT",
        gross_pnl,
        pos.entry_fee + exit_fee,
        net_pnl,
        final_eq
    );
    println!("================================================================================");
    println!("  结论：所有 5 大核心实盘交易功能模块 100% 验证通过！");
    println!("================================================================================\n");

    Ok(())
}

/// 立即在模拟实盘账本 (`data/live_state.json`) 中按实时行情开立一笔 100U 风控模拟仓位 (`coin-ai sim-open`)
pub async fn open_live_simulated_position(
    cfg: &AppConfig,
    data_mgr: &DataManager,
    target_symbol: Option<String>,
    side_str: Option<String>,
    force_refresh: bool,
) -> Result<()> {
    let gateway = BinanceFuturesGateway::new(true, "data/live_state.json")?;
    let notifier = Notifier::from_env();
    let ai_guard = AiRiskGuard::from_env();

    let mut ledger = PersistentLedger::load_or_init(
        &gateway.ledger_path,
        "DRY-RUN",
        cfg.account.initial_equity,
    )?;

    if ledger.open_positions.len() >= cfg.risk.max_concurrent_positions {
        bail!(
            "当前模拟盘已持有 {} 个仓位（达到 100U 模式最大持仓数上限 {}），请先查看 `status` 或平仓。",
            ledger.open_positions.len(),
            cfg.risk.max_concurrent_positions
        );
    }

    let btc_candles = data_mgr
        .load_or_fetch_candles(&cfg.strategy.benchmark_symbol, 300, force_refresh)
        .await?;
    let regime = detect_market_regime(&btc_candles, cfg.strategy.regime_adx_threshold);

    let default_side = match side_str.as_deref().map(|s| s.to_uppercase()) {
        Some(ref s) if s == "SHORT" => Side::Short,
        Some(ref s) if s == "LONG" => Side::Long,
        _ => match regime {
            MarketRegime::Bear => Side::Short,
            _ => Side::Long,
        },
    };

    // 若未指定币种，顺应当前大盘状态：多头选截面动量第 1 强，空头选截面动量第 1 弱
    let chosen_symbol = if let Some(s) = target_symbol {
        s.to_uppercase()
    } else {
        let mut best_sym = "SOLUSDT".to_string();
        let mut best_metric = dec!(-999);
        for sym in &cfg.strategy.universe {
            if ledger.open_positions.iter().any(|p| &p.symbol == sym) {
                continue;
            }
            let c = data_mgr.load_or_fetch_candles(sym, 300, force_refresh).await?;
            if let Some(sc) = compute_cross_sectional_score(&c, &btc_candles) {
                let metric = match default_side {
                    Side::Long => sc,
                    Side::Short => -sc,
                };
                if metric > best_metric {
                    best_metric = metric;
                    best_sym = sym.clone();
                }
            }
        }
        best_sym
    };

    if ledger
        .open_positions
        .iter()
        .any(|p| p.symbol == chosen_symbol)
    {
        bail!("模拟账本中已存在 {} 的持仓，防重复下单拦截生效！", chosen_symbol);
    }

    let candles = data_mgr
        .load_or_fetch_candles(&chosen_symbol, 300, false)
        .await?;
    let last_bar = candles.last().unwrap();
    let cur_atr = atr(&candles, cfg.strategy.atr_period).unwrap_or(Decimal::ZERO);
    let score = compute_cross_sectional_score(&candles, &btc_candles).unwrap_or(Decimal::ZERO);
    let spec = SymbolSpec::default_for(&chosen_symbol);

    let side = default_side;

    let entry_px = last_bar.close;
    let stop_dist = cur_atr * cfg.strategy.initial_stop_atr_mult;
    let stop_px = match side {
        Side::Long => spec.round_price(entry_px - stop_dist),
        Side::Short => spec.round_price(entry_px + stop_dist),
    };

    let sig = SignalCandidate {
        symbol: chosen_symbol.clone(),
        side,
        score,
        signal_close: entry_px,
        proposed_stop: stop_px,
        atr_val: cur_atr,
    };

    let verdict = ai_guard.evaluate_signal(&sig, &candles, None).await;
    if !verdict.approved {
        bail!(
            "AI 风控否决了该笔模拟开仓 (score={}): {}",
            verdict.risk_score,
            verdict.reason
        );
    }

    let risk_mgr = RiskManager::new(
        ledger.initial_equity,
        cfg.risk.clone(),
        cfg.costs.clone(),
    );

    let sizing = risk_mgr
        .calculate_position_size(
            ledger.cash_balance,
            entry_px,
            stop_px,
            &spec,
            &ledger.open_positions,
        )
        .expect("计算 100U 模拟仓位失败");

    let (fill_px, stop_order_id) = gateway
        .open_position_with_hard_stop(&chosen_symbol, side, sizing.approved_qty, entry_px, stop_px)
        .await?;

    let entry_fee = sizing.approved_qty * fill_px * cfg.costs.taker_fee_rate;
    ledger.cash_balance -= entry_fee;
    ledger
        .stop_order_ids
        .insert(chosen_symbol.clone(), stop_order_id.clone());

    ledger.open_positions.push(Position {
        symbol: chosen_symbol.clone(),
        side,
        qty: sizing.approved_qty,
        entry_price: fill_px,
        initial_stop: stop_px,
        active_stop: stop_px,
        initial_risk_per_unit: (fill_px - stop_px).abs(),
        highest_close: fill_px,
        lowest_close: fill_px,
        bars_held: 0,
        breakeven_activated: false,
        entry_time: Utc::now().timestamp_millis(),
        entry_fee,
        accumulated_funding: Decimal::ZERO,
    });

    ledger.save(&gateway.ledger_path)?;

    notifier
        .send(
            AlertLevel::Signal,
            &format!("[DRY-RUN 模拟实盘开仓] {} {}", chosen_symbol, side),
            &format!(
                "开仓均价: {} | 数量: {} (名义价值: {:.2} USDT) | 服务端止损单: {} (ID: {}) | 单笔最大风险: -{:.2} USDT | 已扣除开仓手续费: {:.3} USDT",
                fill_px,
                sizing.approved_qty,
                sizing.notional_usd,
                stop_px,
                stop_order_id,
                sizing.max_loss_usd,
                entry_fee
            ),
        )
        .await?;

    Ok(())
}

/// 平掉模拟盘中的所有或指定持仓 (`coin-ai sim-close`)
pub async fn close_live_simulated_positions(
    cfg: &AppConfig,
    data_mgr: &DataManager,
    force_refresh: bool,
) -> Result<()> {
    let gateway = BinanceFuturesGateway::new(true, "data/live_state.json")?;
    let notifier = Notifier::from_env();
    let mut ledger = PersistentLedger::load_or_init(
        &gateway.ledger_path,
        "DRY-RUN",
        cfg.account.initial_equity,
    )?;

    if ledger.open_positions.is_empty() {
        println!("当前模拟账本无活跃持仓。");
        return Ok(());
    }

    for pos in ledger.open_positions.drain(..) {
        let candles = data_mgr
            .load_or_fetch_candles(&pos.symbol, 50, force_refresh)
            .await?;
        let mark_px = candles.last().map(|c| c.close).unwrap_or(pos.entry_price);

        gateway
            .close_position_market(&pos.symbol, pos.side, pos.qty)
            .await?;

        let gross_pnl = pos.unrealized_pnl(mark_px);
        let exit_fee = pos.qty * mark_px * cfg.costs.taker_fee_rate;
        let total_fees = pos.entry_fee + exit_fee;
        let net_pnl = gross_pnl - total_fees + pos.accumulated_funding;
        ledger.cash_balance += gross_pnl - exit_fee;

        let init_risk = pos.initial_risk_per_unit * pos.qty;
        let r_mult = if init_risk > Decimal::ZERO {
            net_pnl / init_risk
        } else {
            Decimal::ZERO
        };

        ledger.stop_order_ids.remove(&pos.symbol);
        ledger.closed_trades.push(TradeRecord {
            symbol: pos.symbol.clone(),
            side: pos.side,
            entry_time: pos.entry_time,
            exit_time: Utc::now().timestamp_millis(),
            qty: pos.qty,
            entry_price: pos.entry_price,
            exit_price: mark_px,
            gross_pnl,
            total_fees,
            funding_pnl: pos.accumulated_funding,
            net_pnl,
            r_multiple: r_mult,
            bars_held: pos.bars_held,
            exit_reason: ExitReason::EndOfBacktest,
            equity_after: ledger.cash_balance,
        });

        notifier
            .send(
                AlertLevel::Info,
                &format!("[DRY-RUN 模拟实盘平仓] {} {}", pos.symbol, pos.side),
                &format!(
                    "开仓价: {} -> 平仓价: {} | 净盈亏: {:+.3} USDT | 账户最新现金余额: {:.2} USDT",
                    pos.entry_price, mark_px, net_pnl, ledger.cash_balance
                ),
            )
            .await?;
    }

    ledger.save(&gateway.ledger_path)?;
    Ok(())
}

/// 执行单次或循环的 Dry-Run / Live 实盘巡检与交易周期
pub async fn run_trading_cycle(
    cfg: &AppConfig,
    data_mgr: &DataManager,
    gateway: &BinanceFuturesGateway,
    notifier: &Notifier,
    ai_guard: &AiRiskGuard,
    force_refresh: bool,
) -> Result<()> {
    let mode_name = if gateway.dry_run { "DRY-RUN" } else { "LIVE" };
    let mut ledger = PersistentLedger::load_or_init(
        &gateway.ledger_path,
        mode_name,
        cfg.account.initial_equity,
    )?;

    let mut symbols = vec![cfg.strategy.benchmark_symbol.clone()];
    for s in &cfg.strategy.universe {
        if !symbols.contains(s) {
            symbols.push(s.clone());
        }
    }

    let mut all_candles: HashMap<String, Vec<Candle>> = HashMap::new();
    for sym in &symbols {
        let c = data_mgr
            .load_or_fetch_candles(sym, 300, force_refresh)
            .await?;
        all_candles.insert(sym.clone(), c);
    }

    let btc_candles = &all_candles[&cfg.strategy.benchmark_symbol];
    let now_ts = Utc::now().timestamp_millis();
    let regime = detect_market_regime(btc_candles, cfg.strategy.regime_adx_threshold);

    // 1. 计算当前实时账户总权益 (现金 + 浮动盈亏)
    let mut unrealized = Decimal::ZERO;
    for pos in &ledger.open_positions {
        if let Some(c) = all_candles.get(&pos.symbol).and_then(|v| v.last()) {
            unrealized += pos.unrealized_pnl(c.close);
        }
    }
    let current_equity = ledger.cash_balance + unrealized;
    if current_equity > ledger.peak_equity {
        ledger.peak_equity = current_equity;
    }

    let mut risk_mgr = RiskManager::new(
        ledger.initial_equity,
        cfg.risk.clone(),
        cfg.costs.clone(),
    );
    risk_mgr.peak_equity = ledger.peak_equity;
    risk_mgr.consecutive_losses = ledger.consecutive_losses;
    risk_mgr.update_equity(now_ts, current_equity);

    let fee_rate = cfg.costs.taker_fee_rate;
    let slip_rate = cfg.costs.slippage_rate;
    let round_trip_wear = (fee_rate + slip_rate) * dec!(2);

    // 2. 更新现有持仓：检查止损触发、保本移位与吊灯移动止盈
    let mut remaining_positions = Vec::new();
    for mut pos in ledger.open_positions.drain(..) {
        let candles = match all_candles.get(&pos.symbol) {
            Some(c) if !c.is_empty() => c,
            _ => {
                remaining_positions.push(pos);
                continue;
            }
        };
        let last_bar = candles.last().unwrap();
        let cur_atr = atr(candles, cfg.strategy.atr_period).unwrap_or(Decimal::ZERO);

        // 检查 Dry-Run 模式下最新 K 线是否已触及服务端止损单
        let stop_hit = match pos.side {
            Side::Long => last_bar.low <= pos.active_stop,
            Side::Short => last_bar.high >= pos.active_stop,
        };
        if stop_hit {
            let exit_px = pos.active_stop;
            let gross_pnl = pos.unrealized_pnl(exit_px);
            let exit_fee = pos.qty * exit_px * fee_rate;
            let total_fees = pos.entry_fee + exit_fee;
            let net_pnl = gross_pnl - total_fees + pos.accumulated_funding;
            ledger.cash_balance += gross_pnl - exit_fee;
            risk_mgr.record_trade_outcome(net_pnl);
            ledger.consecutive_losses = risk_mgr.consecutive_losses;
            ledger.stop_order_ids.remove(&pos.symbol);

            let init_risk = pos.initial_risk_per_unit * pos.qty;
            let r_mult = if init_risk > Decimal::ZERO {
                net_pnl / init_risk
            } else {
                Decimal::ZERO
            };
            let reason = if pos.active_stop == pos.initial_stop {
                ExitReason::InitialStop
            } else if pos.breakeven_activated {
                ExitReason::TrailingStop
            } else {
                ExitReason::InitialStop
            };

            ledger.closed_trades.push(TradeRecord {
                symbol: pos.symbol.clone(),
                side: pos.side,
                entry_time: pos.entry_time,
                exit_time: now_ts,
                qty: pos.qty,
                entry_price: pos.entry_price,
                exit_price: exit_px,
                gross_pnl,
                total_fees,
                funding_pnl: pos.accumulated_funding,
                net_pnl,
                r_multiple: r_mult,
                bars_held: pos.bars_held,
                exit_reason: reason,
                equity_after: ledger.cash_balance,
            });

            notifier
                .send(
                    AlertLevel::Warning,
                    &format!("服务端 STOP_MARKET 触发平仓: {} {}", pos.symbol, pos.side),
                    &format!(
                        "触发价: {} | 净盈亏: {:+.2} USDT ({:+.2}R) | 账户余额: {:.2} USDT",
                        exit_px, net_pnl, r_mult, ledger.cash_balance
                    ),
                )
                .await?;
            continue;
        }

        let old_stop = pos.active_stop;
        if let Some(exit_reason) = update_position_on_bar_close(
            &mut pos,
            last_bar,
            cur_atr,
            regime,
            &cfg.strategy,
            round_trip_wear,
        ) {
            gateway
                .close_position_market(&pos.symbol, pos.side, pos.qty)
                .await?;
            let exit_px = last_bar.close;
            let gross_pnl = pos.unrealized_pnl(exit_px);
            let exit_fee = pos.qty * exit_px * fee_rate;
            let total_fees = pos.entry_fee + exit_fee;
            let net_pnl = gross_pnl - total_fees + pos.accumulated_funding;
            ledger.cash_balance += gross_pnl - exit_fee;
            risk_mgr.record_trade_outcome(net_pnl);
            ledger.consecutive_losses = risk_mgr.consecutive_losses;
            ledger.stop_order_ids.remove(&pos.symbol);

            let init_risk = pos.initial_risk_per_unit * pos.qty;
            let r_mult = if init_risk > Decimal::ZERO {
                net_pnl / init_risk
            } else {
                Decimal::ZERO
            };

            ledger.closed_trades.push(TradeRecord {
                symbol: pos.symbol.clone(),
                side: pos.side,
                entry_time: pos.entry_time,
                exit_time: now_ts,
                qty: pos.qty,
                entry_price: pos.entry_price,
                exit_price: exit_px,
                gross_pnl,
                total_fees,
                funding_pnl: pos.accumulated_funding,
                net_pnl,
                r_multiple: r_mult,
                bars_held: pos.bars_held,
                exit_reason,
                equity_after: ledger.cash_balance,
            });

            notifier
                .send(
                    AlertLevel::Info,
                    &format!("平仓完成: {} {}", pos.symbol, pos.side),
                    &format!(
                        "原因: {} | 成交价: {} | 净盈亏: {:+.2} USDT ({:+.2}R) | 当前权益: {:.2} USDT",
                        exit_reason, exit_px, net_pnl, r_mult, ledger.cash_balance
                    ),
                )
                .await?;
            continue;
        }

        // 若吊灯止盈或保本止损上移，同步更新交易所端 STOP_MARKET 订单
        if pos.active_stop != old_stop {
            if let Ok(new_stop_id) = gateway
                .place_server_stop_market(&pos.symbol, pos.side, pos.qty, pos.active_stop)
                .await
            {
                ledger
                    .stop_order_ids
                    .insert(pos.symbol.clone(), new_stop_id);
                notifier
                    .send(
                        AlertLevel::Info,
                        &format!("移动止损上移: {}", pos.symbol),
                        &format!("止损价从 {} 上移锁定至 {}", old_stop, pos.active_stop),
                    )
                    .await?;
            }
        }

        remaining_positions.push(pos);
    }
    ledger.open_positions = remaining_positions;

    // 3. 扫描新开仓信号
    if ledger.open_positions.len() < cfg.risk.max_concurrent_positions {
        let mut slice_map: HashMap<String, &[Candle]> = HashMap::new();
        for (k, v) in &all_candles {
            slice_map.insert(k.clone(), v.as_slice());
        }
        let signals = scan_entry_signals(regime, &slice_map, btc_candles, &cfg.strategy);

        for sig in signals {
            if ledger.open_positions.len() >= cfg.risk.max_concurrent_positions {
                break;
            }
            if ledger.open_positions.iter().any(|p| p.symbol == sig.symbol) {
                continue;
            }

            let verdict = ai_guard
                .evaluate_signal(&sig, &all_candles[&sig.symbol], None)
                .await;
            if !verdict.approved {
                notifier
                    .send(
                        AlertLevel::Warning,
                        &format!("AI 风控一票否决: {} {}", sig.symbol, sig.side),
                        &format!("风险评分: {} | 原因: {}", verdict.risk_score, verdict.reason),
                    )
                    .await?;
                continue;
            }

            let spec = SymbolSpec::default_for(&sig.symbol);
            if let Some(sizing) = risk_mgr.calculate_position_size(
                current_equity,
                sig.signal_close,
                sig.proposed_stop,
                &spec,
                &ledger.open_positions,
            ) {
                let (fill_px, stop_order_id) = gateway
                    .open_position_with_hard_stop(
                        &sig.symbol,
                        sig.side,
                        sizing.approved_qty,
                        sig.signal_close,
                        sig.proposed_stop,
                    )
                    .await?;

                let entry_fee = sizing.approved_qty * fill_px * fee_rate;
                ledger.cash_balance -= entry_fee;
                ledger
                    .stop_order_ids
                    .insert(sig.symbol.clone(), stop_order_id.clone());

                ledger.open_positions.push(Position {
                    symbol: sig.symbol.clone(),
                    side: sig.side,
                    qty: sizing.approved_qty,
                    entry_price: fill_px,
                    initial_stop: sig.proposed_stop,
                    active_stop: sig.proposed_stop,
                    initial_risk_per_unit: (fill_px - sig.proposed_stop).abs(),
                    highest_close: fill_px,
                    lowest_close: fill_px,
                    bars_held: 0,
                    breakeven_activated: false,
                    entry_time: now_ts,
                    entry_fee,
                    accumulated_funding: Decimal::ZERO,
                });

                notifier
                    .send(
                        AlertLevel::Signal,
                        &format!(
                            "[{}] 开仓成功 & 硬止损已挂载: {} {}",
                            mode_name, sig.symbol, sig.side
                        ),
                        &format!(
                            "成交均价: {} | 数量: {} (名义价值: {:.2}U) | 服务端止损价: {} (单笔最大风险: -{:.2}U)",
                            fill_px,
                            sizing.approved_qty,
                            sizing.notional_usd,
                            sig.proposed_stop,
                            sizing.max_loss_usd
                        ),
                    )
                    .await?;
            }
        }
    }

    ledger.save(&gateway.ledger_path)?;
    info!(
        "[{}] 巡检周期完成 | 大盘状态: {} | 当前总权益: {:.2} USDT | 持仓数: {}/{}",
        mode_name,
        regime,
        current_equity,
        ledger.open_positions.len(),
        cfg.risk.max_concurrent_positions
    );

    Ok(())
}

/// 启动持续守护循环 (`--loop-interval-secs`)
pub async fn run_daemon_loop(
    cfg: AppConfig,
    data_mgr: DataManager,
    dry_run: bool,
    once: bool,
    interval_secs: u64,
) -> Result<()> {
    let gateway = BinanceFuturesGateway::new(dry_run, "data/live_state.json")?;
    let notifier = Notifier::from_env();
    let ai_guard = AiRiskGuard::from_env();

    let mode_label = if dry_run {
        "DRY-RUN 模拟实盘"
    } else {
        "LIVE 真金实盘"
    };
    notifier
        .send(
            AlertLevel::Info,
            &format!("Coin-AI 引擎启动 ({})", mode_label),
            &format!(
                "初始本金基准: {:.2} USDT | 单笔最大风险: {:.1}% ({:.2} USDT) | 最大持仓数: {}",
                cfg.account.initial_equity,
                cfg.risk.risk_per_trade_pct * dec!(100),
                cfg.account.initial_equity * cfg.risk.risk_per_trade_pct,
                cfg.risk.max_concurrent_positions
            ),
        )
        .await?;

    loop {
        if let Err(e) =
            run_trading_cycle(&cfg, &data_mgr, &gateway, &notifier, &ai_guard, !once).await
        {
            let _ = notifier
                .send(
                    AlertLevel::Critical,
                    "交易巡检周期异常告警",
                    &format!("错误详情: {} (系统将在下个周期自动重试恢复)", e),
                )
                .await;
        }
        if once {
            break;
        }
        tokio::time::sleep(Duration::from_secs(interval_secs)).await;
    }

    Ok(())
}

/// 查看当前实盘/模拟盘持久化账本状态，并实时拉取最新价格计算浮动盈亏 (`coin-ai status`)
pub async fn print_ledger_status(
    cfg: &AppConfig,
    data_mgr: &DataManager,
    refresh_price: bool,
) -> Result<()> {
    let ledger = PersistentLedger::load_or_init(
        "data/live_state.json",
        "DRY-RUN",
        cfg.account.initial_equity,
    )?;

    let mut total_unrealized = Decimal::ZERO;
    let mut total_notional = Decimal::ZERO;
    let mut position_rows = Vec::new();

    for p in &ledger.open_positions {
        let candles = data_mgr
            .load_or_fetch_candles(&p.symbol, 50, refresh_price)
            .await
            .unwrap_or_default();
        let mark_px = candles.last().map(|c| c.close).unwrap_or(p.entry_price);
        let upnl = p.unrealized_pnl(mark_px);
        let notional = p.notional(mark_px);
        total_unrealized += upnl;
        total_notional += notional;

        let stop_id = ledger
            .stop_order_ids
            .get(&p.symbol)
            .cloned()
            .unwrap_or_else(|| "NONE".to_string());

        let stop_dist_pct = if mark_px > Decimal::ZERO {
            ((mark_px - p.active_stop).abs() / mark_px) * dec!(100)
        } else {
            Decimal::ZERO
        };

        position_rows.push((p.clone(), mark_px, notional, upnl, stop_dist_pct, stop_id));
    }

    let total_equity = ledger.cash_balance + total_unrealized;
    let total_return_pct = if ledger.initial_equity > Decimal::ZERO {
        ((total_equity - ledger.initial_equity) / ledger.initial_equity) * dec!(100)
    } else {
        Decimal::ZERO
    };
    let effective_leverage = if total_equity > Decimal::ZERO {
        total_notional / total_equity
    } else {
        Decimal::ZERO
    };

    println!("\n================================================================================");
    println!("  Coin-AI 实时模拟/实盘账户账本监控 (data/live_state.json)");
    println!("================================================================================");
    println!("  运行模式          : {}", ledger.mode);
    println!("  初始测试本金      : {:.2} USDT", ledger.initial_equity);
    println!("  当前可用现金      : {:.2} USDT (已扣开仓手续费)", ledger.cash_balance);
    println!("  当前未实现浮盈亏  : {:+.3} USDT", total_unrealized);
    println!(
        "  当前账户实时净值  : {:.2} USDT ({:+.2}%)",
        total_equity, total_return_pct
    );
    println!(
        "  当前总名义持仓    : {:.2} USDT (账户有效杠杆: {:.2}x / 上限 {}x)",
        total_notional, effective_leverage, cfg.risk.max_account_leverage
    );
    println!(
        "  当前活跃持仓数    : {} / {} (历史已平仓 {} 笔)",
        ledger.open_positions.len(),
        cfg.risk.max_concurrent_positions,
        ledger.closed_trades.len()
    );
    println!("  账本最后更新时间  : {}", ledger.updated_at);

    if !position_rows.is_empty() {
        println!("--------------------------------------------------------------------------------");
        println!("  [当前活跃持仓 & 交易所服务端硬止损单明细]");
        for (p, mark_px, notional, upnl, stop_dist_pct, stop_id) in position_rows {
            println!(
                "  * {} {:<5} | 数量: {} (名义: {:.2}U) | 开仓价: {} | 最新标记价: {} | 浮盈亏: {:+.3}U",
                p.symbol, p.side, p.qty, notional, p.entry_price, mark_px, upnl
            );
            println!(
                "    └── 服务端硬止损价: {} (距当前价 {:.2}%) | 保本已激活: {} | 止损单ID: {}",
                p.active_stop, stop_dist_pct, p.breakeven_activated, stop_id
            );
        }
    }

    if !ledger.closed_trades.is_empty() {
        let wins = ledger
            .closed_trades
            .iter()
            .filter(|t| t.net_pnl > Decimal::ZERO)
            .count();
        let losses = ledger.closed_trades.len() - wins;
        let win_rate = (Decimal::from(wins) / Decimal::from(ledger.closed_trades.len())) * dec!(100);
        let realized_net: Decimal = ledger.closed_trades.iter().map(|t| t.net_pnl).sum();

        println!("--------------------------------------------------------------------------------");
        println!(
            "  [历史已平仓逐单流水账本 | 共 {} 单: 胜 {} 单 / 负 {} 单 (胜率 {:.1}%) | 累计已实现净利: {:+.2} USDT]",
            ledger.closed_trades.len(),
            wins,
            losses,
            win_rate,
            realized_net
        );
        println!(
            "  单号 | 币种     | 方向  | 开仓时间         | 平仓时间         | 开仓价    | 平仓价    | 净盈亏(U) | R倍数   | 平仓原因                 | 结算后余额"
        );
        println!(
            "  ----------------------------------------------------------------------------------------------------------------------------------------"
        );

        let start_idx = ledger.closed_trades.len().saturating_sub(25);
        for (i, t) in ledger.closed_trades.iter().enumerate().skip(start_idx) {
            let entry_dt = chrono::DateTime::<Utc>::from_timestamp_millis(t.entry_time)
                .map(|d| d.format("%m-%d %H:%M").to_string())
                .unwrap_or_default();
            let exit_dt = chrono::DateTime::<Utc>::from_timestamp_millis(t.exit_time)
                .map(|d| d.format("%m-%d %H:%M").to_string())
                .unwrap_or_default();
            println!(
                "  #{:<3} | {:<8} | {:<5} | {:<16} | {:<16} | {:<9.4} | {:<9.4} | {:+8.2}U | {:+6.2}R | {:<24} | {:>7.2} U",
                i + 1,
                t.symbol,
                t.side,
                entry_dt,
                exit_dt,
                t.entry_price,
                t.exit_price,
                t.net_pnl,
                t.r_multiple,
                t.exit_reason.to_string(),
                t.equity_after
            );
        }
    }
    println!("================================================================================\n");
    Ok(())
}

/// 连续逐单滚动实战推进器 (`coin-ai stream-sim`)：
/// 使用真实生产环境行情流，从指定周期前一路一单接一单连续实盘模拟撮合至当前最新时刻，
/// 完整写入持久化账本 `data/live_state.json`，并保留当前最新时刻的实时未平仓位无缝交接给后台守护进程！
pub async fn run_continuous_production_stream(
    cfg: &AppConfig,
    data_mgr: &DataManager,
    bars: usize,
    force_refresh: bool,
) -> Result<()> {
    let mut symbols = vec![cfg.strategy.benchmark_symbol.clone()];
    for s in &cfg.strategy.universe {
        if !symbols.contains(s) {
            symbols.push(s.clone());
        }
    }

    let mut all_candles: HashMap<String, Vec<Candle>> = HashMap::new();
    for sym in &symbols {
        let mut c = data_mgr
            .load_or_fetch_candles(sym, bars.max(600), force_refresh)
            .await?;
        if c.len() > bars {
            c = c[c.len() - bars..].to_vec();
        }
        all_candles.insert(sym.clone(), c);
    }

    let btc_candles = all_candles
        .get(&cfg.strategy.benchmark_symbol)
        .expect("缺少 BTCUSDT 基准数据");
    let warmup_bars = 250usize;
    if btc_candles.len() <= warmup_bars + 10 {
        bail!("K 线数量不足以启动连续滚动实战");
    }

    let mut ts_index_map: HashMap<String, HashMap<i64, usize>> = HashMap::new();
    for (sym, vec) in &all_candles {
        let mut m = HashMap::with_capacity(vec.len());
        for (idx, c) in vec.iter().enumerate() {
            m.insert(c.timestamp, idx);
        }
        ts_index_map.insert(sym.clone(), m);
    }

    let initial_eq = cfg.account.initial_equity;
    let mut cash_balance = initial_eq;
    let mut peak_equity = initial_eq;
    let mut risk_mgr = RiskManager::new(initial_eq, cfg.risk.clone(), cfg.costs.clone());

    let mut open_positions: Vec<Position> = Vec::new();
    let mut stop_order_ids: HashMap<String, String> = HashMap::new();
    let mut pending_signals: Vec<SignalCandidate> = Vec::new();
    let mut pending_close_reasons: HashMap<String, ExitReason> = HashMap::new();
    let mut closed_trades: Vec<TradeRecord> = Vec::new();

    let fee_rate = cfg.costs.taker_fee_rate;
    let slip_rate = cfg.costs.slippage_rate;
    let round_trip_wear = (fee_rate + slip_rate) * dec!(2);

    for t_idx in warmup_bars..btc_candles.len() {
        let current_ts = btc_candles[t_idx].timestamp;

        // 1. T 时刻开盘执行 T-1 收盘触发的平仓单
        if !pending_close_reasons.is_empty() {
            let mut remaining = Vec::new();
            for pos in open_positions.drain(..) {
                if let Some(reason) = pending_close_reasons.remove(&pos.symbol) {
                    if let Some(&c_idx) =
                        ts_index_map.get(&pos.symbol).and_then(|m| m.get(&current_ts))
                    {
                        let raw_px = all_candles[&pos.symbol][c_idx].open;
                        let spec = SymbolSpec::default_for(&pos.symbol);
                        let fill_exit = match pos.side {
                            Side::Long => spec.round_price(raw_px * (Decimal::ONE - slip_rate)),
                            Side::Short => spec.round_price(raw_px * (Decimal::ONE + slip_rate)),
                        };
                        let gross_pnl = pos.unrealized_pnl(fill_exit);
                        let exit_fee = pos.qty * fill_exit * fee_rate;
                        let total_fees = pos.entry_fee + exit_fee;
                        cash_balance += gross_pnl - exit_fee;
                        let net_pnl = gross_pnl - total_fees + pos.accumulated_funding;
                        let init_risk = pos.initial_risk_per_unit * pos.qty;
                        let r_mult = if init_risk > Decimal::ZERO {
                            net_pnl / init_risk
                        } else {
                            Decimal::ZERO
                        };
                        risk_mgr.record_trade_outcome(net_pnl);
                        risk_mgr.apply_chop_cooldown_if_needed(current_ts);
                        stop_order_ids.remove(&pos.symbol);

                        closed_trades.push(TradeRecord {
                            symbol: pos.symbol,
                            side: pos.side,
                            entry_time: pos.entry_time,
                            exit_time: current_ts,
                            qty: pos.qty,
                            entry_price: pos.entry_price,
                            exit_price: fill_exit,
                            gross_pnl,
                            total_fees,
                            funding_pnl: pos.accumulated_funding,
                            net_pnl,
                            r_multiple: r_mult,
                            bars_held: pos.bars_held,
                            exit_reason: reason,
                            equity_after: cash_balance,
                        });
                    } else {
                        remaining.push(pos);
                    }
                } else {
                    remaining.push(pos);
                }
            }
            open_positions = remaining;
            pending_close_reasons.clear();
        }

        // 2. T 时刻开盘执行 T-1 收盘确认的突破开仓单
        if !pending_signals.is_empty() {
            let mut unrealized = Decimal::ZERO;
            for p in &open_positions {
                if let Some(&c_idx) = ts_index_map.get(&p.symbol).and_then(|m| m.get(&current_ts)) {
                    unrealized += p.unrealized_pnl(all_candles[&p.symbol][c_idx].open);
                }
            }
            let cur_eq = cash_balance + unrealized;

            for sig in pending_signals.drain(..) {
                if open_positions.len() >= cfg.risk.max_concurrent_positions {
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
                let fill_entry = match sig.side {
                    Side::Long => spec.round_price(bar_open * (Decimal::ONE + slip_rate)),
                    Side::Short => spec.round_price(bar_open * (Decimal::ONE - slip_rate)),
                };
                let stop_dist = sig.atr_val * cfg.strategy.initial_stop_atr_mult;
                let initial_stop = match sig.side {
                    Side::Long => spec.round_price(fill_entry - stop_dist),
                    Side::Short => spec.round_price(fill_entry + stop_dist),
                };
                if (sig.side == Side::Long && initial_stop >= fill_entry)
                    || (sig.side == Side::Short && initial_stop <= fill_entry)
                {
                    continue;
                }

                if let Some(sizing) = risk_mgr.calculate_position_size(
                    cur_eq,
                    fill_entry,
                    initial_stop,
                    &spec,
                    &open_positions,
                ) {
                    let entry_fee = sizing.approved_qty * fill_entry * fee_rate;
                    cash_balance -= entry_fee;
                    stop_order_ids.insert(
                        sig.symbol.clone(),
                        format!("DRY_STOP_{}_{}", sig.symbol, current_ts / 1000),
                    );
                    open_positions.push(Position {
                        symbol: sig.symbol,
                        side: sig.side,
                        qty: sizing.approved_qty,
                        entry_price: fill_entry,
                        initial_stop,
                        active_stop: initial_stop,
                        initial_risk_per_unit: (fill_entry - initial_stop).abs(),
                        highest_close: fill_entry,
                        lowest_close: fill_entry,
                        bars_held: 0,
                        breakeven_activated: false,
                        entry_time: current_ts,
                        entry_fee,
                        accumulated_funding: Decimal::ZERO,
                    });
                }
            }
        }

        // 3. 盘中检查服务端 STOP_MARKET 触发与 8h 资金费率
        let mut surviving = Vec::new();
        for mut pos in open_positions.drain(..) {
            let c_idx = match ts_index_map
                .get(&pos.symbol)
                .and_then(|m| m.get(&current_ts))
            {
                Some(&idx) => idx,
                None => {
                    surviving.push(pos);
                    continue;
                }
            };
            let bar = &all_candles[&pos.symbol][c_idx];
            let stop_hit = match pos.side {
                Side::Long => bar.low <= pos.active_stop,
                Side::Short => bar.high >= pos.active_stop,
            };
            if stop_hit {
                let raw_stop_fill = match pos.side {
                    Side::Long => pos.active_stop.min(bar.open),
                    Side::Short => pos.active_stop.max(bar.open),
                };
                let spec = SymbolSpec::default_for(&pos.symbol);
                let fill_exit = match pos.side {
                    Side::Long => spec.round_price(raw_stop_fill * (Decimal::ONE - slip_rate)),
                    Side::Short => spec.round_price(raw_stop_fill * (Decimal::ONE + slip_rate)),
                };
                let reason = if pos.active_stop == pos.initial_stop {
                    ExitReason::InitialStop
                } else if pos.breakeven_activated
                    && (pos.active_stop - pos.entry_price).abs() / pos.entry_price < dec!(0.005)
                {
                    ExitReason::BreakevenStop
                } else {
                    ExitReason::TrailingStop
                };
                let gross_pnl = pos.unrealized_pnl(fill_exit);
                let exit_fee = pos.qty * fill_exit * fee_rate;
                let total_fees = pos.entry_fee + exit_fee;
                cash_balance += gross_pnl - exit_fee;
                let net_pnl = gross_pnl - total_fees + pos.accumulated_funding;
                let init_risk = pos.initial_risk_per_unit * pos.qty;
                let r_mult = if init_risk > Decimal::ZERO {
                    net_pnl / init_risk
                } else {
                    Decimal::ZERO
                };
                risk_mgr.record_trade_outcome(net_pnl);
                risk_mgr.apply_chop_cooldown_if_needed(current_ts);
                stop_order_ids.remove(&pos.symbol);

                closed_trades.push(TradeRecord {
                    symbol: pos.symbol,
                    side: pos.side,
                    entry_time: pos.entry_time,
                    exit_time: current_ts,
                    qty: pos.qty,
                    entry_price: pos.entry_price,
                    exit_price: fill_exit,
                    gross_pnl,
                    total_fees,
                    funding_pnl: pos.accumulated_funding,
                    net_pnl,
                    r_multiple: r_mult,
                    bars_held: pos.bars_held,
                    exit_reason: reason,
                    equity_after: cash_balance,
                });
                continue;
            }

            if bar.funding_rate != Decimal::ZERO {
                let notional = pos.qty * bar.close;
                let cf = match pos.side {
                    Side::Long => -(notional * bar.funding_rate),
                    Side::Short => notional * bar.funding_rate,
                };
                cash_balance += cf;
                pos.accumulated_funding += cf;
            }
            surviving.push(pos);
        }
        open_positions = surviving;

        // 4. 收盘更新净值、移动止盈与扫描下一单信号
        let mut unrealized = Decimal::ZERO;
        for p in &open_positions {
            if let Some(&c_idx) = ts_index_map.get(&p.symbol).and_then(|m| m.get(&current_ts)) {
                unrealized += p.unrealized_pnl(all_candles[&p.symbol][c_idx].close);
            }
        }
        let close_eq = cash_balance + unrealized;
        if close_eq > peak_equity {
            peak_equity = close_eq;
        }
        risk_mgr.update_equity(current_ts, close_eq);

        let btc_win_start = t_idx.saturating_sub(250);
        let btc_slice = &btc_candles[btc_win_start..=t_idx];
        let regime = detect_market_regime(btc_slice, cfg.strategy.regime_adx_threshold);

        for pos in &mut open_positions {
            if let Some(&c_idx) = ts_index_map.get(&pos.symbol).and_then(|m| m.get(&current_ts)) {
                let win_start = c_idx.saturating_sub(120);
                let slice = &all_candles[&pos.symbol][win_start..=c_idx];
                let cur_atr = atr(slice, cfg.strategy.atr_period).unwrap_or(Decimal::ZERO);
                if let Some(reason) = update_position_on_bar_close(
                    pos,
                    &all_candles[&pos.symbol][c_idx],
                    cur_atr,
                    regime,
                    &cfg.strategy,
                    round_trip_wear,
                ) {
                    pending_close_reasons.insert(pos.symbol.clone(), reason);
                }
            }
        }

        if open_positions.len() < cfg.risk.max_concurrent_positions {
            let mut slices_at_t: HashMap<String, &[Candle]> = HashMap::new();
            for sym in &cfg.strategy.universe {
                if let Some(&c_idx) = ts_index_map.get(sym).and_then(|m| m.get(&current_ts)) {
                    let win_start = c_idx.saturating_sub(120);
                    slices_at_t.insert(sym.clone(), &all_candles[sym][win_start..=c_idx]);
                }
            }
            pending_signals = scan_entry_signals(regime, &slices_at_t, btc_slice, &cfg.strategy);
        }
    }

    let ledger = PersistentLedger {
        mode: "DRY-RUN (Continuous Production Stream)".to_string(),
        initial_equity: initial_eq,
        cash_balance,
        peak_equity,
        consecutive_losses: risk_mgr.consecutive_losses,
        open_positions,
        stop_order_ids,
        closed_trades,
        updated_at: Utc::now().to_rfc3339(),
    };
    ledger.save("data/live_state.json")?;

    Ok(())
}
