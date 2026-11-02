//! 回放压测 Harness：真实秒级行情固化 + 延迟注入回放引擎。
//!
//! Phase 1: 把币安 spot 1s K 线固化到 `data/ticks_1s.db`（断点续传，绝不触碰生产库）。
//! Phase 3: 虚拟时钟回放引擎（见 ReplayStress 命令）。

use crate::config::AppConfig;
use crate::data::DataManager;
use crate::db::{ApiQuoteRow, DbPositionRow};
use crate::indicators::{atr, detect_market_regime};
use crate::paper_engine::{LiveExitDecision, SqlitePaperEngine};
use crate::risk::{CircuitBreakerState, RiskManager};
use crate::strategy::scan_entry_signals;
use crate::tick_db::{TickDatabase, TickRow};
use crate::types::{Candle, Side, SymbolSpec};
use anyhow::{Context, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Instant;
use tracing::{info, warn};

/// 1s K 线拉取源（vision 官方公开源优先，主站兜底；fapi 不支持 1s 故不在链路内）
const KLINES_BASES: [&str; 2] = [
    "https://data-api.binance.vision/api/v3/klines",
    "https://api1.binance.com/api/v3/klines",
];

/// 将近 `days` 天的 1s K 线固化到 ticks_1s.db。
/// 断点续传：库中已有数据时从 `max_ts + 1000` 继续拉到当前时刻，绝不重复请求。
pub async fn download_ticks_to_db(
    data_mgr: &DataManager,
    db: &mut TickDatabase,
    symbol: &str,
    days: u32,
    force: bool,
) -> Result<()> {
    let now_ms = Utc::now().timestamp_millis();
    let target_start = now_ms - (days as i64) * 86_400_000;

    if force {
        let deleted = db.delete_symbol(symbol)?;
        info!("[tick 固化] --force：已清除 {} 旧数据 {} 行", symbol, deleted);
    }

    let start_ts = match db.max_ts(symbol)? {
        Some(max_ts) if max_ts >= now_ms - 60_000 => {
            info!(
                "[tick 固化] {} 数据已是最新 (max_ts={}, 距今 <1 分钟)，无需续传",
                symbol, max_ts
            );
            print_coverage(db, symbol)?;
            return Ok(());
        }
        Some(max_ts) => {
            let resume = max_ts + 1000;
            if resume < target_start {
                warn!(
                    "[tick 固化] {} 本地数据起点过旧 (max_ts 距今超过 {} 天)，按完整窗口重拉",
                    symbol, days
                );
                target_start
            } else {
                info!(
                    "[tick 固化] {} 断点续传: 从 max_ts+1s = {} 继续拉到当前",
                    symbol, resume
                );
                resume
            }
        }
        None => target_start,
    };

    let expected_rows = (now_ms - start_ts) / 1000 + 1;
    info!(
        "[tick 固化] 开始拉取 {} 的 1s K 线: 起点约 {} 天前, 期望 {:.1} 万行, 每页 1000 根",
        symbol,
        days,
        expected_rows as f64 / 10_000.0
    );

    let started = Instant::now();
    let mut cursor = start_ts;
    let mut total = 0usize;
    let mut pages = 0usize;
    let mut empty_streak = 0usize;
    let mut base_idx = 0usize;

    loop {
        let page_res = data_mgr
            .fetch_klines_page(KLINES_BASES[base_idx], symbol, "1s", cursor, 1000)
            .await;

        let page = match page_res {
            Ok(p) if !p.is_empty() => p,
            Ok(_) => {
                // 空页：可能命中无成交秒洞或已到最新，右移游标；连续 3 页为空则结束
                empty_streak += 1;
                if empty_streak >= 3 || cursor > now_ms {
                    break;
                }
                cursor += 1000;
                continue;
            }
            Err(e) => {
                // 单源瞬时失败：切到备用源重试当前页；两源都挂则本页重试 3 次
                warn!(
                    "[tick 固化] {} 拉取失败 ({}), 切换备用源重试...",
                    KLINES_BASES[base_idx], e
                );
                base_idx = (base_idx + 1) % KLINES_BASES.len();
                empty_streak += 1;
                if empty_streak >= 6 {
                    anyhow::bail!("两个行情源连续失败, 已固化 {} 行, 可重跑命令续传", total);
                }
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
        };
        empty_streak = 0;

        let ticks: Vec<TickRow> = page
            .iter()
            .map(|k| TickRow {
                ts: k.ts,
                open: k.open,
                high: k.high,
                low: k.low,
                close: k.close,
                volume: k.volume,
            })
            .collect();
        let last_ts = ticks.last().unwrap().ts;
        db.upsert_ticks(symbol, &ticks)?;
        total += ticks.len();
        pages += 1;

        if pages % 50 == 0 {
            info!(
                "[tick 固化] {} 进度: {} 页 / {} 行 | 已拉到距今 {:.1} 小时处",
                symbol,
                pages,
                total,
                (now_ms - last_ts) as f64 / 3_600_000.0
            );
        }

        if (ticks.len() as i64) < 1000 || last_ts >= now_ms {
            break;
        }
        cursor = last_ts + 1000;
    }

    info!(
        "[tick 固化] {} 拉取完成: {} 页 / 新增 {} 行, 耗时 {:.1} 分钟",
        symbol,
        pages,
        total,
        started.elapsed().as_secs_f64() / 60.0
    );
    print_coverage(db, symbol)?;
    Ok(())
}

/// 打印并返回某币种的固化覆盖情况（行数 / 时间范围 / 缺口秒数）
pub fn print_coverage(db: &TickDatabase, symbol: &str) -> Result<crate::tick_db::CoverageStats> {
    let stats = db.coverage_stats(symbol)?;
    match (stats.min_ts, stats.max_ts) {
        (Some(lo), Some(hi)) => {
            let fmt = |ts: i64| {
                chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ts)
                    .map(|d| d.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                    .unwrap_or_else(|| "?".to_string())
            };
            info!(
                "[tick 固化] {} 覆盖情况: {} 行 | {} ~ {} | 缺口 {} 秒 ({:.2}%)",
                stats.symbol,
                stats.rows,
                fmt(lo),
                fmt(hi),
                stats.missing_seconds,
                stats.missing_seconds as f64 * 100.0 / stats.rows.max(1) as f64
            );
        }
        _ => warn!("[tick 固化] {} 库中暂无数据", symbol),
    }
    Ok(stats)
}

// ============================================================================
// Phase 3: 压力窗口扫描 + 虚拟时钟回放引擎 + 延迟注入
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowKind {
    /// 急跌（单小时最大跌幅）
    Crash,
    /// 急涨（单小时最大涨幅）
    Pump,
    /// 高波动（单小时最大振幅）
    HighVol,
    /// 低波动横盘
    Calm,
}

impl WindowKind {
    fn label(&self) -> &'static str {
        match self {
            WindowKind::Crash => "急跌",
            WindowKind::Pump => "急涨",
            WindowKind::HighVol => "高波动",
            WindowKind::Calm => "横盘",
        }
    }
    fn seed_side(&self) -> Side {
        match self {
            WindowKind::Crash => Side::Short,
            _ => Side::Long,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplayWindow {
    pub label: String,
    pub kind: WindowKind,
    pub event_ts: i64,
    pub start_ms: i64,
    pub end_ms: i64,
    /// 窗口内事件幅度（Crash: 跌幅%, Pump: 涨幅%, 其余: 振幅%）
    pub magnitude_pct: Decimal,
}

/// 扫描 tick 覆盖范围内的 1h K线，选出最具代表性的压力窗口（事件点 ± `half_hours` 小时）。
/// 窗口互相去重（重叠即合并保留排序更靠前者），全部裁剪到 tick 数据范围内。
pub fn scan_stress_windows(
    btc_1h: &[Candle],
    tick_min: i64,
    tick_max: i64,
    top_n: usize,
    half_hours: i64,
) -> Vec<ReplayWindow> {
    let hour_ms = 3_600_000i64;
    // 候选事件：完全落在 tick 覆盖内的 1h K线
    let candidates: Vec<&Candle> = btc_1h
        .iter()
        .filter(|c| c.timestamp >= tick_min && c.timestamp + hour_ms <= tick_max)
        .collect();
    if candidates.is_empty() {
        return Vec::new();
    }

    let pct = |a: Decimal, b: Decimal| -> Decimal { (a / b - Decimal::ONE) * dec!(100) };

    #[derive(Clone)]
    struct Rank {
        kind: WindowKind,
        ts: i64,
        score: Decimal, // 越大越极端
        magnitude: Decimal,
    }

    // 每根 K 线按四个维度独立打分：急跌 / 急涨 / 振幅 / （反向）横盘
    let mut crash_ranks: Vec<Rank> = Vec::new();
    let mut pump_ranks: Vec<Rank> = Vec::new();
    let mut vol_ranks: Vec<Rank> = Vec::new();
    let mut calm_ranks: Vec<Rank> = Vec::new();
    for c in &candidates {
        let down = (c.open - c.low) / c.open;
        let up = (c.high - c.open) / c.open;
        let amp = (c.high - c.low) / c.open;
        crash_ranks.push(Rank {
            kind: WindowKind::Crash,
            ts: c.timestamp,
            score: down,
            magnitude: -pct(c.low, c.open),
        });
        pump_ranks.push(Rank {
            kind: WindowKind::Pump,
            ts: c.timestamp,
            score: up,
            magnitude: pct(c.high, c.open),
        });
        vol_ranks.push(Rank {
            kind: WindowKind::HighVol,
            ts: c.timestamp,
            score: amp,
            magnitude: pct(c.high, c.low),
        });
        calm_ranks.push(Rank {
            kind: WindowKind::Calm,
            ts: c.timestamp,
            score: -amp, // 负号：振幅最小者得分最高
            magnitude: pct(c.high, c.low),
        });
    }
    crash_ranks.sort_by(|a, b| b.score.cmp(&a.score));
    pump_ranks.sort_by(|a, b| b.score.cmp(&a.score));
    vol_ranks.sort_by(|a, b| b.score.cmp(&a.score));
    calm_ranks.sort_by(|a, b| b.score.cmp(&a.score));

    // 每类保证一个代表（与已选事件相距 > 24h 的最优者），剩余名额按振幅极端度补齐
    let spacing = 24 * hour_ms;
    let mut picked: Vec<Rank> = Vec::new();
    let mut take_best = |ranks: &[Rank], picked: &mut Vec<Rank>| {
        for r in ranks {
            if picked.iter().all(|p| (p.ts - r.ts).abs() > spacing) {
                picked.push(r.clone());
                return;
            }
        }
    };
    take_best(&crash_ranks, &mut picked);
    take_best(&pump_ranks, &mut picked);
    take_best(&vol_ranks, &mut picked);
    take_best(&calm_ranks, &mut picked);
    let mut rest: Vec<Rank> = vol_ranks.clone();
    rest.sort_by(|a, b| b.score.cmp(&a.score));
    for r in rest {
        if picked.len() >= top_n {
            break;
        }
        if !picked.iter().any(|p| p.ts == r.ts) {
            picked.push(r);
        }
    }

    // 生成窗口并去重叠
    let half_ms = half_hours * hour_ms;
    let mut windows: Vec<ReplayWindow> = Vec::new();
    for r in picked.into_iter().take(top_n) {
        let start = (r.ts - half_ms).max(tick_min + hour_ms);
        let end = (r.ts + half_ms).min(tick_max);
        if end - start < 6 * hour_ms {
            continue;
        }
        // 与已选窗口重叠则跳过
        if windows
            .iter()
            .any(|w| start < w.end_ms && end > w.start_ms)
        {
            continue;
        }
        let label = format!(
            "{}_{}",
            match r.kind {
                WindowKind::Crash => "crash",
                WindowKind::Pump => "pump",
                WindowKind::HighVol => "vol",
                WindowKind::Calm => "calm",
            },
            chrono::DateTime::<chrono::Utc>::from_timestamp_millis(r.ts)
                .map(|d| d.format("%m%d_%H").to_string())
                .unwrap_or_else(|| r.ts.to_string())
        );
        windows.push(ReplayWindow {
            label,
            kind: r.kind,
            event_ts: r.ts,
            start_ms: start,
            end_ms: end,
            magnitude_pct: r.magnitude,
        });
    }
    windows
}

/// 一笔回放成交（含决策价 vs 落地价的漂移度量）
#[derive(Debug, Clone)]
pub struct FillRecord {
    pub ts: i64,
    pub action: &'static str, // OPEN / OPEN_ABANDONED / CLOSE
    pub side: String,
    pub qty: Decimal,
    /// 决策时刻（t）的盘口价 —— 引擎以为能拿到的价
    pub decision_px: Decimal,
    /// 落地时刻（t+Δ）的实际成交价（ABANDONED 时为被拒绝的市场价）
    pub fill_px: Decimal,
    /// 有符号漂移 (bps)：正值 = 延迟后成交更不利，负值 = 延迟后反而更有利
    pub drift_bps: Decimal,
    pub reason: String,
    pub net_pnl: Option<Decimal>,
}

/// 单窗口 × 单延迟档的回放结果
#[derive(Debug, Clone)]
pub struct WindowRunResult {
    pub latency_ms: i64,
    pub fills: Vec<FillRecord>,
    pub final_equity: Decimal,
    pub max_dd_pct: Decimal,
    pub open_count: usize,
    pub close_count: usize,
    /// 因护圈（落地价偏离决策价 > collar）被放弃的开仓笔数
    pub abandoned_count: usize,
    /// 因波动闸门（决策时最近 N 秒振幅超阈值）被拒绝的开仓笔数
    pub gated_count: usize,
    /// 黑天鹅探测器首次触发距窗口开始的秒数
    pub detector_first_fire_secs: Option<i64>,
}

fn dec_from_f64(v: f64) -> Decimal {
    Decimal::from_str(&format!("{:.8}", v)).unwrap_or(Decimal::ZERO)
}

fn fmt_ts(ts: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ts)
        .map(|d| d.format("%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| "?".to_string())
}

/// 用 tick 收盘价 ± 点差/2 重建决策时刻的盘口（与生产 bookTicker 同构）
fn build_quote(symbol: &str, tick: &TickRow, spread_bps: Decimal, now_ms: i64) -> ApiQuoteRow {
    let mid = dec_from_f64(tick.close);
    let half = mid * spread_bps / dec!(20000);
    ApiQuoteRow {
        symbol: symbol.to_string(),
        bid_price: (mid - half).round_dp(2),
        ask_price: (mid + half).round_dp(2),
        last_price: mid,
        spread_bps,
        source_api: "replay_ticks_1s".to_string(),
        fetched_at: chrono::DateTime::<chrono::Utc>::from_timestamp_millis(now_ms)
            .unwrap_or_default()
            .to_rfc3339(),
    }
}

/// t+Δ 落地时刻的成交 tick：优先取 >= t+Δ 的最近 tick（越界则取 <= 的最近一根兜底）
fn tick_at_with_latency(db: &TickDatabase, symbol: &str, ts: i64) -> Result<Option<TickRow>> {
    if let Some(t) = db.quote_at_or_after(symbol, ts)? {
        return Ok(Some(t));
    }
    db.quote_at_or_before(symbol, ts)
}

/// t+Δ 落地成交价：**优先毫秒级 aggTrades**（亚秒延迟测量的精度来源），
/// 无逐笔数据时回退 1s tick 收盘价。
fn fill_price_at_with_latency(db: &TickDatabase, symbol: &str, ts: i64) -> Result<Option<f64>> {
    let trades = db.agg_range(symbol, ts, ts + 10_000)?;
    if let Some(first) = trades.first() {
        return Ok(Some(first.price));
    }
    Ok(db.quote_at_or_after(symbol, ts)?.map(|t| t.close))
}

/// 为指定时间区间下载聚合逐笔成交（aggTrades，毫秒级），固化到 tick 库。
/// 仅用于压力窗口的事件小时深钻，量小（约每小时数万笔）。
pub async fn download_agg_trades_range(
    data_mgr: &DataManager,
    db: &mut TickDatabase,
    symbol: &str,
    t0: i64,
    t1: i64,
) -> Result<usize> {
    const BASES: [&str; 2] = [
        "https://data-api.binance.vision/api/v3/aggTrades",
        "https://api1.binance.com/api/v3/aggTrades",
    ];
    let mut total = 0usize;
    let mut cursor = t0;
    let mut base_idx = 0usize;
    let mut failures = 0usize;

    while cursor < t1 {
        let page = match data_mgr
            .fetch_agg_trades_page(BASES[base_idx], symbol, cursor, 1000)
            .await
        {
            Ok(p) => p,
            Err(e) => {
                failures += 1;
                if failures >= 6 {
                    anyhow::bail!("aggTrades 两个行情源连续失败: {}", e);
                }
                base_idx = (base_idx + 1) % BASES.len();
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
        };
        if page.is_empty() {
            break;
        }
        let rows: Vec<crate::tick_db::AggTradeRow> = page
            .iter()
            .map(|&(ts, price, qty)| crate::tick_db::AggTradeRow { ts, price, qty })
            .collect();
        let last_ts = rows.last().unwrap().ts;
        db.upsert_agg_trades(symbol, &rows)?;
        total += rows.len();
        if last_ts >= t1 || rows.len() < 1000 {
            break;
        }
        cursor = last_ts + 1;
    }
    Ok(total)
}

/// 计算有符号漂移 (bps)。各方向的「不利」定义：
/// LONG 开仓买贵 > 0 / LONG 平仓卖便宜 > 0 / SHORT 开仓卖便宜 > 0 / SHORT 平仓买贵 > 0
pub fn drift_bps(action: &str, side: &str, decision_px: Decimal, fill_px: Decimal) -> Decimal {
    let adverse = match (action, side) {
        ("OPEN", "LONG") => fill_px - decision_px,
        ("CLOSE", "LONG") => decision_px - fill_px,
        ("OPEN", "SHORT") => decision_px - fill_px,
        ("CLOSE", "SHORT") => fill_px - decision_px,
        _ => Decimal::ZERO,
    };
    if decision_px > Decimal::ZERO {
        adverse / decision_px * dec!(10000)
    } else {
        Decimal::ZERO
    }
}

/// 单窗口 × 单延迟档回放：虚拟时钟每 `poll_secs` 秒一拍推进，
/// 平仓/开仓决策走生产同源纯函数，成交在 t+Δ 的 tick 上按不利侧吃单。
/// `collar_bps` > 0 时开仓模拟 IOC 限价护圈：落地价偏离决策价超阈值 → 放弃成交。
/// `seed_at_event` = true 时种子仓等到事件时刻才入场（制造「决策时平静、落地时崩落」的护圈测试场景）。
#[allow(clippy::too_many_arguments)]
pub fn replay_window_with_latency(
    engine: &SqlitePaperEngine,
    cfg: &AppConfig,
    tick_db: &TickDatabase,
    btc_1h: &[Candle],
    window: &ReplayWindow,
    latency_ms: i64,
    poll_secs: i64,
    spread_bps: Decimal,
    seed_position: bool,
    seed_at_event: bool,
    collar_bps: Decimal,
) -> Result<WindowRunResult> {
    let symbol = cfg.strategy.benchmark_symbol.clone(); // 单币种回放：标的即基准
    let fee_rate = cfg.costs.taker_fee_rate;
    let hour_ms = 3_600_000i64;
    let poll_ms = poll_secs * 1000;

    let mut risk_mgr = RiskManager::new(
        cfg.account.initial_equity,
        cfg.risk.clone(),
        cfg.costs.clone(),
    );
    let mut cash = cfg.account.initial_equity;
    let mut peak_equity = cash;
    let mut max_dd = Decimal::ZERO;

    let mut positions: Vec<DbPositionRow> = Vec::new();
    let mut fills: Vec<FillRecord> = Vec::new();
    let mut last_close_ms: HashMap<String, i64> = HashMap::new();
    let mut baselines: HashMap<String, Decimal> = HashMap::new();
    let mut detector_first_fire: Option<i64> = None;
    let mut open_count = 0usize;
    let mut close_count = 0usize;
    let mut abandoned_count = 0usize;

    // 每小时刷新的策略状态
    let mut regime = crate::types::MarketRegime::Chop;
    let mut atr_map: HashMap<String, Decimal> = HashMap::new();
    let mut cached_entries: Vec<(String, Side, Decimal)> = Vec::new();
    let mut last_hour = -1i64;

    let asof_1h = |now: i64| -> Vec<Candle> {
        btc_1h
            .iter()
            .filter(|c| c.timestamp + hour_ms <= now)
            .cloned()
            .collect()
    };

    let mut now = window.start_ms;
    let mut cycle = 0usize;
    let mut seq = 0usize;
    let mut last_seed_ms = window.start_ms - 12 * hour_ms; // 使窗口起点立即满足补种条件

    while now <= window.end_ms {
        cycle += 1;
        let Some(tick) = tick_db.quote_at_or_before(&symbol, now)? else {
            now += poll_ms;
            continue;
        };
        let quote = build_quote(&symbol, &tick, spread_bps, now);
        let mut quotes_map = HashMap::new();
        quotes_map.insert(symbol.clone(), quote.clone());

        // 黑天鹅哨兵：基准价窗口种子 + 每 18 拍滚动（与生产 10s×18≈3min 一致）
        if baselines.is_empty() {
            baselines.insert(symbol.clone(), quote.last_price);
        } else if cycle % 18 == 0 {
            baselines.insert(symbol.clone(), quote.last_price);
        }
        if detector_first_fire.is_none()
            && engine
                .evaluate_black_swan_anomaly(&baselines, &quotes_map, None)
                .is_some()
        {
            detector_first_fire = Some((now - window.start_ms) / 1000);
        }
        let black_swan_active = detector_first_fire.is_some();

        // 整点刷新信号/状态（只用已收盘 K线，杜绝未来数据）
        let cur_hour = now / hour_ms;
        if cur_hour != last_hour {
            last_hour = cur_hour;
            let closed = asof_1h(now);
            if closed.len() >= 310 {
                let tail = &closed[closed.len() - 310..];
                regime = detect_market_regime(tail, cfg.strategy.regime_adx_threshold);
                if let Some(a) = atr(tail, cfg.strategy.atr_period) {
                    atr_map.insert(symbol.clone(), a);
                }
                let start = closed.len().saturating_sub(121);
                let mut slices: HashMap<String, &[Candle]> = HashMap::new();
                slices.insert(symbol.clone(), &closed[start..]);
                let mut signals = scan_entry_signals(regime, &slices, tail, &cfg.strategy);
                signals.sort_by(|a, b| b.score.cmp(&a.score));
                cached_entries = signals
                    .into_iter()
                    .map(|sig| (sig.symbol, sig.side, sig.atr_val))
                    .collect();
            } else {
                regime = crate::types::MarketRegime::Chop;
                cached_entries.clear();
            }
        }

        // 波动闸门输入：最近 N 秒价格振幅（只用过去 tick，无未来函数）
        let recent_move_bps = match tick_db.quote_at_or_before(
            &symbol,
            now - (cfg.strategy.recent_move_window_secs as i64) * 1000,
        )? {
            Some(base_tick) if base_tick.close > 0.0 => {
                let base = dec_from_f64(base_tick.close);
                if base > Decimal::ZERO {
                    let mv = ((quote.last_price - base) / base).abs() * dec!(10000);
                    let mut m = HashMap::new();
                    m.insert(symbol.clone(), mv);
                    m
                } else {
                    HashMap::new()
                }
            }
            _ => HashMap::new(),
        };

        // 种子持仓：保证出场成交样本量（与信号无关，报告注明）。
        // 种子同样走 check_new_entries 完整门控（点差/波动闸门/以损定仓）——
        // 与生产完全同源；seed_at_event=true 时等到事件时刻才入场（护圈测试场景）。
        let seed_due = (cycle == 1 || now - last_seed_ms >= 12 * hour_ms)
            && last_close_ms
                .get(&symbol)
                .map(|t| now - t >= 1_800_000)
                .unwrap_or(true)
            && (!seed_at_event || now >= window.event_ts);
        if seed_position && seed_due && positions.is_empty() {
            last_seed_ms = now;
            let side = window.kind.seed_side();
            let cur_atr = *atr_map.get(&symbol).unwrap_or(&dec!(100));
            let seed_entries = vec![(symbol.clone(), side, cur_atr)];
            let seed_decision = SqlitePaperEngine::check_new_entries(
                &seed_entries,
                &quotes_map,
                &positions,
                cash,
                &risk_mgr,
                &cfg.strategy,
                |_| false,
                &recent_move_bps,
            );
            if let Some(d) = seed_decision.into_iter().next() {
                let Some(fill_px_f) = fill_price_at_with_latency(tick_db, &symbol, now + latency_ms)?
                else {
                    now += poll_ms;
                    continue;
                };
                let fq_mid = dec_from_f64(fill_px_f);
                let half = fq_mid * spread_bps / dec!(20000);
                let actual_entry = if side == Side::Long {
                    (fq_mid + half).round_dp(2)
                } else {
                    (fq_mid - half).round_dp(2)
                };
                // 护圈检查（只拦不利方向）：模拟 IOC 限价单——有利偏离永远成交，不利超 collar 放弃
                let adverse_dev_bps = if d.fill_entry > Decimal::ZERO {
                    match d.side {
                        Side::Long => (actual_entry - d.fill_entry) / d.fill_entry * dec!(10000),
                        Side::Short => (d.fill_entry - actual_entry) / d.fill_entry * dec!(10000),
                    }
                } else {
                    Decimal::ZERO
                };
                if collar_bps > Decimal::ZERO && adverse_dev_bps > collar_bps {
                    abandoned_count += 1;
                    fills.push(FillRecord {
                        ts: now + latency_ms,
                        action: "OPEN_ABANDONED",
                        side: if side == Side::Long { "LONG" } else { "SHORT" }.to_string(),
                        qty: d.approved_qty,
                        decision_px: d.fill_entry,
                        fill_px: actual_entry,
                        drift_bps: drift_bps(
                            "OPEN",
                            if side == Side::Long { "LONG" } else { "SHORT" },
                            d.fill_entry,
                            actual_entry,
                        ),
                        reason: format!(
                            "护圈放弃 (不利偏离 {:+.1}bps > collar {:.1}bps)",
                            adverse_dev_bps, collar_bps
                        ),
                        net_pnl: None,
                    });
                } else {
                    seq += 1;
                    let entry_fee = d.approved_qty * actual_entry * fee_rate;
                    cash -= entry_fee;
                    open_count += 1;
                    positions.push(DbPositionRow {
                        symbol: symbol.clone(),
                        side: if side == Side::Long { "LONG" } else { "SHORT" }.to_string(),
                        qty: d.approved_qty,
                        entry_price: actual_entry,
                        entry_order_id: format!("REPLAY_SEED_{}", seq),
                        stop_order_id: format!("REPLAY_SEED_STOP_{}", seq),
                        initial_stop: d.initial_stop,
                        active_stop: d.initial_stop,
                        take_profit: d.take_profit,
                        highest_mark: actual_entry,
                        lowest_mark: actual_entry,
                        last_mark_price: actual_entry,
                        unrealized_pnl: Decimal::ZERO,
                        entry_fee,
                        bars_held: 0,
                        opened_at: quote.fetched_at.clone(),
                        updated_at: quote.fetched_at.clone(),
                    });
                    fills.push(FillRecord {
                        ts: now + latency_ms,
                        action: "OPEN",
                        side: if side == Side::Long { "LONG" } else { "SHORT" }.to_string(),
                        qty: d.approved_qty,
                        decision_px: d.fill_entry,
                        fill_px: actual_entry,
                        drift_bps: drift_bps(
                            "OPEN",
                            if side == Side::Long { "LONG" } else { "SHORT" },
                            d.fill_entry,
                            actual_entry,
                        ),
                        reason: "种子开仓 (SEED_ENTRY)".to_string(),
                        net_pnl: None,
                    });
                }
            }
        }

        // ---- 平仓决策（生产同源 check_position_exit）----
        let mut total_upnl = Decimal::ZERO;
        let mut surviving: Vec<DbPositionRow> = Vec::new();
        for p in positions.drain(..) {
            let atr_opt = atr_map.get(&p.symbol).copied();
            match SqlitePaperEngine::check_position_exit(
                p,
                &quote,
                regime,
                atr_opt,
                black_swan_active,
                now,
                &cfg.strategy,
            ) {
                LiveExitDecision::Hold(p2) => {
                    total_upnl += p2.unrealized_pnl;
                    surviving.push(p2);
                }
                LiveExitDecision::Exit {
                    p: p2,
                    mark_exit_px,
                    reason,
                    ..
                } => {
                    // 落地成交：t+Δ 的价格（优先毫秒级 aggTrades，回退 1s tick）按平仓方向吃单
                    let Some(fill_px_f) =
                        fill_price_at_with_latency(tick_db, &p2.symbol, now + latency_ms)?
                    else {
                        surviving.push(p2);
                        continue;
                    };
                    let fq_mid = dec_from_f64(fill_px_f);
                    let half = fq_mid * spread_bps / dec!(20000);
                    let actual_exit = if p2.side == "LONG" {
                        (fq_mid - half).round_dp(2)
                    } else {
                        (fq_mid + half).round_dp(2)
                    };
                    let action = "CLOSE";
                    let drift = drift_bps(action, &p2.side, mark_exit_px, actual_exit);
                    let gross = if p2.side == "LONG" {
                        (actual_exit - p2.entry_price) * p2.qty
                    } else {
                        (p2.entry_price - actual_exit) * p2.qty
                    };
                    let exit_fee = p2.qty * actual_exit * fee_rate;
                    let net = gross - p2.entry_fee - exit_fee;
                    cash += gross - exit_fee;
                    last_close_ms.insert(p2.symbol.clone(), now);
                    close_count += 1;
                    fills.push(FillRecord {
                        ts: now + latency_ms,
                        action,
                        side: p2.side.clone(),
                        qty: p2.qty,
                        decision_px: mark_exit_px,
                        fill_px: actual_exit,
                        drift_bps: drift,
                        reason: reason.to_string(),
                        net_pnl: Some(net),
                    });
                }
            }
        }
        positions = surviving;

        // ---- 开仓决策（生产同源 check_new_entries）----
        if !matches!(
            risk_mgr.state,
            CircuitBreakerState::Halted | CircuitBreakerState::DailyCooldown { .. }
        ) && positions.len() < cfg.risk.max_concurrent_positions
            && !cached_entries.is_empty()
        {
            let decisions = SqlitePaperEngine::check_new_entries(
                &cached_entries,
                &quotes_map,
                &positions,
                cash + total_upnl,
                &risk_mgr,
                &cfg.strategy,
                |sym| last_close_ms
                    .get(sym)
                    .map(|t| now - t < 1_800_000)
                    .unwrap_or(false),
                &recent_move_bps,
            );
            for d in decisions {
                let Some(fill_px_f) =
                    fill_price_at_with_latency(tick_db, &d.symbol, now + latency_ms)?
                else {
                    continue;
                };
                let fq_mid = dec_from_f64(fill_px_f);
                let half = fq_mid * spread_bps / dec!(20000);
                let actual_entry = if d.side == Side::Long {
                    (fq_mid + half).round_dp(2)
                } else {
                    (fq_mid - half).round_dp(2)
                };
                // 护圈检查（只拦不利方向）：模拟 IOC 限价单——有利偏离永远成交，不利超 collar 放弃
                let adverse_dev_bps = if d.fill_entry > Decimal::ZERO {
                    match d.side {
                        Side::Long => (actual_entry - d.fill_entry) / d.fill_entry * dec!(10000),
                        Side::Short => (d.fill_entry - actual_entry) / d.fill_entry * dec!(10000),
                    }
                } else {
                    Decimal::ZERO
                };
                if collar_bps > Decimal::ZERO && adverse_dev_bps > collar_bps {
                    abandoned_count += 1;
                    fills.push(FillRecord {
                        ts: now + latency_ms,
                        action: "OPEN_ABANDONED",
                        side: d.side.to_str().to_string(),
                        qty: d.approved_qty,
                        decision_px: d.fill_entry,
                        fill_px: actual_entry,
                        drift_bps: drift_bps("OPEN", d.side.to_str(), d.fill_entry, actual_entry),
                        reason: format!(
                            "护圈放弃 (不利偏离 {:+.1}bps > collar {:.1}bps)",
                            adverse_dev_bps, collar_bps
                        ),
                        net_pnl: None,
                    });
                    continue;
                }
                let action = "OPEN";
                let drift = drift_bps(action, &d.side.to_str(), d.fill_entry, actual_entry);
                let entry_fee = d.approved_qty * actual_entry * fee_rate;
                cash -= entry_fee;
                seq += 1;
                open_count += 1;
                let fill_ts_str = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(
                    now + latency_ms,
                )
                .unwrap_or_default()
                .to_rfc3339();
                positions.push(DbPositionRow {
                    symbol: d.symbol.clone(),
                    side: d.side.to_str().to_string(),
                    qty: d.approved_qty,
                    entry_price: actual_entry,
                    entry_order_id: format!("REPLAY_OPEN_{}", seq),
                    stop_order_id: format!("REPLAY_OPEN_STOP_{}", seq),
                    initial_stop: d.initial_stop,
                    active_stop: d.initial_stop,
                    take_profit: d.take_profit,
                    highest_mark: actual_entry,
                    lowest_mark: actual_entry,
                    last_mark_price: actual_entry,
                    unrealized_pnl: Decimal::ZERO,
                    entry_fee,
                    bars_held: 0,
                    opened_at: fill_ts_str.clone(),
                    updated_at: fill_ts_str,
                });
                fills.push(FillRecord {
                    ts: now + latency_ms,
                    action,
                    side: d.side.to_str().to_string(),
                    qty: d.approved_qty,
                    decision_px: d.fill_entry,
                    fill_px: actual_entry,
                    drift_bps: drift,
                    reason: "信号开仓 (SIGNAL_ENTRY)".to_string(),
                    net_pnl: None,
                });
            }
        }

        // 净值 / 回撤 / 三级熔断喂值
        let equity = cash + total_upnl;
        if equity > peak_equity {
            peak_equity = equity;
        }
        if peak_equity > Decimal::ZERO {
            let dd = (peak_equity - equity) / peak_equity * dec!(100);
            if dd > max_dd {
                max_dd = dd;
            }
        }
        risk_mgr.update_equity(now, equity);

        now += poll_ms;
    }

    // 窗口结束：剩余持仓按最后可得 tick 强平（同样吃 t+Δ 延迟价）
    if let Some(last_tick) = tick_db.quote_at_or_before(&symbol, window.end_ms)? {
        let fq = build_quote(&symbol, &last_tick, spread_bps, window.end_ms);
        for p in positions.drain(..) {
            let actual_exit = if p.side == "LONG" {
                fq.bid_price
            } else {
                fq.ask_price
            };
            let gross = if p.side == "LONG" {
                (actual_exit - p.entry_price) * p.qty
            } else {
                (p.entry_price - actual_exit) * p.qty
            };
            let exit_fee = p.qty * actual_exit * fee_rate;
            let net = gross - p.entry_fee - exit_fee;
            cash += gross - exit_fee;
            close_count += 1;
            fills.push(FillRecord {
                ts: window.end_ms,
                action: "CLOSE",
                side: p.side.clone(),
                qty: p.qty,
                decision_px: fq.last_price,
                fill_px: actual_exit,
                drift_bps: drift_bps("CLOSE", &p.side, fq.last_price, actual_exit),
                reason: "回放窗口结束强平 (END_OF_REPLAY)".to_string(),
                net_pnl: Some(net),
            });
        }
    }

    Ok(WindowRunResult {
        latency_ms,
        fills,
        final_equity: cash,
        max_dd_pct: max_dd,
        open_count,
        close_count,
        abandoned_count,
        gated_count: 0,
        detector_first_fire_secs: detector_first_fire,
    })
}

// ============================================================================
// Phase 4: 合成极端情景（数据中不存在的 -30% 级联崩盘 / 理想化震荡）
// ============================================================================

/// 确定性 xorshift64* PRNG（不加 rand 依赖，同 seed 严格同序列）
struct Prng(u64);
impl Prng {
    fn new(seed: u64) -> Self {
        Prng(seed.max(1))
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn next_gauss(&mut self) -> f64 {
        // Box-Muller
        let u1 = self.next_f64().max(1e-12);
        let u2 = self.next_f64();
        (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
    }
}

/// 合成情景产物：1h 历史 K线（预热用）+ 事件窗口的 1s tick
pub struct SyntheticScenario {
    pub candles_1h: Vec<Candle>,
    pub ticks: Vec<TickRow>,
    pub window: ReplayWindow,
}

/// 生成 "-30% 级联闪崩" 合成情景：
/// 1h 平静 → 30 分钟 -30% 级联（快到足以触发黑天鹅哨兵）→ 1h 高波动混乱 → 部分反弹。
/// 前置 320 根 1h 温和上行历史（供状态机/指标预热），价格锚定 BTC 量级 78000。
pub fn generate_cascade_scenario(seed: u64) -> SyntheticScenario {
    let mut rng = Prng::new(seed);
    let hour_ms = 3_600_000i64;
    let t0 = 1_800_000_000_000i64; // 固定虚拟起点（2027-01-01 附近，仅虚拟时钟用）
    let window_span = 4 * hour_ms; // 4h 事件窗口

    // ---- 1h 历史：320 根温和上行（日漂移 +0.5%，小时噪声 0.35%）----
    let hist_len = 320usize;
    let mut candles: Vec<Candle> = Vec::with_capacity(hist_len);
    let mut px = 70_000.0f64;
    for i in 0..hist_len {
        let ts = t0 - ((hist_len - i) as i64) * hour_ms;
        let open = px;
        px *= 1.0 + 0.005 / 24.0 + 0.0035 * rng.next_gauss();
        let close = px;
        let high = open.max(close) * (1.0 + 0.0015 * rng.next_f64());
        let low = open.min(close) * (1.0 - 0.0015 * rng.next_f64());
        candles.push(Candle {
            timestamp: ts,
            open: dec_from_f64(open),
            high: dec_from_f64(high),
            low: dec_from_f64(low),
            close: dec_from_f64(close),
            volume: dec!(1000),
            funding_rate: Decimal::ZERO,
        });
    }

    // ---- 事件窗口 1s tick：1h 平静 → 30min -30% → 1h 混乱 → 反弹 ----
    let total_secs = (window_span / 1000) as usize;
    let mut ticks: Vec<TickRow> = Vec::with_capacity(total_secs);
    let mut p = 78_000.0f64;
    for s in 0..total_secs {
        let ts = t0 + (s as i64) * 1000;
        let sec_of_win = s as f64;
        if sec_of_win < 3600.0 {
            // 平静段：小时波动率 0.3%
            p *= 1.0 + 0.003 / 60.0f64.sqrt() / 60.0f64.sqrt() * rng.next_gauss();
        } else if sec_of_win < 3600.0 + 1800.0 {
            // 级联段：30 分钟 -30%（几何递减，每秒约 -0.118%，含噪声放大）
            let drift = (0.70f64).powf(1.0 / 1800.0) - 1.0;
            p *= 1.0 + drift + 0.002 * rng.next_gauss().abs() * -1.0;
        } else if sec_of_win < 3600.0 + 3600.0 + 1800.0 {
            // 混乱段：小时波动率 8%，双边巨震
            p *= 1.0 + 0.08 / 60.0 / 60.0f64.sqrt() * rng.next_gauss();
        } else {
            // 反弹段：1 小时回升 +8%
            let drift = (1.08f64).powf(1.0 / (total_secs as f64 - sec_of_win).max(1.0)) - 1.0;
            p *= 1.0 + drift + 0.01 / 60.0 / 60.0f64.sqrt() * rng.next_gauss();
        }
        p = p.max(1000.0);
        ticks.push(TickRow {
            ts,
            open: p,
            high: p * 1.0002,
            low: p * 0.9998,
            close: p,
            volume: 1.0,
        });
    }

    let window = ReplayWindow {
        label: "synth_cascade".to_string(),
        kind: WindowKind::Crash,
        event_ts: t0 + 3600_000, // 级联开始时刻
        start_ms: t0,
        end_ms: t0 + window_span,
        magnitude_pct: dec!(-30),
    };
    SyntheticScenario {
        candles_1h: candles,
        ticks,
        window,
    }
}

/// 生成 "+30% 级联暴涨" 合成情景（cascade 的镜像）：
/// 1h 平静 → 30 分钟 +30% 轧空拉抬 → 1h 高波动混乱 → 回吐。
/// 用于测「多头决策时平静、落地时价格已火箭」的护圈场景（买入价跑飞的灾难对照）。
pub fn generate_pump_scenario(seed: u64) -> SyntheticScenario {
    let mut s = generate_cascade_scenario(seed);
    s.window.label = "synth_pump".into();
    s.window.kind = WindowKind::Pump;
    s.window.magnitude_pct = dec!(30);
    // 镜像：所有 tick 价格按 156000/78000 反射（涨跌互换，波动结构不变）
    for t in s.ticks.iter_mut() {
        let px = 156_000.0 - t.close;
        t.open = px;
        t.high = px * 1.0002;
        t.low = px * 0.9998;
        t.close = px;
    }
    for c in s.candles_1h.iter_mut() {
        let mirror = |v: Decimal| dec!(156000) - v;
        c.open = mirror(c.open);
        c.close = mirror(c.close);
        let hi = mirror(c.low); // 涨跌互换后高低点对调
        let lo = mirror(c.high);
        c.high = hi;
        c.low = lo;
    }
    s
}

/// 生成 "理想化震荡" 合成情景：4h 均值回归横盘（OU 过程，半衰期 ~40 分钟，小时波动率 1.2%）
pub fn generate_chop_scenario(seed: u64) -> SyntheticScenario {
    let mut rng = Prng::new(seed ^ 0x9E3779B9);
    let hour_ms = 3_600_000i64;
    let t0 = 1_800_000_000_000i64;
    let window_span = 4 * hour_ms;

    let hist_len = 320usize;
    let mut candles: Vec<Candle> = Vec::with_capacity(hist_len);
    let mut px = 78_000.0f64;
    for i in 0..hist_len {
        let ts = t0 - ((hist_len - i) as i64) * hour_ms;
        let open = px;
        px += 78_000.0 * 0.012 / 60.0f64.sqrt() * rng.next_gauss() - (px - 78_000.0) * 0.02;
        let close = px;
        let high = open.max(close) * (1.0 + 0.001 * rng.next_f64());
        let low = open.min(close) * (1.0 - 0.001 * rng.next_f64());
        candles.push(Candle {
            timestamp: ts,
            open: dec_from_f64(open),
            high: dec_from_f64(high),
            low: dec_from_f64(low),
            close: dec_from_f64(close),
            volume: dec!(800),
            funding_rate: Decimal::ZERO,
        });
    }

    let total_secs = (window_span / 1000) as usize;
    let mut ticks: Vec<TickRow> = Vec::with_capacity(total_secs);
    let mut p = 78_000.0f64;
    for s in 0..total_secs {
        let ts = t0 + (s as i64) * 1000;
        // OU: 每秒向锚回归 0.03%，噪声 σ≈1.2%/√3600
        p += 78_000.0 * 0.012 / 60.0 * rng.next_gauss() - (p - 78_000.0) * 0.0003;
        ticks.push(TickRow {
            ts,
            open: p,
            high: p * 1.0001,
            low: p * 0.9999,
            close: p,
            volume: 0.5,
        });
    }

    let window = ReplayWindow {
        label: "synth_chop".to_string(),
        kind: WindowKind::Calm,
        event_ts: t0 + hour_ms,
        start_ms: t0,
        end_ms: t0 + window_span,
        magnitude_pct: Decimal::ZERO,
    };
    SyntheticScenario {
        candles_1h: candles,
        ticks,
        window,
    }
}

/// 把合成情景的 tick 固化到独立临时库并回放。
/// `window` 可换成变体（如把级联窗口的 kind 改成 Pump → LONG 种子，用于测黑天鹅紧急清仓路径）。
/// `seed_at_event` = true：种子仓等到事件时刻入场（护圈 A/B 的核心场景）。
pub fn run_synthetic_replay(
    cfg: &AppConfig,
    scenario: &SyntheticScenario,
    window: &ReplayWindow,
    latencies_ms: &[i64],
    spread_bps: Decimal,
    poll_secs: i64,
    seed_at_event: bool,
    collar_bps: Decimal,
) -> Result<Vec<WindowRunResult>> {
    let tmp_dir = std::env::temp_dir().join("coin_ai_replay_synth");
    let _ = std::fs::remove_dir_all(&tmp_dir);
    std::fs::create_dir_all(&tmp_dir)?;
    let mut db = TickDatabase::open(tmp_dir.join("synth_ticks.db"))?;
    db.upsert_ticks(&cfg.strategy.benchmark_symbol, &scenario.ticks)?;

    let mut sim_cfg = cfg.clone();
    sim_cfg.strategy.universe = vec![cfg.strategy.benchmark_symbol.clone()];
    sim_cfg.account.initial_equity = (cfg.account.initial_equity.max(dec!(300))).round_dp(2);
    sim_cfg.validate()?;

    let engine = SqlitePaperEngine::new(sim_cfg.clone(), "data/replay_sim.db")?;
    let mut out = Vec::new();
    for &lat in latencies_ms {
        let r = replay_window_with_latency(
            &engine,
            &sim_cfg,
            &db,
            &scenario.candles_1h,
            window,
            lat,
            poll_secs,
            spread_bps,
            true,
            seed_at_event,
            collar_bps,
        )?;
        out.push(r);
    }
    Ok(out)
}

impl Side {
    fn to_str(&self) -> &'static str {
        match self {
            Side::Long => "LONG",
            Side::Short => "SHORT",
        }
    }
}

/// 漂移统计（bps，有符号 + 绝对值口径）
struct DriftStats {
    mean_bps: Decimal,
    p50_bps: Decimal,
    p95_bps: Decimal,
    worst_bps: Decimal,
    mean_abs_bps: Decimal,
    n: usize,
}

fn drift_stats(fills: &[FillRecord]) -> DriftStats {
    let mut vals: Vec<Decimal> = fills.iter().map(|f| f.drift_bps).collect();
    vals.sort();
    let n = vals.len();
    if n == 0 {
        return DriftStats {
            mean_bps: Decimal::ZERO,
            p50_bps: Decimal::ZERO,
            p95_bps: Decimal::ZERO,
            worst_bps: Decimal::ZERO,
            mean_abs_bps: Decimal::ZERO,
            n: 0,
        };
    }
    let sum: Decimal = vals.iter().sum();
    let pick = |q: Decimal| -> Decimal {
        let idx = ((q * Decimal::from(n)).round() - Decimal::ONE).max(Decimal::ZERO);
        let idx = idx.to_usize().unwrap_or(0).min(n - 1);
        vals[idx]
    };
    DriftStats {
        mean_bps: sum / Decimal::from(n),
        p50_bps: pick(dec!(0.5)),
        p95_bps: pick(dec!(0.95)),
        worst_bps: *vals.last().unwrap(),
        mean_abs_bps: vals.iter().map(|v| v.abs()).sum::<Decimal>() / Decimal::from(n),
        n,
    }
}

/// 真实数据漂移校准表：全量 1s tick 上测 |close(t+Δ)/close(t) - 1| 分布（直接回答“Δ秒后价格走了多少”）
pub fn calibrate_drift_from_ticks(
    tick_db: &TickDatabase,
    symbol: &str,
    deltas_secs: &[i64],
) -> Result<Vec<(i64, Decimal, Decimal)>> {
    let (min_ts, max_ts) = match (tick_db.min_ts(symbol)?, tick_db.max_ts(symbol)?) {
        (Some(a), Some(b)) => (a, b),
        _ => return Ok(Vec::new()),
    };
    let all = tick_db.ticks_range(symbol, min_ts, max_ts)?;
    if all.len() < 100 {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for delta in deltas_secs {
        let step = *delta as usize;
        if all.len() <= step {
            continue;
        }
        let mut changes: Vec<f64> = Vec::with_capacity(all.len() - step);
        for i in 0..all.len() - step {
            let a = all[i].close;
            let b = all[i + step].close;
            if a > 0.0 {
                changes.push((b / a - 1.0) * 10_000.0); // bps
            }
        }
        changes.sort_by(|a, b| a.abs().partial_cmp(&b.abs()).unwrap());
        let n = changes.len();
        if n == 0 {
            continue;
        }
        let mean: f64 = changes.iter().sum::<f64>() / n as f64;
        let p95_abs = changes[((n as f64 * 0.95) as usize).min(n - 1)].abs();
        out.push((*delta, dec_from_f64(mean), dec_from_f64(p95_abs)));
    }
    Ok(out)
}

/// 回放压测主入口：扫描压力窗口 → 全窗口 × 全延迟档回放 →（可选）合成极端情景 → 控制台汇总 + Markdown 报告
pub async fn run_replay_stress(
    cfg: &AppConfig,
    data_mgr: &DataManager,
    tick_db_path: &str,
    top_windows: usize,
    latencies_ms: &[i64],
    spread_bps: Decimal,
    poll_secs: i64,
    report_path: &str,
    synthetic: bool,
    collar_bps: Decimal,
) -> Result<()> {
    let tick_db = TickDatabase::open(tick_db_path)?;
    let symbol = cfg.strategy.benchmark_symbol.clone();
    let stats = print_coverage(&tick_db, &symbol)?;
    let (Some(tick_min), Some(tick_max)) = (stats.min_ts, stats.max_ts) else {
        anyhow::bail!(
            "tick 库中无 {} 数据，请先运行: coin-ai download-ticks",
            symbol
        );
    };

    let btc_1h = data_mgr.load_or_fetch_candles(&symbol, 3000, false).await?;
    let windows = scan_stress_windows(&btc_1h, tick_min, tick_max, top_windows, 24);
    if windows.is_empty() {
        anyhow::bail!("tick 覆盖范围内未找到可用压力窗口");
    }
    info!(
        "[回放压测] 选出 {} 个压力窗口: {:?}",
        windows.len(),
        windows.iter().map(|w| w.label.clone()).collect::<Vec<_>>()
    );

    // 事件小时 ±1h 拉取毫秒级 aggTrades（亚秒延迟测量的精度来源；已固化则跳过）
    {
        let mut tick_db_mut = TickDatabase::open(tick_db_path)?;
        for w in &windows {
            let (t0, t1) = (w.event_ts - 3_600_000, w.event_ts + 3_600_000);
            let existing = tick_db_mut.agg_range(&symbol, t0, t1)?.len();
            if existing > 0 {
                info!(
                    "[回放压测] {} 已有 aggTrades {} 笔，跳过下载",
                    w.label, existing
                );
                continue;
            }
            let n = download_agg_trades_range(data_mgr, &mut tick_db_mut, &symbol, t0, t1)
                .await
                .unwrap_or_else(|e| {
                    warn!(
                        "[回放压测] {} aggTrades 下载失败 ({})，回退 1s 精度",
                        w.label, e
                    );
                    0
                });
            if n > 0 {
                info!(
                    "[回放压测] {} 事件小时 aggTrades 已固化 {} 笔 (毫秒级)",
                    w.label, n
                );
            }
        }
    }
    let tick_db = TickDatabase::open(tick_db_path)?;

    // 单币种回放：交易标的 = 基准币种（截面动量退化为量能分量，报告注明）。
    // 回放账户按 300U 计：BTCUSDT 交易所最小名义 100U，100U 账户的 65% 仓位上限(65U)永远无法开仓；
    // 滑点按 bps 口径呈现，与本金规模无关。
    let mut sim_cfg = cfg.clone();
    sim_cfg.strategy.universe = vec![symbol.clone()];
    sim_cfg.account.initial_equity = (cfg.account.initial_equity.max(dec!(300))).round_dp(2);
    sim_cfg.validate()?;

    let engine = SqlitePaperEngine::new(sim_cfg.clone(), "data/replay_sim.db")
        .context("构造回放引擎失败")?;

    let started = Instant::now();
    struct Row {
        window: String,
        kind: &'static str,
        latency_ms: i64,
        equity: Decimal,
        dd: Decimal,
        open_count: usize,
        close_count: usize,
        abandoned: usize,
        s: DriftStats,
        detector: Option<i64>,
    }
    let mut rows: Vec<Row> = Vec::new();

    for w in &windows {
        for &lat in latencies_ms {
            let r = replay_window_with_latency(
                &engine,
                &sim_cfg,
                &tick_db,
                &btc_1h,
                w,
                lat,
                poll_secs,
                spread_bps,
                true,
                false,
                collar_bps,
            )?;
            let s = drift_stats(&r.fills);
            info!(
                "[回放压测] {} | Δ={}ms | 成交 {} 笔 (开{}平{} 弃{}) | 净值 {:.2}U | 漂移 均值{:+.2}bps / p95 {:.2}bps / 最差{:.2}bps",
                w.label,
                lat,
                s.n,
                r.open_count,
                r.close_count,
                r.abandoned_count,
                r.final_equity,
                s.mean_bps,
                s.p95_bps,
                s.worst_bps
            );
            rows.push(Row {
                window: w.label.clone(),
                kind: w.kind.label(),
                latency_ms: lat,
                equity: r.final_equity,
                dd: r.max_dd_pct,
                open_count: r.open_count,
                close_count: r.close_count,
                abandoned: r.abandoned_count,
                s,
                detector: r.detector_first_fire_secs,
            });
        }
    }

    // 真实漂移校准表（全量 tick，多空双向绝对值口径）
    let calibration = calibrate_drift_from_ticks(&tick_db, &symbol, &[1, 5, 60, 300])?;

    // ---- 合成极端情景（可选）：真实数据中不存在的 -30% 级联 / +30% 轧空 / 理想化震荡 ----
    if synthetic {
        let cascade = generate_cascade_scenario(42);
        let chop = generate_chop_scenario(42);
        let pump = generate_pump_scenario(42);
        // 级联窗口两个变体：SHORT 种子（顺势吃肉）+ LONG 种子（黑天鹅紧急清仓）
        let mut long_seed_window = cascade.window.clone();
        long_seed_window.kind = WindowKind::Pump; // Pump → LONG 种子
        long_seed_window.label = "synth_cascade_long".into();
        // 护圈 A/B：LONG 种子在级联开始时刻入场，对比「无护圈 vs 有护圈(collar)」
        let mut long_seed_collar = long_seed_window.clone();
        long_seed_collar.label = "synth_long_collar".into();
        // 暴涨镜像：多头决策时平静、落地时价格已火箭 —— 「下一秒翻倍」场景
        let mut pump_collar = pump.window.clone();
        pump_collar.label = "synth_pump_collar".into();
        let variants: Vec<(&str, &SyntheticScenario, ReplayWindow, bool, Decimal)> = vec![
            (
                "synth_cascade_short",
                &cascade,
                cascade.window.clone(),
                true,
                collar_bps,
            ),
            ("synth_cascade_long", &cascade, long_seed_window, true, Decimal::ZERO),
            (
                "synth_long_collar",
                &cascade,
                long_seed_collar,
                true,
                collar_bps,
            ),
            ("synth_pump_long", &pump, pump.window.clone(), true, Decimal::ZERO),
            ("synth_pump_collar", &pump, pump_collar, true, collar_bps),
            ("synth_chop", &chop, chop.window.clone(), false, collar_bps),
        ];
        for (label, scenario, window, seed_at_event, run_collar) in variants {
            let runs = run_synthetic_replay(
                cfg,
                scenario,
                &window,
                latencies_ms,
                spread_bps,
                poll_secs,
                seed_at_event,
                run_collar,
            )?;
            for r in runs {
                let s = drift_stats(&r.fills);
                let evac: Vec<&FillRecord> = r
                    .fills
                    .iter()
                    .filter(|f| f.reason.contains("BLACK_SWAN"))
                    .collect();
                info!(
                    "[合成情景] {} | Δ={}ms | 成交 {} 笔 (弃{}) | 净值 {:.2}U | 漂移均值 {:+.2}bps / 最差 {:.2}bps | 黑天鹅清仓 {} 笔 | 探测器 {}",
                    label,
                    r.latency_ms,
                    s.n,
                    r.abandoned_count,
                    r.final_equity,
                    s.mean_bps,
                    s.worst_bps,
                    evac.len(),
                    match r.detector_first_fire_secs { Some(x) => format!("+{}s", x), None => "-".into() }
                );
                rows.push(Row {
                    window: label.to_string(),
                    kind: "合成",
                    latency_ms: r.latency_ms,
                    equity: r.final_equity,
                    dd: r.max_dd_pct,
                    open_count: r.open_count,
                    close_count: r.close_count,
                    abandoned: r.abandoned_count,
                    s,
                    detector: r.detector_first_fire_secs,
                });
            }
        }
    }

    // ---- 控制台汇总 ----
    println!("\n====================================================================================================================");
    println!(
        "  Coin-AI 回放压测报告 | 标的: {} | 窗口: {} 个 × 延迟档: {} | 数据: 1s tick ({} 行) | 耗时 {:.1}s",
        symbol,
        windows.len(),
        latencies_ms.len(),
        stats.rows,
        started.elapsed().as_secs_f64()
    );
    println!("====================================================================================================================");
    println!(
        "  窗口            | 类型   | 延迟(ms) | 成交数 | 弃单 | 净值(U)   | 最大回撤 | 漂移均值(bps) | p95 | 最差 | 探测器触发(s)"
    );
    println!("  ------------------------------------------------------------------------------------------------------------------");
    for r in &rows {
        println!(
            "  {:<19} | {:<6} | {:>7} | {:>6} | {:>4} | {:>8.2} | {:>6.2}% | {:+10.2} | {:>8.2} | {:>10.2} | {}",
            r.window,
            r.kind,
            r.latency_ms,
            r.s.n,
            r.abandoned,
            r.equity,
            r.dd,
            r.s.mean_bps,
            r.s.p95_bps,
            r.s.worst_bps,
            match r.detector {
                Some(s) => format!("+{}s", s),
                None => "-".to_string(),
            }
        );
    }

    println!("  ------------------------------------------------------------------");
    println!("  [真实 1s 漂移校准表 | {} 全量 tick | 有符号均值 / |p95|, 单位 bps]", symbol);
    for (delta, mean, p95) in &calibration {
        println!("    Δ={:>4}s : 均值 {:+.3} bps | |p95| {:.2} bps", delta, mean, p95);
    }
    println!("====================================================================================================================\n");

    // ---- Markdown 报告 ----
    let mut md = String::new();
    md.push_str("# 回放压测报告：真实秒级行情 + 订单落地延迟注入\n\n");
    md.push_str(&format!(
        "- 标的: `{}` (单币种回放：交易标的即基准币种，截面动量分量退化为量能项)\n",
        symbol
    ));
    md.push_str(&format!(
        "- 回放账户本金: {:.0}U (为满足 BTCUSDT 交易所最小名义 100U 上调至 >=300U；滑点按 bps 呈现与本金无关)\n",
        sim_cfg.account.initial_equity
    ));
    md.push_str(&format!(
        "- 数据: `{}` 1s tick 共 {} 行, 覆盖 {} ~ {}, 缺口 {} 秒\n",
        tick_db_path,
        stats.rows,
        fmt_ts(tick_min),
        fmt_ts(tick_max),
        stats.missing_seconds
    ));
    md.push_str(&format!(
        "- 成交模型: 决策在 t (每 {}s 一拍, 点差 {}bps 重建盘口), 落地成交在 t+Δ 的 tick 不利侧吃单\n",
        poll_secs, spread_bps
    ));
    md.push_str(&format!(
        "- 双层防线: 波动闸门 max_recent_move_bps={} (最近 {}s 振幅超限放弃) + 护圈 collar={}bps (落地偏离超限模拟 IOC 限价放弃, 0=关闭)\n",
        cfg.strategy.max_recent_move_bps, cfg.strategy.recent_move_window_secs, collar_bps
    ));
    md.push_str("- 决策代码: 与生产循环 100% 同源 (`check_position_exit` / `check_new_entries` / `scan_entry_signals` / `evaluate_black_swan_anomaly`)\n");
    md.push_str("- 每窗口起点附带 1 笔种子持仓（保证出场样本，与信号无关）\n\n");

    for w in &windows {
        md.push_str(&format!(
            "## 窗口: {} ({}, 事件幅度 {:+.2}%, {} ~ {})\n\n",
            w.label,
            w.kind.label(),
            w.magnitude_pct,
            fmt_ts(w.start_ms),
            fmt_ts(w.end_ms)
        ));
        md.push_str("| 延迟(ms) | 成交数 | 开/平 | 净值(U) | 最大回撤 | 漂移均值(bps) | p50 | p95 | 最差 | 探测器触发 |\n|---|---|---|---|---|---|---|---|---|---|\n");
        for r in rows.iter().filter(|r| r.window == w.label) {
            md.push_str(&format!(
                "| {} | {} | {}/{} | {:.2} | {:.2}% | {:+.2} | {:.2} | {:.2} | {:.2} | {} |\n",
                r.latency_ms,
                r.s.n,
                r.open_count,
                r.close_count,
                r.equity,
                r.dd,
                r.s.mean_bps,
                r.s.p50_bps,
                r.s.p95_bps,
                r.s.worst_bps,
                match r.detector {
                    Some(s) => format!("+{}s", s),
                    None => "-".to_string(),
                }
            ));
        }
        md.push('\n');
    }

    md.push_str("## 真实 1s 漂移校准表（全量 tick）\n\n");
    md.push_str("| Δ | 有符号均值 (bps) | \\|p95\\| (bps) |\n|---|---|---|\n");
    for (delta, mean, p95) in &calibration {
        md.push_str(&format!("| {}s | {:+.3} | {:.2} |\n", delta, mean, p95));
    }

    if synthetic {
        md.push_str("\n## 合成极端情景（真实数据中不存在）\n\n");
        md.push_str("| 情景 | 延迟(ms) | 成交数 | 开/平/弃 | 净值(U) | 漂移均值(bps) | p95 | 最差 | 探测器触发 |\n|---|---|---|---|---|---|---|---|---|\n");
        for r in rows.iter().filter(|r| r.window.starts_with("synth")) {
            md.push_str(&format!(
                "| {} | {} | {} | {}/{}/{} | {:.2} | {:+.2} | {:.2} | {:.2} | {} |\n",
                r.window,
                r.latency_ms,
                r.s.n,
                r.open_count,
                r.close_count,
                r.abandoned,
                r.equity,
                r.s.mean_bps,
                r.s.p95_bps,
                r.s.worst_bps,
                match r.detector {
                    Some(s) => format!("+{}s", s),
                    None => "-".to_string(),
                }
            ));
        }
        md.push_str("\n- `synth_cascade_short`: -30%/30min 级联 + 空头种子（危机 alpha 路径）\n\
                     - `synth_cascade_long`: 同级联 + 多头种子于级联开始时刻入场, **无护圈**（灾难对照组；注意下跌中买入落地价反而有利）\n\
                     - `synth_long_collar`: 同上但**开护圈**（只拦不利方向；验证有利成交不被错杀）\n\
                     - `synth_pump_long` / `synth_pump_collar`: **+30% 轧空镜像** + 多头追入——决策时平静、落地时价格已火箭，护圈 A/B 的核心场景\n\
                     - `synth_chop`: 理想化 OU 震荡（时间止损磨损路径）\n\n");
    }
    md.push_str(
        "\n## 解读口径\n\n\
        - **漂移为有符号**：正值 = 延迟后成交更不利（买贵/卖便宜），负值 = 反而更有利；均值显著为正说明存在系统性逆向选择成本。\n\
        - BTC 为全市场最深流动性标的，**山寨币同延迟的漂移只会更大**，本报告结果是下界。\n\
        - 黑天鹅探测器在单币种回放下只覆盖 BTC 跌幅腿与点差腿（广度腿需多币种盘口）。\n\
        - 种子持仓不计入信号质量评估，仅用于保证出场路径的延迟测量样本。\n",
    );

    std::fs::write(report_path, md)
        .with_context(|| format!("写入回放压测报告失败: {}", report_path))?;
    info!("[回放压测] Markdown 报告已生成: {}", report_path);

    Ok(())
}
