use anyhow::Result;
use clap::{Parser, Subcommand};
use coin_ai::backtest::BacktestEngine;
use coin_ai::config::{AppConfig, StrategyMode};
use coin_ai::data::DataManager;
use coin_ai::evolve::run_strategy_evolution;
use coin_ai::live::{
    close_live_simulated_positions, open_live_simulated_position, print_ledger_status,
    run_continuous_production_stream, run_daemon_loop, run_market_scan,
    verify_full_trading_lifecycle,
};
use coin_ai::paper_engine::SqlitePaperEngine;
use coin_ai::stats::QuantStatsEngine;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use tracing::info;

#[derive(Parser)]
#[command(
    name = "coin-ai",
    version = "0.1.0",
    about = "Coin-AI: Rust 金融级小资金 (100U/200U) 数字货币量化系统"
)]
struct Cli {
    /// 配置文件路径
    #[arg(short, long, default_value = "config/settings.toml")]
    config: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// 下载全市场候选币种 1h 历史 K 线数据到本地 data/ 目录
    Download {
        /// 每个币种下载的 1h K 线根数 (默认 4320 根 = 约 180 天 / 半年)
        #[arg(short, long, default_value_t = 4320)]
        bars: usize,
        /// 是否强制重新下载覆盖本地缓存
        #[arg(short, long, default_value_t = false)]
        force: bool,
    },
    /// 运行防作弊历史组合回测 (支持自定义 100U 或 200U 初始本金对比)
    Backtest {
        /// 覆盖初始测试本金 (例如 --equity 100 或 --equity 200)
        #[arg(short, long)]
        equity: Option<String>,
        /// 回测使用的 1h K 线根数 (若本地未缓存则自动下载)
        #[arg(short, long, default_value_t = 3000)]
        bars: usize,
        /// 输出 Markdown 报告路径
        #[arg(short, long, default_value = "reports/backtest_report.md")]
        report: String,
        /// 覆盖 ATR 分位下限（0 = 关闭；例如 70 表示只在 ATR 分位 >= 70% 时开仓）
        #[arg(long)]
        min_atr_pct: Option<String>,
        /// 跳过最近 N 根 K 线，用更早的历史区间回测（用于样本外验证）
        #[arg(long, default_value_t = 0)]
        skip_recent: usize,
        /// 覆盖策略模式：long_only_breakout / all_weather_breakout / volatility_squeeze / trend_pullback / ensemble_composite
        #[arg(long)]
        mode: Option<String>,
    },
    /// 实时全市场量化雷达扫描：输出大盘牛熊状态、截面动量排行与 100U 以损定仓建议表
    Scan {
        /// 是否强制从交易所拉取最新实时 K 线
        #[arg(short, long, default_value_t = false)]
        refresh: bool,
    },
    /// 生产数据多策略自动生成与 Walk-Forward 样本外盲测竞技场
    Evolve {
        /// 初始测试本金 (默认 100 USDT)
        #[arg(short, long)]
        equity: Option<String>,
        /// 使用的生产数据 1h K 线根数 (默认 6000 根 = 250 天)
        #[arg(short, long, default_value_t = 6000)]
        bars: usize,
        /// 输出多策略进化对比报告路径
        #[arg(short, long, default_value = "reports/strategy_evolution_report.md")]
        report: String,
    },
    /// 全链路实盘功能自检：自动测试开仓、服务端止损挂载、保本与移动止盈上移、平仓记账
    SimVerify,
    /// 连续逐单滚动实战推进：使用真实生产数据一单接一单连续撮合至当前最新时刻并无缝交接实盘盯盘
    StreamSim {
        /// 连续推进的 1h 生产 K 线根数 (默认 3000 根 = 125 天连续实战)
        #[arg(short, long, default_value_t = 3000)]
        bars: usize,
        /// 是否强制拉取最新实时 K 线
        #[arg(short, long, default_value_t = false)]
        refresh: bool,
    },
    /// 立即在模拟盘开立一笔 100U 以损定仓测试单 (默认自动选择当前动量第 1 名的币种)
    SimOpen {
        /// 指定币种 (如 ETHUSDT 或 SOLUSDT，不填则自动选当前动量第 1 名)
        #[arg(short, long)]
        symbol: Option<String>,
        /// 指定方向 (LONG 或 SHORT，不填则根据动量方向自动判断)
        #[arg(long)]
        side: Option<String>,
        /// 是否强制拉取最新实时价格
        #[arg(short, long, default_value_t = false)]
        refresh: bool,
    },
    /// 平掉模拟盘中的所有当前持仓并结算盈亏
    SimClose {
        /// 是否强制拉取最新实时价格
        #[arg(short, long, default_value_t = false)]
        refresh: bool,
    },
    /// 启动 Dry-Run 模拟实盘引擎 (连接实时行情，模拟撮合与服务端止损并落盘 data/live_state.json)
    DryRun {
        /// 仅执行单次巡检后退出 (用于测试或定时任务触发)
        #[arg(long, default_value_t = false)]
        once: bool,
        /// 守护进程巡检间隔秒数 (默认 300 秒 = 5 分钟)
        #[arg(short, long, default_value_t = 300)]
        interval_secs: u64,
    },
    /// 启动 Live 真金实盘引擎 (需配置 BINANCE_API_KEY，自动挂载服务端 STOP_MARKET 硬止损单)
    Live {
        /// 仅执行单次巡检后退出
        #[arg(long, default_value_t = false)]
        once: bool,
        /// 守护进程巡检间隔秒数 (默认 300 秒 = 5 分钟)
        #[arg(short, long, default_value_t = 300)]
        interval_secs: u64,
    },
    /// 查看当前实盘 / 模拟盘账户净值、实时浮盈亏与服务端止损单状态
    Status {
        /// 是否拉取交易所最新实时价格计算浮盈亏
        #[arg(short, long, default_value_t = false)]
        refresh: bool,
    },
    /// 启动真 API 盘口报价 + SQLite 数据库 (`data/coin_ai.db`) 连续买卖模拟引擎
    PaperDb {
        /// 数据库文件路径
        #[arg(long, default_value = "data/coin_ai.db")]
        db_path: String,
        /// 是否重置并重新从历史生产流初始化数据库
        #[arg(long, default_value_t = false)]
        reset: bool,
        /// 实时 API 盘口轮询次数 (设为 999999 即为 7x24 小时持续后台运行)
        #[arg(short, long, default_value_t = 5)]
        cycles: usize,
        /// 每次实时 API 盘口轮询间隔秒数 (默认 5 秒)
        #[arg(short, long, default_value_t = 5)]
        interval_secs: u64,
        /// 每隔多少个轮询周期增量刷新一次 BTC 大盘状态/截面动量/ATR 分位 (默认 360，即约 1 小时)
        #[arg(long, default_value_t = 360)]
        refresh_cycles: usize,
    },
    /// 查询 SQLite 数据库 (`data/coin_ai.db`) 中的实时盘口、买单/卖单、持仓与盈亏报表
    DbStatus {
        /// 数据库文件路径
        #[arg(long, default_value = "data/coin_ai.db")]
        db_path: String,
    },
    /// 根据 SQLite 数据库中积累的真实盘口点差、订单磨损与各币种胜率，自动优化策略并更新配置
    OptimizeDb {
        /// 数据库文件路径
        #[arg(long, default_value = "data/coin_ai.db")]
        db_path: String,
    },
    /// 生成五维量化统计与归因分析报告（核心期望值 / 月度 / 时段 / 币种点差 / 出场机制）
    Stats {
        /// 数据库文件路径
        #[arg(long, default_value = "data/coin_ai.db")]
        db_path: String,
        /// Markdown 统计报告输出路径
        #[arg(short, long, default_value = "reports/quant_statistics.md")]
        report: String,
    },
    /// 用全样本回测成交记录验证亚盘/欧盘/美盘时段效应（含前后半段稳定性检验）
    SessionReport {
        /// 回测使用的 1h K 线根数
        #[arg(short, long, default_value_t = 5750)]
        bars: usize,
    },
    /// 用全样本回测验证 ATR 波动率分位与 ADX 趋势强度对单笔期望的影响
    RegimeReport {
        /// 回测使用的 1h K 线根数
        #[arg(short, long, default_value_t = 5750)]
        bars: usize,
        /// 计算 ATR 分位时的回看窗口（K 线根数）
        #[arg(short, long, default_value_t = 500)]
        lookback: usize,
    },
    /// 演练突发“交易所被盗 / 黑天鹅闪崩”行情的毫秒级感知、紧急停做逃顶与顺势做空狙击
    DrillBlackSwan {
        /// 数据库文件路径
        #[arg(long, default_value = "data/coin_ai.db")]
        db_path: String,
    },
    /// 将真实 1s 秒级 K 线固化到本地 data/ticks_1s.db（回放压测数据层，断点续传，绝不触碰生产库）
    DownloadTicks {
        /// 固化最近 N 天 (默认 15 天)
        #[arg(long, default_value_t = 15)]
        days: u32,
        /// 币种 (默认 BTCUSDT)
        #[arg(long, default_value = "BTCUSDT")]
        symbol: String,
        /// 强制清空该币种旧数据后重拉
        #[arg(long, default_value_t = false)]
        force: bool,
        /// Tick 数据库路径
        #[arg(long, default_value = "data/ticks_1s.db")]
        db_path: String,
    },
    /// 回放压测：用真实 1s tick 重放极端/趋势/震荡窗口，注入订单落地延迟，测量滑点与盈亏影响
    ReplayStress {
        /// 选取的压力窗口数量 (急跌/急涨/高波动/横盘 各保证一个)
        #[arg(long, default_value_t = 4)]
        windows: usize,
        /// 延迟档位列表 (毫秒, 逗号分隔)
        #[arg(long, default_value = "0,100,300,500,1000,5000")]
        latencies: String,
        /// 重建盘口使用的点差 (bps)
        #[arg(long, default_value = "0.5")]
        spread_bps: String,
        /// 虚拟时钟轮询间隔秒数 (与生产 10s 巡检同频)
        #[arg(long, default_value_t = 10)]
        poll_secs: i64,
        /// Tick 数据库路径
        #[arg(long, default_value = "data/ticks_1s.db")]
        db_path: String,
        /// Markdown 报告输出路径
        #[arg(short, long, default_value = "reports/replay_stress.md")]
        report: String,
        /// 附加合成极端情景（-30% 级联闪崩 + 理想化震荡，真实数据中不存在）
        #[arg(long, default_value_t = false)]
        synthetic: bool,
        /// 开仓护圈 (bps)：落地价偏离决策价超此值模拟 IOC 限价放弃（0 = 关闭）
        #[arg(long, default_value = "30")]
        max_fill_deviation_bps: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let mut cfg = AppConfig::load(&cli.config)?;
    let data_mgr = DataManager::new("data")?;

    match cli.command {
        Commands::Download { bars, force } => {
            let mut symbols = vec![cfg.strategy.benchmark_symbol.clone()];
            for s in &cfg.strategy.universe {
                if !symbols.contains(s) {
                    symbols.push(s.clone());
                }
            }
            for sym in symbols {
                data_mgr.load_or_fetch_candles(&sym, bars, force).await?;
            }
            info!("所有交易对历史数据准备完毕！");
        }
        Commands::Backtest {
            equity,
            bars,
            report,
            min_atr_pct,
            skip_recent,
            mode,
        } => {
            if let Some(eq_str) = equity {
                cfg.account.initial_equity = Decimal::from_str(&eq_str)?;
                cfg.validate()?;
            }
            if let Some(pct) = min_atr_pct {
                cfg.strategy.min_atr_percentile = Decimal::from_str(&pct)?;
            }
            if let Some(m) = mode {
                cfg.strategy.mode = match m.as_str() {
                    "long_only_breakout" => StrategyMode::LongOnlyBreakout,
                    "all_weather_breakout" => StrategyMode::AllWeatherBreakout,
                    "volatility_squeeze" => StrategyMode::VolatilitySqueeze,
                    "trend_pullback" => StrategyMode::TrendPullback,
                    "ensemble_composite" => StrategyMode::EnsembleComposite,
                    other => anyhow::bail!(
                        "未知策略模式: {} (可选: long_only_breakout / all_weather_breakout / volatility_squeeze / trend_pullback / ensemble_composite)",
                        other
                    ),
                };
            }

            let mut symbols = vec![cfg.strategy.benchmark_symbol.clone()];
            for s in &cfg.strategy.universe {
                if !symbols.contains(s) {
                    symbols.push(s.clone());
                }
            }

            let mut all_candles = HashMap::new();
            for sym in symbols {
                let mut candles = data_mgr.load_or_fetch_candles(&sym, bars, false).await?;
                if skip_recent > 0 && candles.len() > skip_recent + 500 {
                    let keep = candles.len() - skip_recent;
                    candles.truncate(keep);
                }
                all_candles.insert(sym, candles);
            }

            let start_inst = std::time::Instant::now();
            let engine = BacktestEngine::new(cfg.clone());
            let summary = engine.run(&all_candles)?;
            let elapsed = start_inst.elapsed();

            BacktestEngine::write_markdown_report(&summary, &report)?;

            println!("\n======================================================================");
            println!(
                "  Coin-AI (Rust 引擎) 防作弊回测完成 | 耗时: {:.2?}",
                elapsed
            );
            println!("======================================================================");
            println!(
                "  初始本金        : {:.2} USDT",
                summary.initial_equity
            );
            println!(
                "  期末净值        : {:.2} USDT ({:+.2}%)",
                summary.final_equity, summary.total_return_pct
            );
            println!(
                "  最大历史回撤    : {:.2}% ({:.2} USDT) [硬熔断线: 20.0%]",
                summary.max_drawdown_pct, summary.max_drawdown_usd
            );
            println!(
                "  年化夏普比率    : {:.2}",
                summary.sharpe_ratio
            );
            println!(
                "  盈亏比(PF)      : {:.2} (单笔均盈 +{:.2}U / 单笔均亏 -{:.2}U)",
                summary.profit_factor, summary.avg_win_usd, summary.avg_loss_usd
            );
            println!(
                "  总交易笔数      : {} 笔 (胜 {} / 负 {}, 胜率 {:.1}%)",
                summary.total_trades, summary.win_trades, summary.loss_trades, summary.win_rate_pct
            );
            println!(
                "  累计手续费磨损  : {:.2} USDT (双边 Taker 0.05% + 滑点 0.05%)",
                summary.total_fees_paid
            );
            println!(
                "  期末风控状态    : {}",
                summary.final_breaker_state
            );
            println!(
                "  详细审计报告    : {}",
                report
            );
            println!("======================================================================\n");
        }
        Commands::Scan { refresh } => {
            run_market_scan(&cfg, &data_mgr, refresh).await?;
        }
        Commands::Evolve {
            equity,
            bars,
            report,
        } => {
            if let Some(eq_str) = equity {
                cfg.account.initial_equity = Decimal::from_str(&eq_str)?;
                cfg.validate()?;
            }

            let mut symbols = vec![cfg.strategy.benchmark_symbol.clone()];
            for s in &cfg.strategy.universe {
                if !symbols.contains(s) {
                    symbols.push(s.clone());
                }
            }

            let mut all_candles = HashMap::new();
            for sym in symbols {
                let candles = data_mgr.load_or_fetch_candles(&sym, bars, false).await?;
                all_candles.insert(sym, candles);
            }

            let start_inst = std::time::Instant::now();
            let results = run_strategy_evolution(&cfg, &all_candles, &report)?;
            let elapsed = start_inst.elapsed();

            println!("\n====================================================================================================================");
            println!(
                "  Coin-AI 生产数据多策略进化竞技场 (Walk-Forward 样本外盲测) | 本金: {:.2}U | 耗时: {:.2?}",
                cfg.account.initial_equity, elapsed
            );
            println!("====================================================================================================================");
            println!(
                "  排名 | 编号  | 策略流派与名称                           | 全周期净盈亏     | 样本内   | 样本外盲测净利    | 近60天实况净利    | 最大回撤 | 夏普 | 胜率"
            );
            println!("  ------------------------------------------------------------------------------------------------------------------");

            for (i, r) in results.iter().enumerate() {
                let s = &r.full_summary;
                let full_pnl = s.final_equity - s.initial_equity;
                println!(
                    "  #{:<3} | {:<5} | {:<38} | {:+6.2}U ({:+6.2}%) | {:+6.2}% | {:+6.2}U ({:+6.2}%) | {:+6.2}U ({:+6.2}%) | {:>6.2}% | {:>4.2} | {:>4.1}%",
                    i + 1,
                    r.spec.id,
                    r.spec.name,
                    full_pnl,
                    s.total_return_pct,
                    r.in_sample_return_pct,
                    r.out_of_sample_pnl_usd,
                    r.out_of_sample_return_pct,
                    r.recent_60d_pnl_usd,
                    r.recent_60d_return_pct,
                    s.max_drawdown_pct,
                    s.sharpe_ratio,
                    s.win_rate_pct
                );
            }

            if let Some(champ) = results.first() {
                BacktestEngine::write_markdown_report(
                    &champ.full_summary,
                    "reports/champion_backtest_100u.md",
                )?;
                println!("--------------------------------------------------------------------------------------------------------------------");
                println!(
                    "  [冠军策略胜出!] [{}] {}",
                    champ.spec.id, champ.spec.name
                );
                println!(
                    "  100U 净值增长: {:.2} USDT -> {:.2} USDT (净赚 {:+.2} USDT / {:+.2}%, 最大回撤 {:.2}%, 夏普 {:.2})",
                    champ.full_summary.initial_equity,
                    champ.full_summary.final_equity,
                    champ.full_summary.final_equity - champ.full_summary.initial_equity,
                    champ.full_summary.total_return_pct,
                    champ.full_summary.max_drawdown_pct,
                    champ.full_summary.sharpe_ratio
                );
                println!("  完整多策略竞技场报告: {}", report);
                println!("  冠军策略逐笔交易明细: reports/champion_backtest_100u.md");
            }
            println!("====================================================================================================================\n");
        }
        Commands::SimVerify => {
            verify_full_trading_lifecycle(&cfg, &data_mgr).await?;
        }
        Commands::StreamSim { bars, refresh } => {
            run_continuous_production_stream(&cfg, &data_mgr, bars, refresh).await?;
            print_ledger_status(&cfg, &data_mgr, false).await?;
        }
        Commands::SimOpen {
            symbol,
            side,
            refresh,
        } => {
            open_live_simulated_position(&cfg, &data_mgr, symbol, side, refresh).await?;
            print_ledger_status(&cfg, &data_mgr, false).await?;
        }
        Commands::SimClose { refresh } => {
            close_live_simulated_positions(&cfg, &data_mgr, refresh).await?;
            print_ledger_status(&cfg, &data_mgr, false).await?;
        }
        Commands::DryRun {
            once,
            interval_secs,
        } => {
            run_daemon_loop(cfg, data_mgr, true, once, interval_secs).await?;
        }
        Commands::Live {
            once,
            interval_secs,
        } => {
            run_daemon_loop(cfg, data_mgr, false, once, interval_secs).await?;
        }
        Commands::Status { refresh } => {
            print_ledger_status(&cfg, &data_mgr, refresh).await?;
        }
        Commands::PaperDb {
            db_path,
            reset,
            cycles,
            interval_secs,
            refresh_cycles,
        } => {
            let engine = SqlitePaperEngine::new(cfg.clone(), &db_path)?;
            let db_exists = std::path::Path::new(&db_path).exists();
            if reset || !db_exists {
                engine
                    .sync_production_history_to_sqlite(&data_mgr, 3000)
                    .await?;
            }
            engine
                .run_live_api_sqlite_ticks(&data_mgr, cycles, interval_secs, refresh_cycles)
                .await?;
            engine.print_sqlite_report()?;
        }
        Commands::DbStatus { db_path } => {
            let engine = SqlitePaperEngine::new(cfg, &db_path)?;
            engine.print_sqlite_report()?;
        }
        Commands::OptimizeDb { db_path } => {
            let mut engine = SqlitePaperEngine::new(cfg, &db_path)?;
            engine.optimize_strategy_from_sqlite(&data_mgr).await?;
        }
        Commands::Stats { db_path, report } => {
            let engine =
                QuantStatsEngine::new(&db_path, cfg.account.initial_equity);
            engine.generate_full_statistics(&report)?;
        }
        Commands::SessionReport { bars } => {
            let mut symbols = vec![cfg.strategy.benchmark_symbol.clone()];
            for s in &cfg.strategy.universe {
                if !symbols.contains(s) {
                    symbols.push(s.clone());
                }
            }
            let mut all_candles = HashMap::new();
            for sym in symbols {
                all_candles.insert(
                    sym.clone(),
                    data_mgr.load_or_fetch_candles(&sym, bars, false).await?,
                );
            }
            let summary = BacktestEngine::new(cfg.clone()).run(&all_candles)?;
            println!("\n====================================================================================");
            println!("  Coin-AI 全样本时段效应验证 (基于防作弊回测的真实撮合记录)");
            println!("====================================================================================");
            coin_ai::stats::print_backtest_session_attribution(&summary.trades);
        }
        Commands::RegimeReport { bars, lookback } => {
            let mut symbols = vec![cfg.strategy.benchmark_symbol.clone()];
            for s in &cfg.strategy.universe {
                if !symbols.contains(s) {
                    symbols.push(s.clone());
                }
            }
            let mut all_candles = HashMap::new();
            for sym in symbols {
                all_candles.insert(
                    sym.clone(),
                    data_mgr.load_or_fetch_candles(&sym, bars, false).await?,
                );
            }
            let summary = BacktestEngine::new(cfg.clone()).run(&all_candles)?;
            println!("\n====================================================================================");
            println!("  Coin-AI 波动率/趋势状态效应验证 (基于防作弊回测的真实撮合记录)");
            println!("====================================================================================");
            coin_ai::regime::print_regime_attribution(&summary.trades, &all_candles, lookback);
        }
        Commands::DrillBlackSwan { db_path } => {
            let engine = SqlitePaperEngine::new(cfg, &db_path)?;
            engine.run_black_swan_drill(&data_mgr).await?;
        }
        Commands::DownloadTicks {
            days,
            symbol,
            force,
            db_path,
        } => {
            let mut tick_db = coin_ai::tick_db::TickDatabase::open(&db_path)?;
            coin_ai::sim::download_ticks_to_db(&data_mgr, &mut tick_db, &symbol, days, force)
                .await?;
        }
        Commands::ReplayStress {
            windows,
            latencies,
            spread_bps,
            poll_secs,
            db_path,
            report,
            synthetic,
            max_fill_deviation_bps,
        } => {
            let lat_vec: Vec<i64> = latencies
                .split(',')
                .filter_map(|s| s.trim().parse::<i64>().ok())
                .collect();
            if lat_vec.is_empty() {
                anyhow::bail!("延迟档位列表不能为空, 例如: 0,100,300,500,1000,5000");
            }
            let spread = Decimal::from_str(&spread_bps)?;
            let collar = Decimal::from_str(&max_fill_deviation_bps)?;
            coin_ai::sim::run_replay_stress(
                &cfg,
                &data_mgr,
                &db_path,
                windows,
                &lat_vec,
                spread,
                poll_secs,
                &report,
                synthetic,
                collar,
            )
            .await?;
        }
    }

    Ok(())
}
