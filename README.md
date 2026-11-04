# Coin-AI：Rust 金融级小资金（100U / 200U）数字货币量化系统

本项目是一套使用 **纯 Rust** 构建、专为 **100 USDT ～ 200 USDT 小资金实盘验证**
设计的数字货币量化交易系统。系统采用**“大盘状态过滤 + 截面动量选币 + 1h 唐奇安趋势突破 +
AI 风险一票否决”**的四层漏斗架构，以“先求生存、严控回撤、回测与实盘 100% 同源”为核心准则。

## 核心工程特性

1. **128 位定点数零精度误差 (`rust_decimal`)**：所有价格、数量、止损与资金计算均采用十进制高精度定点数，自动对齐交易所 `tick_size` 与 `step_size`，100% 杜绝浮点数精度拒单。
1. **以损定仓与三级账户熔断 (`src/risk.rs`)**：单笔最大亏损严格锁定在总本金的 **2.0%（100U 模式下单笔最多亏 2 USDT）**；内置日内 5% 冷却、周 10% 降仓、总回撤 20% 硬停机三级熔断。
1. **服务端硬止损与零裸奔保护 (`src/exchange.rs`)**：实盘开仓成交后 1 秒内自动在交易所挂载 `STOP_MARKET`（`reduceOnly=true`）触发止损单；若止损单挂载失败，系统立即自动市价平仓回滚，绝不允许仓位裸奔。
1. **AI 风险一票否决器 (`src/ai.rs`)**：结合本地微观结构异常检测（极端天地针、资金费率拥挤度）与大模型（DeepSeek / OpenAI）公告语义审查，在开仓前拦截突发黑天鹅。

## 真实历史数据回测表现（3,000 小时 / 10 币种组合）

回测严格执行：`T` 时刻收盘确认信号、`T+1` 时刻开盘价成交，双边强制扣除 `0.05%` Taker 手续费 + `0.05%` 滑点 + 每 8 小时资金费率：

| 指标 | 100 USDT 模式 ([`reports/backtest_100u.md`](reports/backtest_100u.md)) | 200 USDT 模式 ([`reports/backtest_200u.md`](reports/backtest_200u.md)) |
| --- | --- | --- |
| 初始本金 | `100.00 USDT` | `200.00 USDT` |
| 期末净值 | **`119.15 USDT (+19.15%)`** | **`238.43 USDT (+19.21%)`** |
| 最大历史回撤 | `16.84%` (低于 20% 熔断线) | `16.87%` (低于 20% 熔断线) |
| 年化夏普比率 | **`1.40`** | **`1.40`** |
| 盈亏比 (Profit Factor) | **`1.39`** (单笔盈亏比 `2.04 : 1`) | **`1.39`** (单笔盈亏比 `2.04 : 1`) |
| 总交易笔数 / 胜率 | `69 笔` / `40.5%` | `69 笔` / `40.5%` |
| Rust 引擎回测耗时 | **`0.68 秒`** | **`0.70 秒`** |

## 快速开始（CLI 命令指南）

### 1. 实时全市场雷达扫描（无需 API Key）

一键拉取当前最新市场行情，输出 `BTCUSDT` 大盘牛熊诊断、9 大主流币截面动量排行榜，以及 **100U 以损定仓实时测算表**：

```bash
cargo run --release -- scan --refresh
```

### 2. 运行防作弊历史回测（对比 100U 与 200U）

```bash
# 运行 100 USDT 回测并生成报告
cargo run --release -- backtest --equity 100 --bars 3000 --report reports/backtest_100u.md

# 运行 200 USDT 回测并生成报告
cargo run --release -- backtest --equity 200 --bars 3000 --report reports/backtest_200u.md
```

### 2.1 策略模式与当前冠军：RegimeRouter

`config/settings.toml` 的 `[strategy] mode` 决定用哪套规则，回测时也可用 `--mode` 临时覆盖：

| mode | 说明 |
| --- | --- |
| `regime_router` | **当前生产默认**。Bull 走突破多头臂，Bear 走成熟熊市空头臂，Chop 空仓 |
| `long_only_breakout` | 只做多头突破，Bear/Chop 空仓（原冠军，Router 的双窗口对照组） |
| `bear_short` | 只做成熟熊市空头（Router 的 Bear 臂单独形态） |
| `chop_mean_reversion` | 震荡区间均值回归（9000h 回测 -19.83%，未采用） |
| `all_weather_breakout` / `volatility_squeeze` / `trend_pullback` / `ensemble_composite` | 早期多空/融合模式，横向对比全部落后于 Router |

双窗口验证数据见 [`reports/regime_router_validation.md`](reports/regime_router_validation.md)：
最近 8791h 净值 142.83U / 回撤 13.88% / Sharpe 1.35，更早 5791h 净值 111.45U / 回撤 12.90%，
两个窗口都优于 `long_only_breakout` 的 133.80U / 17.62% 与 98.68U / 17.62%。

```bash
# 指定模式跑回测
./target/release/coin-ai backtest --bars 9000 --mode regime_router --report reports/backtest_9000_regime_router.md
```

### 3. 启动 100U 模拟实盘守护进程（Dry-Run 模式）

连接实时行情进行模拟撮合，持仓与止损状态自动持久化至 `data/live_state.json`：

```bash
# 执行单次巡检
cargo run --release -- dry-run --once

# 启动 7x24 小时后台持续巡检 (默认每 300 秒巡检一次)
cargo run --release -- dry-run --interval-secs 300

# 随时查看当前账户净值、持仓与服务端止损单状态
cargo run --release -- status
```

### 4. SQLite 真实盘口连续模拟 + 统计归因闭环

```bash
# 7x24 小时后台运行：每 10 秒拉真实 bookTicker 盘口，买卖/止损/盈亏全部落库
./target/release/coin-ai paper-db --cycles 999999 --interval-secs 10

# 生产推荐：用监管脚本启动，进程异常退出后 5 秒自动拉起，并自动轮转日志
nohup bash scripts/run-paper-db.sh >/dev/null 2>&1 &
tail -f logs/paper-db.log

# 查看数据库实时盘口、订单、持仓与闭环流水
./target/release/coin-ai db-status

# 五维量化统计与归因（核心期望值 / 月度 / 亚欧美时段 / 币种点差 / 出场机制）
./target/release/coin-ai stats

# 用全样本回测验证时段效应（任何“某时段更强”的说法先过这道验证）
./target/release/coin-ai session-report --bars 5750

# 根据数据库累积的点差与胜率自动优化策略并回写配置（含 time_stop 网格寻优）
./target/release/coin-ai optimize-db

# 黑天鹅（交易所被盗/闪崩）应急演练：停做多单 + 清退持仓 + 顺势狙击空头
./target/release/coin-ai drill-black-swan
```

守护进程每满 1 小时会自动把最新五维统计写入 [`reports/quant_statistics.md`](reports/quant_statistics.md)，并执行一次策略自检。

### 5. 启动 100U 真金实盘（Live 模式）

先复制 `.env.example` 配置币安合约 API Key（建议先开启模拟测试或小额权限，仅勾选“允许合约交易”，**严禁勾选提币权限**）：

```bash
export BINANCE_API_KEY="你的币安API_KEY"
export BINANCE_API_SECRET="你的币安API_SECRET"
export NOTIFY_WEBHOOK_URL="你的飞书或Telegram机器人Webhook(可选)"

cargo run --release -- live --interval-secs 300
```

## 文档导航

- [01 - 策略体系设计 (`docs/01-strategy-design.md`)](docs/01-strategy-design.md)
- [02 - 风控与仓位管理规范 (`docs/02-risk-management.md`)](docs/02-risk-management.md)
- [03 - Rust 系统架构与实盘路线图 (`docs/03-system-architecture.md`)](docs/03-system-architecture.md)
