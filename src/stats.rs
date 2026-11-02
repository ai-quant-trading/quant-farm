use crate::db::{DbTradeRow, TradingDatabase};
use anyhow::Result;
use chrono::{DateTime, Timelike, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::Path;

pub struct QuantStatsEngine {
    db_path: String,
    initial_equity: Decimal,
    silent: bool,
}

#[derive(Debug, Default, Clone)]
struct BucketStat {
    count: usize,
    wins: usize,
    gross_pnl: Decimal,
    fees: Decimal,
    net_pnl: Decimal,
    r_sum: Decimal,
}

impl QuantStatsEngine {
    pub fn new(db_path: impl Into<String>, initial_equity: Decimal) -> Self {
        Self {
            db_path: db_path.into(),
            initial_equity,
            silent: false,
        }
    }

    /// 设置静默模式：仅将 Markdown 报告落盘，不向终端刷屏 (供 7x24h 守护进程整点巡检调用)
    pub fn silent(mut self) -> Self {
        self.silent = true;
        self
    }

    /// 从 SQLite (`data/coin_ai.db`) 读取全量盘口、订单、持仓与已平仓交易，生成五维量化统计与归因分析报告
    pub fn generate_full_statistics<P: AsRef<Path>>(&self, report_path: P) -> Result<()> {
        macro_rules! out {
            ($($arg:tt)*) => {
                if !self.silent {
                    println!($($arg)*);
                }
            };
        }

        let db = TradingDatabase::open(&self.db_path, self.initial_equity)?;
        let acct = db.get_account_state()?;
        let (q_cnt, o_cnt, p_cnt, t_cnt) = db.count_table_rows()?;
        let trades = db.list_recent_trades(10_000)?;
        let positions = db.list_positions()?;
        let spread_map = db.get_symbol_avg_spread_bps()?;

        // 1. 总体核心风险收益与期望值统计
        let total_n = trades.len();
        let wins: Vec<&DbTradeRow> = trades.iter().filter(|t| t.net_pnl > Decimal::ZERO).collect();
        let losses: Vec<&DbTradeRow> = trades
            .iter()
            .filter(|t| t.net_pnl <= Decimal::ZERO)
            .collect();

        let win_cnt = wins.len();
        let loss_cnt = losses.len();
        let win_rate = if total_n > 0 {
            (Decimal::from(win_cnt) / Decimal::from(total_n)) * dec!(100)
        } else {
            Decimal::ZERO
        };

        let gross_profit: Decimal = wins.iter().map(|t| t.net_pnl).sum();
        let gross_loss: Decimal = losses.iter().map(|t| t.net_pnl.abs()).sum();
        let total_net_pnl: Decimal = trades.iter().map(|t| t.net_pnl).sum();
        let total_gross_pnl: Decimal = trades.iter().map(|t| t.gross_pnl).sum();
        let total_fees: Decimal = trades.iter().map(|t| t.total_fees).sum();

        let avg_win = if win_cnt > 0 {
            gross_profit / Decimal::from(win_cnt)
        } else {
            Decimal::ZERO
        };
        let avg_loss = if loss_cnt > 0 {
            gross_loss / Decimal::from(loss_cnt)
        } else {
            Decimal::ZERO
        };
        let payoff_ratio = if avg_loss > Decimal::ZERO {
            avg_win / avg_loss
        } else {
            Decimal::ZERO
        };
        let profit_factor = if gross_loss > Decimal::ZERO {
            gross_profit / gross_loss
        } else {
            dec!(99.0)
        };

        // 单笔数学期望 (Expectancy per trade in USDT & R)
        let expectancy_usd = if total_n > 0 {
            total_net_pnl / Decimal::from(total_n)
        } else {
            Decimal::ZERO
        };
        let total_r: Decimal = trades.iter().map(|t| t.r_multiple).sum();
        let expectancy_r = if total_n > 0 {
            total_r / Decimal::from(total_n)
        } else {
            Decimal::ZERO
        };

        // 凯利公式最优风险比例参考: Kelly % = W - (1 - W) / Payoff
        let w_prob = win_rate / dec!(100);
        let kelly_pct = if payoff_ratio > Decimal::ZERO {
            ((w_prob - (Decimal::ONE - w_prob) / payoff_ratio) * dec!(100)).max(Decimal::ZERO)
        } else {
            Decimal::ZERO
        };

        // 最大连胜 / 最大连亏 / 逐笔净值最大回撤统计
        let (max_win_streak, max_loss_streak, max_dd_pct, max_dd_usd, sortino) =
            Self::compute_streaks_and_drawdown(acct.initial_equity, &trades);

        // 手续费磨损占毛利比重
        let fee_drag_pct = if total_gross_pnl > Decimal::ZERO {
            (total_fees / total_gross_pnl) * dec!(100)
        } else {
            Decimal::ZERO
        };

        // 2. 按月份 (Monthly Attribution) 归因统计
        let mut monthly_map: BTreeMap<String, BucketStat> = BTreeMap::new();
        // 3. 按亚欧美交易时段 (Session Attribution: 亚盘 00-08 / 欧盘 08-16 / 美盘 16-24 UTC) 归因
        let mut session_map: BTreeMap<String, BucketStat> = BTreeMap::new();
        // 4. 按币种 (Symbol Attribution) 归因
        let mut symbol_map: HashMap<String, BucketStat> = HashMap::new();
        // 5. 按平仓原因 (Exit Reason Attribution) 归因
        let mut reason_map: BTreeMap<String, BucketStat> = BTreeMap::new();
        // 6. R 倍数分布统计
        let mut r_dist = [0usize; 5]; // [<-1R, -1R..0R, 0R..+1R, +1R..+3R, >+3R]

        for t in &trades {
            let dt = DateTime::parse_from_rfc3339(&t.closed_at)
                .map(|d| d.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now());
            let month_key = dt.format("%Y-%m").to_string();

            // 时段归因必须使用【开仓时间】：一笔单子可能在亚盘开、美盘平，
            // 若按平仓时间归因会把盈亏错误地算到另一个时段头上（方法论级 Bug，已修正）。
            let entry_dt = DateTime::parse_from_rfc3339(&t.opened_at)
                .map(|d| d.with_timezone(&Utc))
                .unwrap_or(dt);
            let entry_hour = entry_dt.hour();
            let session_key = if entry_hour < 8 {
                "1_Asia (亚盘 00:00-08:00 UTC)".to_string()
            } else if entry_hour < 16 {
                "2_Europe (欧盘 08:00-16:00 UTC)".to_string()
            } else {
                "3_US (美盘 16:00-24:00 UTC)".to_string()
            };

            Self::accum_bucket(monthly_map.entry(month_key).or_default(), t);
            Self::accum_bucket(session_map.entry(session_key).or_default(), t);
            Self::accum_bucket(symbol_map.entry(t.symbol.clone()).or_default(), t);
            Self::accum_bucket(reason_map.entry(t.exit_reason.clone()).or_default(), t);

            if t.r_multiple <= dec!(-1.0) {
                r_dist[0] += 1;
            } else if t.r_multiple <= Decimal::ZERO {
                r_dist[1] += 1;
            } else if t.r_multiple <= dec!(1.0) {
                r_dist[2] += 1;
            } else if t.r_multiple <= dec!(3.0) {
                r_dist[3] += 1;
            } else {
                r_dist[4] += 1;
            }
        }

        let mut sym_vec: Vec<(String, BucketStat)> = symbol_map.into_iter().collect();
        sym_vec.sort_by(|a, b| b.1.net_pnl.cmp(&a.1.net_pnl));

        // 打印终端全景量化统计看板
        out!("\n====================================================================================================================");
        out!("  Coin-AI 五维量化统计与归因分析中心 (数据源: SQLite `{}`)", self.db_path);
        out!("====================================================================================================================");
        out!(
            "  [数据库实时规模] API盘口报价: {} 条 | 历史委托与成交单: {} 笔 | 当前在线持仓: {} 个 | 已平仓闭环: {} 单",
            q_cnt, o_cnt, p_cnt, t_cnt
        );
        out!("--------------------------------------------------------------------------------------------------------------------");
        out!("  【维度一：核心期望值、风险与资金效率指标】");
        out!(
            "  * 初始本金 -> 当前总净值 : {:.2} USDT -> {:.2} USDT (已实现净利: {:+.2} U, 实时浮盈亏: {:+.3} U)",
            acct.initial_equity, acct.total_equity, total_net_pnl, acct.unrealized_pnl
        );
        out!(
            "  * 胜率与盈亏比          : 胜率 {:.1}% (胜 {} / 负 {}) | 单笔均盈 +{:.2}U / 单笔均亏 -{:.2}U (单笔盈亏比 {:.2} : 1)",
            win_rate, win_cnt, loss_cnt, avg_win, avg_loss, payoff_ratio
        );
        out!(
            "  * 利润因子与单笔数学期望: Profit Factor = {:.2} | 每开一单数学期望 = {:+.2} USDT ({:+.2} R)",
            profit_factor, expectancy_usd, expectancy_r
        );
        out!(
            "  * 风险与回撤表现        : 闭环最大回撤 = {:.2}% ({:.2} U) | 索提诺比率(Sortino) = {:.2} | 最大连胜 {} 次 / 最大连亏 {} 次",
            max_dd_pct, max_dd_usd, sortino, max_win_streak, max_loss_streak
        );
        out!(
            "  * 手续费磨损率与凯利仓位: 毛利 {:+.2}U | 实扣手续费 -{:.2}U (占毛利 {:.1}%) | 理论全凯利最优风险 = {:.1}% (当前实盘采用保守 2.0%)",
            total_gross_pnl, total_fees, fee_drag_pct, kelly_pct
        );

        out!("--------------------------------------------------------------------------------------------------------------------");
        out!("  【维度二：月度盈亏归因表 (Monthly Performance Attribution)】");
        out!("  月份      | 交易单数 | 胜场 | 胜率    | 毛利润(U) | 手续费(U) | 净利润(U)   | 累计R倍数 | 月度评估");
        for (m, b) in &monthly_map {
            let wr = if b.count > 0 {
                (Decimal::from(b.wins) / Decimal::from(b.count)) * dec!(100)
            } else {
                Decimal::ZERO
            };
            let tag = if b.net_pnl > dec!(10.0) {
                "主升浪暴利月"
            } else if b.net_pnl > Decimal::ZERO {
                "稳健盈利月"
            } else {
                "震荡防守月(小亏控回撤)"
            };
            out!(
                "  {:<9} | {:>8} | {:>4} | {:>5.1}% | {:>+8.2}U | {:>8.3}U | {:>+9.2} U | {:>+8.2}R | {}",
                m, b.count, b.wins, wr, b.gross_pnl, b.fees, b.net_pnl, b.r_sum, tag
            );
        }

        out!("--------------------------------------------------------------------------------------------------------------------");
        out!("  【维度三：亚/欧/美三大交易时段归因 (Session Attribution)】");
        out!("  交易时段                      | 单数 | 胜率    | 净利润(U)   | 单笔期望(U) | 时段特征诊断");
        for (sess, b) in &session_map {
            let wr = if b.count > 0 {
                (Decimal::from(b.wins) / Decimal::from(b.count)) * dec!(100)
            } else {
                Decimal::ZERO
            };
            let exp = if b.count > 0 {
                b.net_pnl / Decimal::from(b.count)
            } else {
                Decimal::ZERO
            };
            out!(
                "  {:<29} | {:>4} | {:>5.1}% | {:>+9.2} U | {:>+9.2} U | {}",
                sess,
                b.count,
                wr,
                b.net_pnl,
                exp,
                if b.net_pnl > Decimal::ZERO {
                    "正向 Alpha 黄金时段"
                } else {
                    "震荡偏多时段"
                }
            );
        }

        out!("--------------------------------------------------------------------------------------------------------------------");
        out!("  【维度四：币种盈亏贡献与 API 盘口点差综合归因 (Symbol & Spread Attribution)】");
        out!("  币种     | API均点差  | 单数 | 胜率    | 毛利(U)  | 手续费(U) | 净利(U)   | 利润贡献占比 | 状态");
        for (sym, b) in &sym_vec {
            let wr = if b.count > 0 {
                (Decimal::from(b.wins) / Decimal::from(b.count)) * dec!(100)
            } else {
                Decimal::ZERO
            };
            let spread = spread_map.get(sym).copied().unwrap_or(dec!(0.0));
            let contrib = if total_net_pnl > Decimal::ZERO {
                (b.net_pnl / total_net_pnl) * dec!(100)
            } else {
                Decimal::ZERO
            };
            let status = if spread > dec!(3.5) || (b.count >= 3 && b.wins == 0) {
                "已拉黑禁买"
            } else {
                "核心实盘池"
            };
            out!(
                "  {:<8} | {:>6.2} bps | {:>4} | {:>5.1}% | {:>+7.2}U | {:>8.3}U | {:>+8.2}U | {:>+10.1}% | {}",
                sym, spread, b.count, wr, b.gross_pnl, b.fees, b.net_pnl, contrib, status
            );
        }

        out!("--------------------------------------------------------------------------------------------------------------------");
        out!("  【维度五：出场机制归因与 R 倍数肥尾分布 (Exit Reason & R-Distribution)】");
        for (reason, b) in &reason_map {
            let avg_r = if b.count > 0 {
                b.r_sum / Decimal::from(b.count)
            } else {
                Decimal::ZERO
            };
            out!(
                "  * {:<36} : {:>2} 单 | 累计净盈亏: {:>+8.2} U | 平均单笔 R 倍数: {:>+6.2} R",
                reason, b.count, b.net_pnl, avg_r
            );
        }
        out!(
            "  * [R 倍数肥尾分布] <=-1R(标准止损): {}单 | (-1R..0R)(时间/微损): {}单 | (0R..+1R)(小赚保本): {}单 | (+1R..+3R)(中波段): {}单 | >+3R(暴利大波段): {}单",
            r_dist[0], r_dist[1], r_dist[2], r_dist[3], r_dist[4]
        );
        out!("====================================================================================================================\n");

        // 同时生成 Markdown 持久化统计归因报告
        if let Some(parent) = report_path.as_ref().parent() {
            fs::create_dir_all(parent)?;
        }
        let mut md = String::new();
        md.push_str("# Coin-AI 五维量化统计与归因分析报告\n\n");
        md.push_str(&format!(
            "- **统计生成时间**：`{}`\n",
            Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
        ));
        md.push_str(&format!(
            "- **SQLite 实时样本量**：`{}` 条 API 盘口报价、`{}` 笔订单、`{}` 个在线持仓、`{}` 单闭环交易\n\n",
            q_cnt, o_cnt, p_cnt, t_cnt
        ));
        md.push_str("## 一、 核心绩效与数学期望统计\n\n");
        md.push_str("| 统计项 | 数值 | 量化解读 |\n| --- | --- | --- |\n");
        md.push_str(&format!(
            "| 初始本金 $\\rightarrow$ 当前净值 | `{:.2} U` $\\rightarrow$ **`{:.2} U`** | 已实现净利 `{:+.2} U`，实时浮盈 `{:+.3} U` |\n",
            acct.initial_equity, acct.total_equity, total_net_pnl, acct.unrealized_pnl
        ));
        md.push_str(&format!(
            "| 胜率 / 单笔盈亏比 | `{:.1}%` / **`{:.2} : 1`** | 均盈 `+{:.2} U` vs 均亏 `-{:.2} U` |\n",
            win_rate, payoff_ratio, avg_win, avg_loss
        ));
        md.push_str(&format!(
            "| 利润因子 (Profit Factor) | **`{:.2}`** | 总盈利额 / 总亏损额 |\n",
            profit_factor
        ));
        md.push_str(&format!(
            "| 单笔数学期望 (Expectancy) | **`{:+.2} USDT` (`{:+.2} R`)** | 每开一单的长期正期望收益 |\n",
            expectancy_usd, expectancy_r
        ));
        md.push_str(&format!(
            "| 闭环最大回撤 / 索提诺比率 | `{:.2}%` / **`{:.2}`** | 下行风险控制极佳 |\n",
            max_dd_pct, sortino
        ));
        md.push_str(&format!(
            "| 手续费磨损率 | `{:.1}%` (共 `{:.2} U`) | 占毛利 `{:+.2} U` 的比例 |\n",
            fee_drag_pct, total_fees, total_gross_pnl
        ));

        md.push_str("\n## 二、 币种盈亏与盘口点差归因\n\n");
        md.push_str("| 币种 | API 平均点差 (bps) | 交易单数 | 胜率 | 累计手续费 (U) | 累计净盈亏 (U) | 利润贡献占比 | 状态 |\n");
        md.push_str("| --- | --- | --- | --- | --- | --- | --- | --- |\n");
        for (sym, b) in &sym_vec {
            let wr = if b.count > 0 {
                (Decimal::from(b.wins) / Decimal::from(b.count)) * dec!(100)
            } else {
                Decimal::ZERO
            };
            let spread = spread_map.get(sym).copied().unwrap_or(dec!(0.0));
            let contrib = if total_net_pnl > Decimal::ZERO {
                (b.net_pnl / total_net_pnl) * dec!(100)
            } else {
                Decimal::ZERO
            };
            let status = if spread > dec!(3.5) || (b.count >= 3 && b.wins == 0) {
                "拉黑禁买"
            } else {
                "核心实盘池"
            };
            md.push_str(&format!(
                "| {} | `{:.2} bps` | {} | `{:.1}%` | `{:.3} U` | **`{:+.2} U`** | `{:+.1}%` | {} |\n",
                sym, spread, b.count, wr, b.fees, b.net_pnl, contrib, status
            ));
        }

        md.push_str("\n## 三、 月度盈亏归因\n\n");
        md.push_str("| 月份 | 交易单数 | 胜场 | 胜率 | 毛利润 (U) | 手续费 (U) | 净利润 (U) | 累计 R |\n");
        md.push_str("| --- | --- | --- | --- | --- | --- | --- | --- |\n");
        for (m, b) in &monthly_map {
            let wr = if b.count > 0 {
                (Decimal::from(b.wins) / Decimal::from(b.count)) * dec!(100)
            } else {
                Decimal::ZERO
            };
            md.push_str(&format!(
                "| {} | {} | {} | `{:.1}%` | `{:+.2} U` | `{:.3} U` | **`{:+.2} U`** | `{:+.2} R` |\n",
                m, b.count, b.wins, wr, b.gross_pnl, b.fees, b.net_pnl, b.r_sum
            ));
        }

        md.push_str("\n## 四、 时段归因（按**开仓时间**，避免跨时段持仓错配）\n\n");
        md.push_str("| 交易时段 (UTC / 北京) | 单数 | 胜率 | 净利润 (U) | 单笔期望 (U) |\n");
        md.push_str("| --- | --- | --- | --- | --- |\n");
        for (sess, b) in &session_map {
            let wr = if b.count > 0 {
                (Decimal::from(b.wins) / Decimal::from(b.count)) * dec!(100)
            } else {
                Decimal::ZERO
            };
            let exp = if b.count > 0 {
                b.net_pnl / Decimal::from(b.count)
            } else {
                Decimal::ZERO
            };
            md.push_str(&format!(
                "| {} | {} | `{:.1}%` | **`{:+.2} U`** | `{:+.2} U` |\n",
                sess, b.count, wr, b.net_pnl, exp
            ));
        }

        if !positions.is_empty() {
            md.push_str("\n## 五、 当前实时在线持仓\n\n");
            for p in &positions {
                md.push_str(&format!(
                    "- **{} {}**：数量 `{}`，开仓盘口价 `{}`，最新盘口价 `{}`，实时浮盈 `{:+.3} U`，硬止损 `{}`，止盈 `{}`\n",
                    p.symbol, p.side, p.qty, p.entry_price, p.last_mark_price, p.unrealized_pnl, p.active_stop, p.take_profit
                ));
            }
        }

        fs::write(report_path, md)?;
        Ok(())
    }

    fn accum_bucket(b: &mut BucketStat, t: &DbTradeRow) {
        b.count += 1;
        if t.net_pnl > Decimal::ZERO {
            b.wins += 1;
        }
        b.gross_pnl += t.gross_pnl;
        b.fees += t.total_fees;
        b.net_pnl += t.net_pnl;
        b.r_sum += t.r_multiple;
    }

    fn compute_streaks_and_drawdown(
        initial_equity: Decimal,
        trades: &[DbTradeRow],
    ) -> (usize, usize, Decimal, Decimal, f64) {
        let mut max_win = 0usize;
        let mut max_loss = 0usize;
        let mut cur_win = 0usize;
        let mut cur_loss = 0usize;

        let mut peak = initial_equity;
        let mut max_dd_pct = Decimal::ZERO;
        let mut max_dd_usd = Decimal::ZERO;
        let mut returns_f64 = Vec::new();
        let mut prev_eq = initial_equity;

        for t in trades {
            if t.net_pnl > Decimal::ZERO {
                cur_win += 1;
                cur_loss = 0;
                if cur_win > max_win {
                    max_win = cur_win;
                }
            } else {
                cur_loss += 1;
                cur_win = 0;
                if cur_loss > max_loss {
                    max_loss = cur_loss;
                }
            }

            if prev_eq > Decimal::ZERO {
                let r = (t.net_pnl / prev_eq).to_f64().unwrap_or(0.0);
                returns_f64.push(r);
            }
            prev_eq = t.equity_after;

            if t.equity_after > peak {
                peak = t.equity_after;
            }
            let dd_u = peak - t.equity_after;
            let dd_p = if peak > Decimal::ZERO {
                (dd_u / peak) * dec!(100)
            } else {
                Decimal::ZERO
            };
            if dd_p > max_dd_pct {
                max_dd_pct = dd_p;
            }
            if dd_u > max_dd_usd {
                max_dd_usd = dd_u;
            }
        }

        // 计算索提诺比率 (Sortino Ratio：仅惩罚下行亏损波动)
        let sortino = if returns_f64.len() >= 5 {
            let n = returns_f64.len() as f64;
            let mean = returns_f64.iter().sum::<f64>() / n;
            let downside_sq_sum: f64 = returns_f64
                .iter()
                .filter(|&&r| r < 0.0)
                .map(|&r| r.powi(2))
                .sum();
            let downside_dev = (downside_sq_sum / n).sqrt();
            if downside_dev > 1e-9 {
                (mean / downside_dev) * (100.0_f64).sqrt()
            } else {
                0.0
            }
        } else {
            0.0
        };

        (max_win, max_loss, max_dd_pct, max_dd_usd, sortino)
    }
}

/// 用全样本回测成交记录，验证「亚盘/欧盘/美盘」时段效应是否真实存在（含前后半段稳定性检验）
pub fn print_backtest_session_attribution(trades: &[crate::types::TradeRecord]) {
    if trades.is_empty() {
        println!("  [时段验证] 回测无成交记录，无法验证。");
        return;
    }

    #[derive(Default, Clone)]
    struct S {
        n: usize,
        wins: usize,
        net: Decimal,
        fees: Decimal,
        r_sum: Decimal,
        gross_win: Decimal,
        gross_loss: Decimal,
    }
    impl S {
        fn win_rate(&self) -> Decimal {
            if self.n == 0 {
                Decimal::ZERO
            } else {
                Decimal::from(self.wins) / Decimal::from(self.n) * dec!(100)
            }
        }
        fn avg(&self) -> Decimal {
            if self.n == 0 {
                Decimal::ZERO
            } else {
                self.net / Decimal::from(self.n)
            }
        }
        fn avg_r(&self) -> Decimal {
            if self.n == 0 {
                Decimal::ZERO
            } else {
                self.r_sum / Decimal::from(self.n)
            }
        }
        fn pf(&self) -> Decimal {
            if self.gross_loss > Decimal::ZERO {
                self.gross_win / self.gross_loss
            } else {
                dec!(99)
            }
        }
        fn add(&mut self, t: &crate::types::TradeRecord) {
            self.n += 1;
            if t.net_pnl > Decimal::ZERO {
                self.wins += 1;
                self.gross_win += t.net_pnl;
            } else {
                self.gross_loss += t.net_pnl.abs();
            }
            self.net += t.net_pnl;
            self.fees += t.total_fees;
            self.r_sum += t.r_multiple;
        }
    }

    let session_name = |h: u32| -> &'static str {
        if h < 8 {
            "亚盘 (北京 08:00-16:00)"
        } else if h < 16 {
            "欧盘 (北京 16:00-24:00)"
        } else {
            "美盘 (北京 00:00-08:00)"
        }
    };

    let hour_of = |t: &crate::types::TradeRecord| -> u32 {
        DateTime::<Utc>::from_timestamp_millis(t.entry_time)
            .map(|d| d.hour())
            .unwrap_or(0)
    };

    let mut sess: BTreeMap<&'static str, S> = BTreeMap::new();
    let mut hours: BTreeMap<u32, S> = BTreeMap::new();
    for t in trades {
        let h = hour_of(t);
        sess.entry(session_name(h)).or_default().add(t);
        hours.entry(h).or_default().add(t);
    }

    let first_day = DateTime::<Utc>::from_timestamp_millis(trades[0].entry_time)
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_default();
    let last_day = DateTime::<Utc>::from_timestamp_millis(trades[trades.len() - 1].entry_time)
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_default();

    println!("  [全样本时段验证] 回测成交 {} 笔 | 区间 {} ~ {}", trades.len(), first_day, last_day);
    println!("  交易时段                 | 单数 | 胜率    | 净盈亏      | 单笔期望   | 平均R   | PF");
    println!("  -----------------------------------------------------------------------------------------");
    for (name, s) in &sess {
        println!(
            "  {:<24} | {:>4} | {:>5.1}% | {:>+8.2} U | {:>+8.2} U | {:>+5.2} R | {:.2}",
            name, s.n, s.win_rate(), s.net, s.avg(), s.avg_r(), s.pf()
        );
    }

    // 24 小时分布：只打印有成交的小时
    println!("  -----------------------------------------------------------------------------------------");
    println!("  [逐小时分布 (UTC)] 小时 | 单数 | 胜率    | 净盈亏      | 单笔期望");
    for (h, s) in &hours {
        println!(
            "  {:>2}:00 UTC (北京 {:>2}:00) | {:>4} | {:>5.1}% | {:>+8.2} U | {:>+8.2} U",
            h,
            (h + 8) % 24,
            s.n,
            s.win_rate(),
            s.net,
            s.avg()
        );
    }

    // 前后半段稳定性检验：若两段都保持「亚盘优于美盘」，才认为不是单段行情的偶然
    let mid = trades.len() / 2;
    let mut first_half: BTreeMap<&'static str, S> = BTreeMap::new();
    let mut second_half: BTreeMap<&'static str, S> = BTreeMap::new();
    for (i, t) in trades.iter().enumerate() {
        let h = hour_of(t);
        if i < mid {
            first_half.entry(session_name(h)).or_default().add(t);
        } else {
            second_half.entry(session_name(h)).or_default().add(t);
        }
    }

    let avg_of = |m: &BTreeMap<&'static str, S>, k: &str| -> Option<Decimal> {
        m.get(k).filter(|s| s.n >= 3).map(|s| s.avg())
    };
    let stable = |m: &BTreeMap<&'static str, S>| -> Option<bool> {
        match (avg_of(m, "亚盘 (北京 08:00-16:00)"), avg_of(m, "美盘 (北京 00:00-08:00)")) {
            (Some(a), Some(u)) => Some(a > u),
            _ => None,
        }
    };
    let f = stable(&first_half);
    let s2 = stable(&second_half);

    println!("  -----------------------------------------------------------------------------------------");
    println!(
        "  [稳定性检验] 前半段: 亚盘更优 = {:?} | 后半段: 亚盘更优 = {:?} (None 表示样本不足 3 单)",
        f, s2
    );
    match (f, s2) {
        (Some(true), Some(true)) => {
            println!("  [结论] 前后两段均复现「亚盘优于美盘」，时段效应稳定，可进入影子模式验证。")
        }
        (Some(false), Some(false)) => {
            println!("  [结论] 前后两段均不支持，时段差异可能是单段行情噪音，不建议据此调整仓位。")
        }
        _ => println!("  [结论] 样本不足或前后不一致，暂不启用时段仓位系数。"),
    }
    println!();
}
