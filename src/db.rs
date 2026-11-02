use anyhow::Result;
use chrono::Utc;
use rusqlite::{params, Connection};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::path::Path;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct ApiQuoteRow {
    pub symbol: String,
    pub bid_price: Decimal,
    pub ask_price: Decimal,
    pub last_price: Decimal,
    pub spread_bps: Decimal,
    pub source_api: String,
    pub fetched_at: String,
}

#[derive(Debug, Clone)]
pub struct DbOrderRow {
    pub order_id: String,
    pub symbol: String,
    pub side: String,           // BUY / SELL
    pub action: String,         // OPEN_LONG / CLOSE_LONG / OPEN_SHORT / CLOSE_SHORT / STOP_TRIGGER
    pub order_type: String,     // MARKET / STOP_MARKET / TAKE_PROFIT / TRAILING_STOP
    pub qty: Decimal,
    pub fill_price: Decimal,
    pub notional_usd: Decimal,
    pub fee_usd: Decimal,
    pub status: String,         // FILLED / NEW_TRIGGER / CANCELED
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct DbPositionRow {
    pub symbol: String,
    pub side: String,           // LONG / SHORT
    pub qty: Decimal,
    pub entry_price: Decimal,
    pub entry_order_id: String,
    pub stop_order_id: String,
    pub initial_stop: Decimal,
    pub active_stop: Decimal,
    pub take_profit: Decimal,
    pub highest_mark: Decimal,
    pub lowest_mark: Decimal,
    pub last_mark_price: Decimal,
    pub unrealized_pnl: Decimal,
    pub entry_fee: Decimal,
    pub bars_held: usize,
    pub opened_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct DbTradeRow {
    pub trade_id: i64,
    pub symbol: String,
    pub side: String,
    pub entry_order_id: String,
    pub exit_order_id: String,
    pub qty: Decimal,
    pub entry_price: Decimal,
    pub exit_price: Decimal,
    pub gross_pnl: Decimal,
    pub total_fees: Decimal,
    pub net_pnl: Decimal,
    pub r_multiple: Decimal,
    pub exit_reason: String,
    pub equity_after: Decimal,
    pub opened_at: String,
    pub closed_at: String,
}

#[derive(Debug, Clone)]
pub struct DbAccountState {
    pub initial_equity: Decimal,
    pub cash_balance: Decimal,
    pub unrealized_pnl: Decimal,
    pub total_equity: Decimal,
    pub peak_equity: Decimal,
    pub total_trades: usize,
    pub win_trades: usize,
    pub loss_trades: usize,
    pub total_fees_paid: Decimal,
    pub updated_at: String,
}

pub struct TradingDatabase {
    conn: Connection,
}

impl TradingDatabase {
    pub fn open<P: AsRef<Path>>(db_path: P, initial_equity: Decimal) -> Result<Self> {
        if let Some(parent) = db_path.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(db_path)?;
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA busy_timeout = 5000;",
        )?;

        let db = Self { conn };
        db.init_schema(initial_equity)?;
        Ok(db)
    }

    fn init_schema(&self, initial_equity: Decimal) -> Result<()> {
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS api_quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                bid_price TEXT NOT NULL,
                ask_price TEXT NOT NULL,
                last_price TEXT NOT NULL,
                spread_bps TEXT NOT NULL,
                source_api TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                action TEXT NOT NULL,
                order_type TEXT NOT NULL,
                qty TEXT NOT NULL,
                fill_price TEXT NOT NULL,
                notional_usd TEXT NOT NULL,
                fee_usd TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS positions (
                symbol TEXT PRIMARY KEY,
                side TEXT NOT NULL,
                qty TEXT NOT NULL,
                entry_price TEXT NOT NULL,
                entry_order_id TEXT NOT NULL,
                stop_order_id TEXT NOT NULL,
                initial_stop TEXT NOT NULL,
                active_stop TEXT NOT NULL,
                take_profit TEXT NOT NULL,
                highest_mark TEXT NOT NULL,
                lowest_mark TEXT NOT NULL,
                last_mark_price TEXT NOT NULL,
                unrealized_pnl TEXT NOT NULL,
                entry_fee TEXT NOT NULL,
                bars_held INTEGER NOT NULL,
                opened_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS trades (
                trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_order_id TEXT NOT NULL,
                exit_order_id TEXT NOT NULL,
                qty TEXT NOT NULL,
                entry_price TEXT NOT NULL,
                exit_price TEXT NOT NULL,
                gross_pnl TEXT NOT NULL,
                total_fees TEXT NOT NULL,
                net_pnl TEXT NOT NULL,
                r_multiple TEXT NOT NULL,
                exit_reason TEXT NOT NULL,
                equity_after TEXT NOT NULL,
                opened_at TEXT NOT NULL,
                closed_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS account_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                initial_equity TEXT NOT NULL,
                cash_balance TEXT NOT NULL,
                unrealized_pnl TEXT NOT NULL,
                total_equity TEXT NOT NULL,
                peak_equity TEXT NOT NULL,
                total_trades INTEGER NOT NULL,
                win_trades INTEGER NOT NULL,
                loss_trades INTEGER NOT NULL,
                total_fees_paid TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS strategy_optimizations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                optimized_at TEXT NOT NULL,
                quotes_analyzed INTEGER NOT NULL,
                trades_analyzed INTEGER NOT NULL,
                pruned_symbols TEXT NOT NULL,
                core_alpha_symbols TEXT NOT NULL,
                old_net_pnl TEXT NOT NULL,
                new_net_pnl TEXT NOT NULL,
                old_max_dd TEXT NOT NULL,
                new_max_dd TEXT NOT NULL,
                old_sharpe TEXT NOT NULL,
                new_sharpe TEXT NOT NULL,
                optimization_notes TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS black_swan_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                detected_at TEXT NOT NULL,
                severity TEXT NOT NULL,
                trigger_source TEXT NOT NULL,
                btc_drop_pct TEXT NOT NULL,
                bnb_drop_pct TEXT NOT NULL,
                avg_spread_bps TEXT NOT NULL,
                panic_breadth_pct TEXT NOT NULL,
                action_taken TEXT NOT NULL,
                crisis_short_symbol TEXT NOT NULL,
                details TEXT NOT NULL
            );",
        )?;

        let count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM account_state", [], |r| r.get(0))?;
        if count == 0 {
            let eq_str = initial_equity.to_string();
            let now = Utc::now().to_rfc3339();
            self.conn.execute(
                "INSERT INTO account_state (
                    id, initial_equity, cash_balance, unrealized_pnl, total_equity,
                    peak_equity, total_trades, win_trades, loss_trades, total_fees_paid, updated_at
                ) VALUES (1, ?1, ?1, '0', ?1, ?1, 0, 0, 0, '0', ?2)",
                params![eq_str, now],
            )?;
        }

        Ok(())
    }

    pub fn insert_api_quote(&self, q: &ApiQuoteRow) -> Result<()> {
        self.conn.execute(
            "INSERT INTO api_quotes (symbol, bid_price, ask_price, last_price, spread_bps, source_api, fetched_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                q.symbol,
                q.bid_price.to_string(),
                q.ask_price.to_string(),
                q.last_price.to_string(),
                format!("{:.2}", q.spread_bps),
                q.source_api,
                q.fetched_at,
            ],
        )?;
        Ok(())
    }

    pub fn insert_order(&self, o: &DbOrderRow) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO orders (
                order_id, symbol, side, action, order_type, qty,
                fill_price, notional_usd, fee_usd, status, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                o.order_id,
                o.symbol,
                o.side,
                o.action,
                o.order_type,
                o.qty.to_string(),
                o.fill_price.to_string(),
                format!("{:.4}", o.notional_usd),
                format!("{:.4}", o.fee_usd),
                o.status,
                o.created_at,
            ],
        )?;
        Ok(())
    }

    pub fn update_order_status(&self, order_id: &str, new_status: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE orders SET status = ?1 WHERE order_id = ?2",
            params![new_status, order_id],
        )?;
        Ok(())
    }

    pub fn upsert_position(&self, p: &DbPositionRow) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO positions (
                symbol, side, qty, entry_price, entry_order_id, stop_order_id,
                initial_stop, active_stop, take_profit, highest_mark, lowest_mark,
                last_mark_price, unrealized_pnl, entry_fee, bars_held, opened_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            params![
                p.symbol,
                p.side,
                p.qty.to_string(),
                p.entry_price.to_string(),
                p.entry_order_id,
                p.stop_order_id,
                p.initial_stop.to_string(),
                p.active_stop.to_string(),
                p.take_profit.to_string(),
                p.highest_mark.to_string(),
                p.lowest_mark.to_string(),
                p.last_mark_price.to_string(),
                format!("{:.4}", p.unrealized_pnl),
                format!("{:.4}", p.entry_fee),
                p.bars_held as i64,
                p.opened_at,
                p.updated_at,
            ],
        )?;
        Ok(())
    }

    pub fn remove_position(&self, symbol: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM positions WHERE symbol = ?1",
            params![symbol],
        )?;
        Ok(())
    }

    pub fn list_positions(&self) -> Result<Vec<DbPositionRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT symbol, side, qty, entry_price, entry_order_id, stop_order_id,
                    initial_stop, active_stop, take_profit, highest_mark, lowest_mark,
                    last_mark_price, unrealized_pnl, entry_fee, bars_held, opened_at, updated_at
             FROM positions ORDER BY opened_at ASC",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(DbPositionRow {
                symbol: r.get(0)?,
                side: r.get(1)?,
                qty: parse_d(&r.get::<_, String>(2)?),
                entry_price: parse_d(&r.get::<_, String>(3)?),
                entry_order_id: r.get(4)?,
                stop_order_id: r.get(5)?,
                initial_stop: parse_d(&r.get::<_, String>(6)?),
                active_stop: parse_d(&r.get::<_, String>(7)?),
                take_profit: parse_d(&r.get::<_, String>(8)?),
                highest_mark: parse_d(&r.get::<_, String>(9)?),
                lowest_mark: parse_d(&r.get::<_, String>(10)?),
                last_mark_price: parse_d(&r.get::<_, String>(11)?),
                unrealized_pnl: parse_d(&r.get::<_, String>(12)?),
                entry_fee: parse_d(&r.get::<_, String>(13)?),
                bars_held: r.get::<_, i64>(14)? as usize,
                opened_at: r.get(15)?,
                updated_at: r.get(16)?,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert_closed_trade(
        &self,
        symbol: &str,
        side: &str,
        entry_order_id: &str,
        exit_order_id: &str,
        qty: Decimal,
        entry_price: Decimal,
        exit_price: Decimal,
        gross_pnl: Decimal,
        total_fees: Decimal,
        net_pnl: Decimal,
        r_multiple: Decimal,
        exit_reason: &str,
        equity_after: Decimal,
        opened_at: &str,
        closed_at: &str,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO trades (
                symbol, side, entry_order_id, exit_order_id, qty, entry_price, exit_price,
                gross_pnl, total_fees, net_pnl, r_multiple, exit_reason, equity_after, opened_at, closed_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
            params![
                symbol,
                side,
                entry_order_id,
                exit_order_id,
                qty.to_string(),
                entry_price.to_string(),
                exit_price.to_string(),
                format!("{:.4}", gross_pnl),
                format!("{:.4}", total_fees),
                format!("{:.4}", net_pnl),
                format!("{:.2}", r_multiple),
                exit_reason,
                format!("{:.2}", equity_after),
                opened_at,
                closed_at,
            ],
        )?;
        Ok(())
    }

    pub fn get_account_state(&self) -> Result<DbAccountState> {
        self.conn.query_row(
            "SELECT initial_equity, cash_balance, unrealized_pnl, total_equity, peak_equity,
                    total_trades, win_trades, loss_trades, total_fees_paid, updated_at
             FROM account_state WHERE id = 1",
            [],
            |r| {
                Ok(DbAccountState {
                    initial_equity: parse_d(&r.get::<_, String>(0)?),
                    cash_balance: parse_d(&r.get::<_, String>(1)?),
                    unrealized_pnl: parse_d(&r.get::<_, String>(2)?),
                    total_equity: parse_d(&r.get::<_, String>(3)?),
                    peak_equity: parse_d(&r.get::<_, String>(4)?),
                    total_trades: r.get::<_, i64>(5)? as usize,
                    win_trades: r.get::<_, i64>(6)? as usize,
                    loss_trades: r.get::<_, i64>(7)? as usize,
                    total_fees_paid: parse_d(&r.get::<_, String>(8)?),
                    updated_at: r.get(9)?,
                })
            },
        ).map_err(Into::into)
    }

    pub fn update_account_state(&self, st: &DbAccountState) -> Result<()> {
        self.conn.execute(
            "UPDATE account_state SET
                cash_balance = ?1,
                unrealized_pnl = ?2,
                total_equity = ?3,
                peak_equity = ?4,
                total_trades = ?5,
                win_trades = ?6,
                loss_trades = ?7,
                total_fees_paid = ?8,
                updated_at = ?9
             WHERE id = 1",
            params![
                format!("{:.4}", st.cash_balance),
                format!("{:.4}", st.unrealized_pnl),
                format!("{:.4}", st.total_equity),
                format!("{:.4}", st.peak_equity),
                st.total_trades as i64,
                st.win_trades as i64,
                st.loss_trades as i64,
                format!("{:.4}", st.total_fees_paid),
                st.updated_at,
            ],
        )?;
        Ok(())
    }

    pub fn list_recent_trades(&self, limit: usize) -> Result<Vec<DbTradeRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT trade_id, symbol, side, entry_order_id, exit_order_id, qty,
                    entry_price, exit_price, gross_pnl, total_fees, net_pnl,
                    r_multiple, exit_reason, equity_after, opened_at, closed_at
             FROM trades ORDER BY trade_id DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |r| {
            Ok(DbTradeRow {
                trade_id: r.get(0)?,
                symbol: r.get(1)?,
                side: r.get(2)?,
                entry_order_id: r.get(3)?,
                exit_order_id: r.get(4)?,
                qty: parse_d(&r.get::<_, String>(5)?),
                entry_price: parse_d(&r.get::<_, String>(6)?),
                exit_price: parse_d(&r.get::<_, String>(7)?),
                gross_pnl: parse_d(&r.get::<_, String>(8)?),
                total_fees: parse_d(&r.get::<_, String>(9)?),
                net_pnl: parse_d(&r.get::<_, String>(10)?),
                r_multiple: parse_d(&r.get::<_, String>(11)?),
                exit_reason: r.get(12)?,
                equity_after: parse_d(&r.get::<_, String>(13)?),
                opened_at: r.get(14)?,
                closed_at: r.get(15)?,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        out.reverse();
        Ok(out)
    }

    pub fn list_recent_orders(&self, limit: usize) -> Result<Vec<DbOrderRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT order_id, symbol, side, action, order_type, qty,
                    fill_price, notional_usd, fee_usd, status, created_at
             FROM orders ORDER BY rowid DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |r| {
            Ok(DbOrderRow {
                order_id: r.get(0)?,
                symbol: r.get(1)?,
                side: r.get(2)?,
                action: r.get(3)?,
                order_type: r.get(4)?,
                qty: parse_d(&r.get::<_, String>(5)?),
                fill_price: parse_d(&r.get::<_, String>(6)?),
                notional_usd: parse_d(&r.get::<_, String>(7)?),
                fee_usd: parse_d(&r.get::<_, String>(8)?),
                status: r.get(9)?,
                created_at: r.get(10)?,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        out.reverse();
        Ok(out)
    }

    pub fn count_table_rows(&self) -> Result<(i64, i64, i64, i64)> {
        let q_cnt: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM api_quotes", [], |r| r.get(0))?;
        let o_cnt: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM orders", [], |r| r.get(0))?;
        let p_cnt: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM positions", [], |r| r.get(0))?;
        let t_cnt: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM trades", [], |r| r.get(0))?;
        Ok((q_cnt, o_cnt, p_cnt, t_cnt))
    }

    /// 从 `api_quotes` 表统计各币种真实 API 盘口平均买卖点差 (bps)
    pub fn get_symbol_avg_spread_bps(&self) -> Result< std::collections::HashMap<String, Decimal>> {
        let mut stmt = self.conn.prepare(
            "SELECT symbol, AVG(CAST(spread_bps AS REAL)) FROM api_quotes GROUP BY symbol",
        )?;
        let rows = stmt.query_map([], |r| {
            let sym: String = r.get(0)?;
            let avg_f: f64 = r.get(1)?;
            Ok((sym, Decimal::from_str(&format!("{:.2}", avg_f)).unwrap_or(dec!(0))))
        })?;
        let mut map = std::collections::HashMap::new();
        for r in rows {
            let (k, v) = r?;
            map.insert(k, v);
        }
        Ok(map)
    }

    /// 从 `trades` 表统计各币种历史闭环表现: (总单数, 胜单数, 累计手续费, 累计净盈亏)
    pub fn get_symbol_trade_stats(
        &self,
    ) -> Result<std::collections::HashMap<String, (usize, usize, Decimal, Decimal)>> {
        let mut stmt = self.conn.prepare(
            "SELECT symbol,
                    COUNT(*),
                    SUM(CASE WHEN CAST(net_pnl AS REAL) > 0 THEN 1 ELSE 0 END),
                    SUM(CAST(total_fees AS REAL)),
                    SUM(CAST(net_pnl AS REAL))
             FROM trades GROUP BY symbol",
        )?;
        let rows = stmt.query_map([], |r| {
            let sym: String = r.get(0)?;
            let cnt: i64 = r.get(1)?;
            let wins: i64 = r.get(2)?;
            let fees: f64 = r.get(3)?;
            let pnl: f64 = r.get(4)?;
            Ok((
                sym,
                (
                    cnt as usize,
                    wins as usize,
                    Decimal::from_str(&format!("{:.4}", fees)).unwrap_or(dec!(0)),
                    Decimal::from_str(&format!("{:.4}", pnl)).unwrap_or(dec!(0)),
                ),
            ))
        })?;
        let mut map = std::collections::HashMap::new();
        for r in rows {
            let (k, v) = r?;
            map.insert(k, v);
        }
        Ok(map)
    }

    /// 检查某币种是否在最近 `cooldown_secs` 秒内刚刚平仓（防止平仓后同秒原地重开刷手续费）
    pub fn is_symbol_in_cooldown(&self, symbol: &str, cooldown_secs: i64) -> Result<bool> {
        let mut stmt = self.conn.prepare(
            "SELECT closed_at FROM trades WHERE symbol = ?1 ORDER BY trade_id DESC LIMIT 1",
        )?;
        let mut rows = stmt.query(params![symbol])?;
        if let Some(row) = rows.next()? {
            let closed_str: String = row.get(0)?;
            if let Ok(closed_dt) = chrono::DateTime::parse_from_rfc3339(&closed_str) {
                let elapsed = Utc::now()
                    .signed_duration_since(closed_dt.with_timezone(&Utc))
                    .num_seconds();
                if elapsed >= 0 && elapsed < cooldown_secs {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert_strategy_optimization(
        &self,
        quotes_analyzed: i64,
        trades_analyzed: i64,
        pruned_symbols: &str,
        core_alpha_symbols: &str,
        old_net_pnl: Decimal,
        new_net_pnl: Decimal,
        old_max_dd: Decimal,
        new_max_dd: Decimal,
        old_sharpe: f64,
        new_sharpe: f64,
        notes: &str,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO strategy_optimizations (
                optimized_at, quotes_analyzed, trades_analyzed, pruned_symbols, core_alpha_symbols,
                old_net_pnl, new_net_pnl, old_max_dd, new_max_dd, old_sharpe, new_sharpe, optimization_notes
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                Utc::now().to_rfc3339(),
                quotes_analyzed,
                trades_analyzed,
                pruned_symbols,
                core_alpha_symbols,
                format!("{:+.2}", old_net_pnl),
                format!("{:+.2}", new_net_pnl),
                format!("{:.2}%", old_max_dd),
                format!("{:.2}%", new_max_dd),
                format!("{:.2}", old_sharpe),
                format!("{:.2}", new_sharpe),
                notes,
            ],
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert_black_swan_event(
        &self,
        severity: &str,
        trigger_source: &str,
        btc_drop_pct: Decimal,
        bnb_drop_pct: Decimal,
        avg_spread_bps: Decimal,
        panic_breadth_pct: Decimal,
        action_taken: &str,
        crisis_short_symbol: &str,
        details: &str,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO black_swan_events (
                detected_at, severity, trigger_source, btc_drop_pct, bnb_drop_pct,
                avg_spread_bps, panic_breadth_pct, action_taken, crisis_short_symbol, details
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                Utc::now().to_rfc3339(),
                severity,
                trigger_source,
                format!("{:+.2}%", btc_drop_pct),
                format!("{:+.2}%", bnb_drop_pct),
                format!("{:.2}", avg_spread_bps),
                format!("{:.0}%", panic_breadth_pct),
                action_taken,
                crisis_short_symbol,
                details,
            ],
        )?;
        Ok(())
    }
}

fn parse_d(s: &str) -> Decimal {
    Decimal::from_str(s.trim()).unwrap_or(dec!(0))
}
