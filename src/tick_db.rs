//! 秒级 Tick 数据库 (`data/ticks_1s.db`)：回放压测 Harness 的行情固化层。
//!
//! 与生产账本 `data/coin_ai.db` 完全隔离，paper-db 守护进程永不触碰本库。
//! 表结构按多币种设计（当前默认只固化 BTCUSDT，后续一条命令即可扩展币种）。

use anyhow::Result;
use rusqlite::{params, Connection};
use std::path::Path;

/// 一根 1s K 线（价格用 f64 存储：仅用于回放查价与滑点测量，进 Decimal 结算时才转换）
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TickRow {
    pub ts: i64, // 毫秒时间戳（该秒的 openTime）
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

/// 一条聚合逐笔成交（Phase 4 亚秒级深钻用）
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AggTradeRow {
    pub ts: i64,
    pub price: f64,
    pub qty: f64,
}

/// 币种数据覆盖情况统计
#[derive(Debug, Clone)]
pub struct CoverageStats {
    pub symbol: String,
    pub rows: i64,
    pub min_ts: Option<i64>,
    pub max_ts: Option<i64>,
    /// [min_ts, max_ts] 区间内缺失的秒数（= 期望秒数 - 实际行数）
    pub missing_seconds: i64,
}

pub struct TickDatabase {
    conn: Connection,
}

impl TickDatabase {
    pub fn open<P: AsRef<Path>>(db_path: P) -> Result<Self> {
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
        db.init_schema()?;
        Ok(db)
    }

    fn init_schema(&self) -> Result<()> {
        // WITHOUT ROWID：按复合主键 (symbol, ts) 物理有序存储，区间扫描最快且省去 rowid 索引
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS ticks_1s (
                symbol TEXT NOT NULL,
                ts INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                PRIMARY KEY (symbol, ts)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS agg_trades (
                symbol TEXT NOT NULL,
                ts INTEGER NOT NULL,
                price REAL NOT NULL,
                qty REAL NOT NULL,
                PRIMARY KEY (symbol, ts)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS kv_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );",
        )?;
        Ok(())
    }

    /// 某币种已固化的最新时间戳（断点续传起点）
    pub fn max_ts(&self, symbol: &str) -> Result<Option<i64>> {
        let mut stmt = self
            .conn
            .prepare("SELECT MAX(ts) FROM ticks_1s WHERE symbol = ?1")?;
        let mut rows = stmt.query(params![symbol])?;
        match rows.next()? {
            Some(r) => Ok(r.get::<_, Option<i64>>(0)?),
            None => Ok(None),
        }
    }

    pub fn min_ts(&self, symbol: &str) -> Result<Option<i64>> {
        let mut stmt = self
            .conn
            .prepare("SELECT MIN(ts) FROM ticks_1s WHERE symbol = ?1")?;
        let mut rows = stmt.query(params![symbol])?;
        match rows.next()? {
            Some(r) => Ok(r.get::<_, Option<i64>>(0)?),
            None => Ok(None),
        }
    }

    pub fn count(&self, symbol: &str) -> Result<i64> {
        let mut stmt = self
            .conn
            .prepare("SELECT COUNT(*) FROM ticks_1s WHERE symbol = ?1")?;
        let mut rows = stmt.query(params![symbol])?;
        Ok(rows
            .next()?
            .map(|r| r.get::<_, i64>(0).unwrap_or(0))
            .unwrap_or(0))
    }

    /// 批量写入 1s K 线（同 (symbol, ts) 覆盖旧值），返回写入行数。
    /// 每 1 万行一个事务，兼顾写入吞吐与断点安全性。
    pub fn upsert_ticks(&mut self, symbol: &str, rows: &[TickRow]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let mut written = 0usize;
        for chunk in rows.chunks(10_000) {
            let tx = self.conn.transaction()?;
            {
                let mut stmt =
                    tx.prepare("INSERT OR REPLACE INTO ticks_1s (symbol, ts, open, high, low, close, volume) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)")?;
                for r in chunk {
                    stmt.execute(params![
                        symbol,
                        r.ts,
                        r.open,
                        r.high,
                        r.low,
                        r.close,
                        r.volume
                    ])?;
                }
            }
            tx.commit()?;
            written += chunk.len();
        }
        Ok(written)
    }

    /// 决策时刻 t 的盘口：取 <= t 的最近一根 tick（轮询看到的报价）
    pub fn quote_at_or_before(&self, symbol: &str, ts: i64) -> Result<Option<TickRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT ts, open, high, low, close, volume FROM ticks_1s WHERE symbol = ?1 AND ts <= ?2 ORDER BY ts DESC LIMIT 1",
        )?;
        let mut rows = stmt.query(params![symbol, ts])?;
        Ok(rows.next()?.map(|r| self.row_to_tick(r)).transpose()?)
    }

    /// t+Δ 落地时刻的成交价：取 >= ts 的最近一根 tick（延迟后的真实可成交价）
    pub fn quote_at_or_after(&self, symbol: &str, ts: i64) -> Result<Option<TickRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT ts, open, high, low, close, volume FROM ticks_1s WHERE symbol = ?1 AND ts >= ?2 ORDER BY ts ASC LIMIT 1",
        )?;
        let mut rows = stmt.query(params![symbol, ts])?;
        Ok(rows.next()?.map(|r| self.row_to_tick(r)).transpose()?)
    }

    /// 区间拉取（闭区间 [t0, t1]，按 ts 升序）
    pub fn ticks_range(&self, symbol: &str, t0: i64, t1: i64) -> Result<Vec<TickRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT ts, open, high, low, close, volume FROM ticks_1s WHERE symbol = ?1 AND ts BETWEEN ?2 AND ?3 ORDER BY ts ASC",
        )?;
        let rows = stmt.query_map(params![symbol, t0, t1], |r| {
            Ok(TickRow {
                ts: r.get(0)?,
                open: r.get(1)?,
                high: r.get(2)?,
                low: r.get(3)?,
                close: r.get(4)?,
                volume: r.get(5)?,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    /// 覆盖情况统计（行数、时间范围、缺失秒数）
    pub fn coverage_stats(&self, symbol: &str) -> Result<CoverageStats> {
        let min = self.min_ts(symbol)?;
        let max = self.max_ts(symbol)?;
        let rows = self.count(symbol)?;
        let missing_seconds = match (min, max) {
            (Some(lo), Some(hi)) if hi > lo => {
                let expected = (hi - lo) / 1000 + 1;
                (expected - rows).max(0)
            }
            _ => 0,
        };
        Ok(CoverageStats {
            symbol: symbol.to_string(),
            rows,
            min_ts: min,
            max_ts: max,
            missing_seconds,
        })
    }

    /// 删除某币种全部 tick 数据（--force 重下时用）
    pub fn delete_symbol(&mut self, symbol: &str) -> Result<usize> {
        let n = self
            .conn
            .execute("DELETE FROM ticks_1s WHERE symbol = ?1", params![symbol])?;
        Ok(n)
    }

    // ---- agg_trades（Phase 4 亚秒级深钻）----

    pub fn upsert_agg_trades(&mut self, symbol: &str, rows: &[AggTradeRow]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let mut written = 0usize;
        for chunk in rows.chunks(10_000) {
            let tx = self.conn.transaction()?;
            {
                let mut stmt = tx
                    .prepare("INSERT OR REPLACE INTO agg_trades (symbol, ts, price, qty) VALUES (?1, ?2, ?3, ?4)")?;
                for r in chunk {
                    stmt.execute(params![symbol, r.ts, r.price, r.qty])?;
                }
            }
            tx.commit()?;
            written += chunk.len();
        }
        Ok(written)
    }

    pub fn agg_range(&self, symbol: &str, t0: i64, t1: i64) -> Result<Vec<AggTradeRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT ts, price, qty FROM agg_trades WHERE symbol = ?1 AND ts BETWEEN ?2 AND ?3 ORDER BY ts ASC",
        )?;
        let rows = stmt.query_map(params![symbol, t0, t1], |r| {
            Ok(AggTradeRow {
                ts: r.get(0)?,
                price: r.get(1)?,
                qty: r.get(2)?,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    fn row_to_tick(&self, r: &rusqlite::Row<'_>) -> Result<TickRow> {
        Ok(TickRow {
            ts: r.get(0)?,
            open: r.get(1)?,
            high: r.get(2)?,
            low: r.get(3)?,
            close: r.get(4)?,
            volume: r.get(5)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_db(name: &str) -> TickDatabase {
        let dir = std::env::temp_dir().join(format!("coin_ai_tick_test_{}", name));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        TickDatabase::open(dir.join("ticks_1s.db")).unwrap()
    }

    #[test]
    fn test_upsert_and_range_query() {
        let mut db = tmp_db("range");
        let rows: Vec<TickRow> = (0..100)
            .map(|i| TickRow {
                ts: i * 1000,
                open: 100.0 + i as f64,
                high: 101.0 + i as f64,
                low: 99.0 + i as f64,
                close: 100.5 + i as f64,
                volume: 1.0,
            })
            .collect();
        assert_eq!(db.upsert_ticks("BTCUSDT", &rows).unwrap(), 100);

        let range = db.ticks_range("BTCUSDT", 10_000, 20_000).unwrap();
        assert_eq!(range.len(), 11); // 闭区间
        assert_eq!(range[0].ts, 10_000);
        assert_eq!(range[10].ts, 20_000);

        // 重复 upsert 覆盖不重复计数
        assert_eq!(db.upsert_ticks("BTCUSDT", &rows[..10]).unwrap(), 10);
        assert_eq!(db.count("BTCUSDT").unwrap(), 100);
    }

    #[test]
    fn test_quote_at_before_after() {
        let mut db = tmp_db("quote");
        let rows: Vec<TickRow> = (0..10)
            .map(|i| TickRow {
                ts: i * 1000,
                open: 100.0,
                high: 100.0,
                low: 100.0,
                close: 100.0 + i as f64,
                volume: 1.0,
            })
            .collect();
        db.upsert_ticks("BTCUSDT", &rows).unwrap();

        // <= 5500 的最近一根是 ts=5000 (close=105)
        assert_eq!(
            db.quote_at_or_before("BTCUSDT", 5500).unwrap().unwrap().close,
            105.0
        );
        // >= 5500 的最近一根是 ts=6000 (close=106)
        assert_eq!(
            db.quote_at_or_after("BTCUSDT", 5500).unwrap().unwrap().close,
            106.0
        );
        // 越界：最早 tick 是 ts=0，故 -100 之前无数据；最晚是 ts=9000，故 9999 之后无数据
        assert!(db.quote_at_or_before("BTCUSDT", -100).unwrap().is_none());
        assert!(db.quote_at_or_after("BTCUSDT", 9999).unwrap().is_none());
    }

    #[test]
    fn test_coverage_and_resume() {
        let mut db = tmp_db("coverage");
        let rows: Vec<TickRow> = (0..50)
            .map(|i| TickRow {
                ts: i * 1000,
                open: 1.0,
                high: 1.0,
                low: 1.0,
                close: 1.0,
                volume: 1.0,
            })
            .collect();
        db.upsert_ticks("BTCUSDT", &rows).unwrap();

        let stats = db.coverage_stats("BTCUSDT").unwrap();
        assert_eq!(stats.rows, 50);
        assert_eq!(stats.min_ts, Some(0));
        assert_eq!(stats.max_ts, Some(49_000));
        assert_eq!(stats.missing_seconds, 0);

        // 断点续传起点 = max_ts + 1000
        assert_eq!(db.max_ts("BTCUSDT").unwrap(), Some(49_000));
    }

    #[test]
    fn test_delete_symbol() {
        let mut db = tmp_db("delete");
        let rows = vec![TickRow {
            ts: 0,
            open: 1.0,
            high: 1.0,
            low: 1.0,
            close: 1.0,
            volume: 1.0,
        }];
        db.upsert_ticks("BTCUSDT", &rows).unwrap();
        assert_eq!(db.delete_symbol("BTCUSDT").unwrap(), 1);
        assert_eq!(db.count("BTCUSDT").unwrap(), 0);
    }
}
