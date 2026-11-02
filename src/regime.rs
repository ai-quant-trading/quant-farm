use crate::indicators::{adx, atr_percentile, atr_series};
use crate::types::{Candle, TradeRecord};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::BTreeMap;

#[derive(Default, Clone)]
struct Bucket {
    n: usize,
    wins: usize,
    net: Decimal,
    r_sum: Decimal,
}

impl Bucket {
    fn add(&mut self, t: &TradeRecord) {
        self.n += 1;
        if t.net_pnl > Decimal::ZERO {
            self.wins += 1;
        }
        self.net += t.net_pnl;
        self.r_sum += t.r_multiple;
    }
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
}

fn bucket_of_atr_pct(p: Decimal) -> &'static str {
    if p < dec!(25) {
        "Q1 极低波动 (0-25%)"
    } else if p < dec!(50) {
        "Q2 中低波动 (25-50%)"
    } else if p < dec!(75) {
        "Q3 中高波动 (50-75%)"
    } else {
        "Q4 极高波动 (75-100%)"
    }
}

fn bucket_of_adx(a: Decimal) -> &'static str {
    if a < dec!(20) {
        "ADX < 20 无趋势/震荡"
    } else if a < dec!(25) {
        "ADX 20-25 趋势萌芽"
    } else if a < dec!(30) {
        "ADX 25-30 趋势成立"
    } else {
        "ADX >= 30 强趋势"
    }
}

fn find_bar_index(candles: &[Candle], ts: i64) -> Option<usize> {
    // 二分查找最后一根 timestamp <= ts 的 K 线
    match candles.binary_search_by_key(&ts, |c| c.timestamp) {
        Ok(i) => Some(i),
        Err(0) => None,
        Err(i) => Some(i - 1),
    }
}

/// 验证「波动率状态 / 趋势强度」是否对单笔期望有稳定影响。
///
/// 方法：对每笔回测成交，回溯其开仓 K 线，计算当时的 ATR/价格 分位（过去 500 根）
/// 与 ADX(14)，再按分组统计净盈亏、胜率、单笔期望，并做前后半段稳定性检验。
pub fn print_regime_attribution(
    trades: &[TradeRecord],
    candles_by_symbol: &std::collections::HashMap<String, Vec<Candle>>,
    lookback: usize,
) {
    if trades.is_empty() {
        println!("  [波动率验证] 无回测成交记录。");
        return;
    }

    // 按开仓时间排序，保证前后半段划分是时间顺序
    let mut list: Vec<&TradeRecord> = trades.iter().collect();
    list.sort_by_key(|t| t.entry_time);

    let mut atr_map: BTreeMap<&'static str, Bucket> = BTreeMap::new();
    let mut adx_map: BTreeMap<&'static str, Bucket> = BTreeMap::new();
    let mut atr_map_first: BTreeMap<&'static str, Bucket> = BTreeMap::new();
    let mut atr_map_second: BTreeMap<&'static str, Bucket> = BTreeMap::new();
    let mid = list.len() / 2;

    let mut matched = 0usize;
    let mut atr_series_cache: std::collections::HashMap<String, Vec<Option<Decimal>>> =
        std::collections::HashMap::new();
    // (ATR分位, 净盈亏, 是否属于前半段)
    let mut samples: Vec<(Decimal, Decimal, bool)> = Vec::new();

    for (order, t) in list.iter().enumerate() {
        let candles = match candles_by_symbol.get(&t.symbol) {
            Some(c) => c,
            None => continue,
        };
        let idx = match find_bar_index(candles, t.entry_time) {
            Some(i) => i,
            None => continue,
        };

        let series = atr_series_cache
            .entry(t.symbol.clone())
            .or_insert_with(|| atr_series(candles, 14));
        let atr_val = atr_percentile(candles, series, idx, lookback);
        let atr_pct = match atr_val {
            Some(p) => p,
            None => continue,
        };
        let adx_val = adx(&candles[..=idx], 14).unwrap_or(Decimal::ZERO);
        matched += 1;
        samples.push((atr_pct, t.net_pnl, order < mid));

        atr_map
            .entry(bucket_of_atr_pct(atr_pct))
            .or_default()
            .add(t);
        adx_map
            .entry(bucket_of_adx(adx_val))
            .or_default()
            .add(t);
        let target = if order < mid {
            &mut atr_map_first
        } else {
            &mut atr_map_second
        };
        target
            .entry(bucket_of_atr_pct(atr_pct))
            .or_default()
            .add(t);
    }

    println!(
        "  [波动率/趋势状态验证] 成功匹配开仓 K 线 {} / {} 笔 (回看窗口 {} 根)",
        matched,
        list.len(),
        lookback
    );
    println!();
    println!("  【按开仓时 ATR/价格 分位分组（过去 {} 根 K 线）】", lookback);
    println!("  波动率分位              | 单数 | 胜率    | 净盈亏      | 单笔期望   | 平均R");
    println!("  -------------------------------------------------------------------------------------");
    let mut atr_rows: Vec<(&str, &Bucket)> = atr_map.iter().map(|(k, v)| (*k, v)).collect();
    atr_rows.sort_by_key(|(k, _)| *k);
    for (k, b) in &atr_rows {
        println!(
            "  {:<22} | {:>4} | {:>5.1}% | {:>+8.2} U | {:>+8.2} U | {:>+5.2} R",
            k, b.n, b.win_rate(), b.net, b.avg(), b.avg_r()
        );
    }

    println!();
    println!("  【按开仓时 ADX(14) 趋势强度分组】");
    println!("  趋势强度                 | 单数 | 胜率    | 净盈亏      | 单笔期望   | 平均R");
    println!("  -------------------------------------------------------------------------------------");
    let mut adx_rows: Vec<(&str, &Bucket)> = adx_map.iter().map(|(k, v)| (*k, v)).collect();
    adx_rows.sort_by_key(|(k, _)| *k);
    for (k, b) in &adx_rows {
        println!(
            "  {:<22} | {:>4} | {:>5.1}% | {:>+8.2} U | {:>+8.2} U | {:>+5.2} R",
            k, b.n, b.win_rate(), b.net, b.avg(), b.avg_r()
        );
    }

    println!();
    println!("  【前后半段稳定性：各波动率分位的单笔期望】");
    println!("  波动率分位              | 前半段 (单数/期望)     | 后半段 (单数/期望)");
    println!("  -------------------------------------------------------------------------------------");
    let mut keys: Vec<&str> = atr_map.keys().copied().collect();
    keys.sort();
    let mut consistent = 0usize;
    let mut total_cmp = 0usize;
    for k in keys {
        let f = atr_map_first.get(k).cloned().unwrap_or_default();
        let s = atr_map_second.get(k).cloned().unwrap_or_default();
        println!(
            "  {:<22} | {:>4} 单 / {:>+8.2} U      | {:>4} 单 / {:>+8.2} U",
            k, f.n, f.avg(), s.n, s.avg()
        );
        if f.n >= 3 && s.n >= 3 {
            total_cmp += 1;
            if (f.avg() > Decimal::ZERO) == (s.avg() > Decimal::ZERO) {
                consistent += 1;
            }
        }
    }

    // 排序检查：单笔期望是否随波动率分位单调变化
    let mut monotonic = true;
    for w in atr_rows.windows(2) {
        if w[0].1.n < 3 || w[1].1.n < 3 {
            continue;
        }
        if w[0].1.avg() > w[1].1.avg() {
            monotonic = false;
            break;
        }
    }

    println!();
    println!(
        "  [稳定性] 前后半段同号的分位: {}/{} | 单笔期望随波动率单调递增: {}",
        consistent,
        total_cmp,
        if monotonic { "是" } else { "否" }
    );

    // ------------------------------------------------------------------
    // 阈值交叉验证：在一个半段上挑最优阈值，拿到另一个半段验证
    // 这样阈值不是用全样本挑的，能真实反映样本外表现。
    // ------------------------------------------------------------------
    println!();
    println!("  【ATR 分位阈值扫描（只做 ATR 分位 >= 阈值的单）】");
    println!("  最低分位 | 前半段(单数/净利)      | 后半段(单数/净利)      | 全样本(单数/净利)");
    println!("  -------------------------------------------------------------------------------------");
    let thresholds = [0u32, 50, 60, 70, 75, 80, 85, 90, 95];
    let mut rows: Vec<(u32, usize, Decimal, usize, Decimal, usize, Decimal)> = Vec::new();
    for thr in thresholds {
        let td = Decimal::from(thr);
        let (mut n1, mut p1, mut n2, mut p2) = (0usize, Decimal::ZERO, 0usize, Decimal::ZERO);
        for (pct, pnl, first) in &samples {
            if *pct < td {
                continue;
            }
            if *first {
                n1 += 1;
                p1 += *pnl;
            } else {
                n2 += 1;
                p2 += *pnl;
            }
        }
        println!(
            "  {:>7}% | {:>4} 单 / {:>+8.2} U | {:>4} 单 / {:>+8.2} U | {:>4} 单 / {:>+8.2} U",
            thr,
            n1,
            p1,
            n2,
            p2,
            n1 + n2,
            p1 + p2
        );
        rows.push((thr, n1, p1, n2, p2, n1 + n2, p1 + p2));
    }

    // 交叉验证：前半段选阈值 -> 后半段检验；反过来再来一次
    let baseline = rows.iter().find(|r| r.0 == 0).copied().unwrap();
    let pick = |use_first: bool| -> Option<(u32, Decimal, Decimal)> {
        let mut best: Option<(u32, Decimal, Decimal)> = None;
        for r in &rows {
            if r.0 == 0 || r.1 < 8 || r.3 < 5 {
                continue;
            }
            let (train, test) = if use_first { (r.2, r.4) } else { (r.4, r.2) };
            if best.is_none() || train > best.unwrap().1 {
                best = Some((r.0, train, test));
            }
        }
        best
    };

    println!();
    println!("  【样本外交叉验证】");
    let mut pass = 0;
    let mut checked = 0;
    for (label, use_first) in [("前半段调参 -> 后半段验证", true), ("后半段调参 -> 前半段验证", false)] {
        if let Some((thr, train, test)) = pick(use_first) {
            let base_test = if use_first { baseline.4 } else { baseline.2 };
            let improved = test > base_test;
            checked += 1;
            if improved {
                pass += 1;
            }
            println!(
                "  * {}: 选中阈值 {}% | 调参段净利 {:+.2} U | 验证段净利 {:+.2} U (不加过滤 {:+.2} U) | {}",
                label,
                thr,
                train,
                test,
                base_test,
                if improved { "样本外改善 PASS" } else { "样本外未改善 FAIL" }
            );
        }
    }

    if checked > 0 && pass == checked {
        println!("  [结论] 高低波动分化在样本外复现，可考虑把「ATR 分位下限」加入策略过滤器（默认关闭，需纸面复核）。");
    } else {
        println!("  [结论] 样本外未能稳定改善，不建议加入波动率过滤器。");
    }
    println!();
}
