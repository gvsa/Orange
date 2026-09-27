//! 鎖の統計。エクスプローラの `/stats` が出す数字を数える。
//!
//! # 何を数えるか
//!
//! - ブロック 1 からの日数と、平均のブロック間隔
//! - ハッシュレート (難易度と時刻から逆算した推定)
//! - 採掘したことのあるアドレスの数、直近 7 日に採掘した数
//! - 直近 7 日にコインを受け取ったアドレスの数
//! - 取引の数 (採掘の報酬を除く)
//!
//! 残高を持つアドレスの数は UTXO セットから数えるので、ここではなく
//! [`crate::service::NodeHandle::utxo_summary`] が受け持つ。
//!
//! # ハッシュレートの出し方
//!
//! ターゲットは `(2^256 − 1) / 難易度` なので、1 ブロックを見つけるのに
//! 要るハッシュ数の期待値は**難易度そのもの**である。直近 n ブロックの
//! 難易度の和を、その n ブロックにかかった時間で割れば、その間の
//! 平均のハッシュレートになる。1 ブロックだけで割ると運に振り回される
//! ので、幅を持たせる。
//!
//! # 数え直しを避ける
//!
//! 一度数えたところまでを覚えておき、次は新しいブロックの分だけ足す。
//! 覚えた先端が鎖から外れていたら (再編成)、最初から数え直す。
//! 滅多に起きず、起きても数秒で済む。

use crate::service::NodeHandle;
use oag_consensus::lock::Lock;
use oag_consensus::Block;
use oag_primitives::Hash;
use std::collections::{HashSet, VecDeque};

/// 「直近」とみなす期間 (秒)。
pub const RECENT_SECONDS: i64 = 7 * 24 * 60 * 60;
/// ハッシュレートの短い窓 (約 1 時間)。
pub const SHORT_WINDOW: usize = 60;
/// ハッシュレートの長い窓 (約 1 日)。
pub const LONG_WINDOW: usize = 1440;

/// 1 ブロック分の覚え書き。直近の分だけ持つ。
#[derive(Debug, Clone)]
struct Recent {
    time: i64,
    difficulty: u64,
    miners: Vec<Lock>,
    receivers: Vec<Lock>,
}

/// 数えた結果。
#[derive(Debug, Default)]
pub struct Tally {
    /// 数え終えた先端。
    tip: Option<(u64, Hash)>,
    /// ブロック 1 の時刻。
    first_time: Option<i64>,
    /// 先端の時刻。
    tip_time: i64,
    /// 採掘したことのある支払い先。
    miners: HashSet<Lock>,
    /// 取引の数 (コインベースを除く)。
    transactions: u64,
    /// 直近のブロック。古い順。
    recent: VecDeque<Recent>,
}

impl Tally {
    /// 空の集計。
    pub fn new() -> Tally {
        Tally::default()
    }

    /// 次のブロックを足す。**高さの順に渡すこと。**
    ///
    /// ジェネシスは数えない。ジェネシスの時刻は鎖が動き出した時刻ではなく、
    /// 報酬は誰のものでもない (焼却されている)。
    pub fn add(&mut self, block: &Block, hash: Hash) {
        let height = block.header.height;
        self.tip = Some((height, hash));
        if height == 0 {
            return;
        }
        let time = block.header.timestamp;
        if self.first_time.is_none() {
            self.first_time = Some(time);
        }
        self.tip_time = time;

        let mut miners = Vec::new();
        let mut receivers = Vec::new();
        for tx in &block.transactions {
            if tx.is_coinbase() {
                for output in &tx.outputs {
                    miners.push(output.lock.clone());
                    self.miners.insert(output.lock.clone());
                }
            } else {
                self.transactions += 1;
            }
            receivers.extend(tx.outputs.iter().map(|o| o.lock.clone()));
        }
        self.recent.push_back(Recent {
            time,
            difficulty: block.header.difficulty,
            miners,
            receivers,
        });

        // 古いものを捨てる。ハッシュレートの長い窓と 7 日の両方が要らなく
        // なったものだけを捨てる。
        while self.recent.len() > LONG_WINDOW + 1 {
            let front = &self.recent[0];
            if front.time >= time - RECENT_SECONDS {
                break;
            }
            self.recent.pop_front();
        }
    }

    /// 数え終えた先端。
    pub fn tip(&self) -> Option<(u64, Hash)> {
        self.tip
    }

    /// ブロック 1 の時刻。
    pub fn first_time(&self) -> Option<i64> {
        self.first_time
    }

    /// ブロック 1 から先端までの平均の間隔 (秒)。
    pub fn average_interval(&self) -> Option<f64> {
        let (height, _) = self.tip?;
        let first = self.first_time?;
        if height < 2 {
            return None;
        }
        Some((self.tip_time - first) as f64 / (height - 1) as f64)
    }

    /// 直近 `blocks` 個のブロックから推定したハッシュレート (H/s)。
    ///
    /// ブロックが足りない、あるいは時刻が進んでいなければ `None`。
    pub fn hashrate(&self, blocks: usize) -> Option<f64> {
        if blocks == 0 || self.recent.len() < blocks + 1 {
            return None;
        }
        let end = self.recent.len() - 1;
        let start = end - blocks;
        let span = self.recent[end].time - self.recent[start].time;
        if span <= 0 {
            return None;
        }
        let work: u128 = self
            .recent
            .range(start + 1..=end)
            .map(|r| u128::from(r.difficulty))
            .sum();
        Some(work as f64 / span as f64)
    }

    /// 採掘したことのある支払い先の数。
    pub fn miners(&self) -> usize {
        self.miners.len()
    }

    /// 直近 7 日に採掘した支払い先の数。
    pub fn recent_miners(&self) -> usize {
        self.distinct_recent(|r| &r.miners)
    }

    /// 直近 7 日にコインを受け取った支払い先の数。
    pub fn recent_receivers(&self) -> usize {
        self.distinct_recent(|r| &r.receivers)
    }

    /// 取引の数 (コインベースを除く)。
    pub fn transactions(&self) -> u64 {
        self.transactions
    }

    fn distinct_recent(&self, pick: impl Fn(&Recent) -> &Vec<Lock>) -> usize {
        let since = self.tip_time - RECENT_SECONDS;
        let mut seen = HashSet::new();
        for r in self.recent.iter().filter(|r| r.time >= since) {
            seen.extend(pick(r).iter());
        }
        seen.len()
    }
}

/// 鎖の先端まで数え進める。
///
/// 覚えた先端が鎖から外れていれば、最初から数え直す。
pub async fn catch_up(tally: &mut Tally, handle: &NodeHandle) -> Result<(), String> {
    if let Some((height, hash)) = tally.tip() {
        if handle.hash_at_height(height).await? != Some(hash) {
            *tally = Tally::new();
        }
    }
    let target = handle.status().await?.height;
    let from = match tally.tip() {
        Some((height, _)) => height + 1,
        None => 0,
    };
    for height in from..=target {
        // 数えている間に再編成が起きると、途中で引けなくなる。次の呼び出しで
        // 先端の食い違いに気づいて数え直すので、ここでは止めるだけでよい。
        let Some(hash) = handle.hash_at_height(height).await? else {
            break;
        };
        let Some(block) = handle.block(hash).await? else {
            return Err(format!("the body of block {height} is not held"));
        };
        tally.add(&block, hash);
    }
    Ok(())
}

/// ハッシュレートを読みやすい単位にする。
pub fn format_hashrate(rate: f64) -> String {
    const UNITS: [&str; 5] = ["H/s", "kH/s", "MH/s", "GH/s", "TH/s"];
    let mut value = rate;
    let mut unit = 0;
    while value >= 1000.0 && unit < UNITS.len() - 1 {
        value /= 1000.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{value:.0} {}", UNITS[unit])
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oag_consensus::block::BlockHeader;
    use oag_consensus::tx::{OutPoint, TxInput, CURRENT_TX_VERSION};
    use oag_consensus::{Transaction, TxOutput};
    use oag_primitives::{Amount, SecretKey};

    fn lock(seed: u8) -> Lock {
        let secret = SecretKey::from_bytes([seed.max(1); 32]).unwrap();
        Lock::pay_to_pubkey(&secret.public_key())
    }

    fn tx(input: OutPoint, to: u8) -> Transaction {
        Transaction {
            version: CURRENT_TX_VERSION,
            inputs: vec![TxInput::new(input)],
            outputs: vec![TxOutput::new(Amount::ONE_OAG, lock(to))],
            locktime: 0,
        }
    }

    fn block(height: u64, time: i64, difficulty: u64, miner: u8, payments: &[u8]) -> Block {
        let mut transactions = vec![tx(OutPoint::null(), miner)];
        for (n, to) in payments.iter().enumerate() {
            transactions.push(tx(OutPoint::new(Hash::ZERO, n as u32), *to));
        }
        Block {
            header: BlockHeader {
                version: 1,
                prev_hash: Hash::ZERO,
                merkle_root: Hash::ZERO,
                timestamp: time,
                difficulty,
                height,
                nonce: 0,
            },
            transactions,
        }
    }

    #[test]
    fn the_genesis_block_is_not_counted() {
        let mut tally = Tally::new();
        tally.add(&block(0, 1_000, 1, 9, &[]), Hash::ZERO);
        assert_eq!(tally.miners(), 0);
        assert_eq!(tally.first_time(), None);
        assert_eq!(tally.tip(), Some((0, Hash::ZERO)));
    }

    #[test]
    fn miners_payments_and_intervals_are_counted() {
        let mut tally = Tally::new();
        tally.add(&block(0, 0, 1, 9, &[]), Hash::ZERO);
        // 60 秒ごとに 3 ブロック。採掘者は 2 人、送金は 2 件。
        tally.add(&block(1, 10_000, 100, 1, &[]), Hash::ZERO);
        tally.add(&block(2, 10_060, 100, 2, &[3]), Hash::ZERO);
        tally.add(&block(3, 10_120, 100, 1, &[4]), Hash::ZERO);

        assert_eq!(tally.first_time(), Some(10_000));
        assert_eq!(tally.average_interval(), Some(60.0));
        assert_eq!(tally.miners(), 2);
        assert_eq!(tally.recent_miners(), 2);
        // 採掘者 2 人と、受け取った 2 人。
        assert_eq!(tally.recent_receivers(), 4);
        assert_eq!(tally.transactions(), 2);
    }

    #[test]
    fn the_hashrate_is_work_over_time() {
        let mut tally = Tally::new();
        for h in 1..=11u64 {
            tally.add(&block(h, 1_000 + 60 * h as i64, 6_000, 1, &[]), Hash::ZERO);
        }
        // 10 ブロックで 6,000 × 10 のハッシュを 600 秒で。
        assert_eq!(tally.hashrate(10), Some(100.0));
        // ブロックが足りなければ出さない。
        assert_eq!(tally.hashrate(11), None);
        assert_eq!(tally.hashrate(0), None);
    }

    #[test]
    fn a_window_whose_clock_did_not_advance_gives_no_hashrate() {
        let mut tally = Tally::new();
        tally.add(&block(1, 1_000, 10, 1, &[]), Hash::ZERO);
        tally.add(&block(2, 1_000, 10, 1, &[]), Hash::ZERO);
        assert_eq!(tally.hashrate(1), None);
    }

    #[test]
    fn only_the_last_seven_days_count_as_recent() {
        let mut tally = Tally::new();
        tally.add(&block(1, 0, 1, 1, &[]), Hash::ZERO);
        tally.add(&block(2, RECENT_SECONDS + 1, 1, 2, &[]), Hash::ZERO);
        assert_eq!(tally.miners(), 2);
        assert_eq!(tally.recent_miners(), 1);
    }

    #[test]
    fn old_blocks_are_dropped_once_neither_window_needs_them() {
        let mut tally = Tally::new();
        let step = RECENT_SECONDS; // 1 ブロックごとに 7 日進める
        for h in 1..=(LONG_WINDOW as u64 + 10) {
            tally.add(&block(h, h as i64 * step, 1, 1, &[]), Hash::ZERO);
        }
        assert_eq!(tally.recent.len(), LONG_WINDOW + 1);
        assert!(tally.hashrate(LONG_WINDOW).is_some());
    }

    #[test]
    fn hashrates_are_shown_in_a_readable_unit() {
        assert_eq!(format_hashrate(950.0), "950 H/s");
        assert_eq!(format_hashrate(3_106.0), "3.11 kH/s");
        assert_eq!(format_hashrate(2_500_000.0), "2.50 MH/s");
    }
}
