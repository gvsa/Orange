//! プール。報酬をいったんノードの鍵で受け取り、掘った分に応じて配る。
//!
//! `--stratum` に `--pool` を足すと、この形になる。ソロとの違いは次の 3 つ
//! である。
//!
//! - ブロックの報酬の受取先が、採掘器のアドレスではなく**プールの鍵**になる。
//! - 採掘器には、ブロックより易しい「シェア」を配る。当たりの手前でも
//!   提出させ、どれだけ掘ったかを数える。
//! - 見つけたブロックの報酬を、直近のシェアの量に応じて分け (PPLNS)、
//!   成熟を待ってからまとめて送る。
//!
//! # 預かり物である
//!
//! 報酬が成熟して送られるまでの間、**採掘者の取り分はプールの鍵の下に
//! ある**。鍵 (`pool.key`) と台帳 (`pool.json`) を失えば、払えなくなる。
//! 運営する人は両方を控えておくこと。
//!
//! # 分け方 (PPLNS)
//!
//! ブロックが見つかったとき、新しい順にシェアの難易度を足していき、
//! その合計が**ブロックの難易度の 2 倍** ([`PPLNS_FACTOR`]) に届くまでの
//! シェアで報酬を分ける。ブロックを見つけた人だけが得をする形にしないため、
//! また、儲かりそうなときだけ来て帰る「プール渡り」を割に合わなくするため
//! である。
//!
//! 窓はメモリに置く。**ノードを再起動すると窓は空になる。** 再起動の後に
//! 見つけたブロックは、再起動の後のシェアだけで分ける。

use crate::service::{NodeEvent, NodeHandle};
use oag_consensus::lock::Lock;
use oag_consensus::params;
use oag_consensus::tx::{OutPoint, TxInput, CURRENT_TX_VERSION};
use oag_consensus::{Decode, Encode, Transaction, TxOutput};
use oag_primitives::amount::MAX_SUPPLY_ATOMIC;
use oag_primitives::{Address, Amount, Hash, Network, SecretKey};
use oag_wallet::build::{Coin, Draft};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::Mutex;

/// 窓の大きさ。ブロックの難易度の何倍分のシェアで分けるか。
pub const PPLNS_FACTOR: u64 = 2;

/// 1 本の支払いに入れる宛先の上限。取引の大きさを抑える。
const MAX_PAYEES: usize = 200;

/// 払ったまま承認されない取引を、何ブロック待ってから出し直すか。
const RESEND_AFTER_BLOCKS: u64 = 10;

/// 台帳に残す支払いの記録の数。
const KEPT_PAYOUTS: usize = 200;

/// シェアの間隔の目安 (秒)。難易度はこれに合わせて上下する。
pub const SHARE_TARGET_SECS: u64 = 15;

/// プールの設定。
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// 運営の取り分 (万分率)。100 で 1 %。
    pub fee_basis_points: u32,
    /// これ以上たまったら払う。
    pub min_payout: Amount,
    /// 支払いをまとめて行う間隔。
    pub payout_interval: Duration,
    /// 運営の取り分の受取先。採掘者と同じように、成熟したら払う。
    ///
    /// 無ければ取り分はプールの鍵の下に残る。`oag-wallet` は種から鍵を
    /// 作る財布で、生の鍵を読めないため、**そのままでは引き出せない。**
    /// 取り分を取るなら必ず渡すこと。
    pub fee_address: Option<Address>,
}

impl Default for PoolConfig {
    /// 取り分 0 %、1 OAG から、10 分ごと。
    fn default() -> PoolConfig {
        PoolConfig {
            fee_basis_points: 0,
            min_payout: Amount::ONE_OAG,
            payout_interval: Duration::from_secs(10 * 60),
            fee_address: None,
        }
    }
}

/// 台帳。`pool.json` に保存する。
///
/// 金額は atomic 単位の整数を**文字列で**持つ。JSON の数は 64 ビットを
/// 超えると処理系によって丸められるためである。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ledger {
    /// どのネットワークの台帳か。取り違えて開かないように確かめる。
    pub network: String,
    /// プールの受取先。
    pub pool_address: String,
    /// 見つけたが、まだ成熟していないブロック。
    pub rounds: Vec<Round>,
    /// 成熟して払えるようになった額。アドレス → atomic。
    pub balances: BTreeMap<String, String>,
    /// これまでに払った額の合計。アドレス → atomic。
    pub paid: BTreeMap<String, String>,
    /// 運営の取り分の合計 (atomic)。
    pub fee_earned: String,
    /// 支払いの記録。新しいものが後ろ。
    pub payouts: Vec<Payout>,
}

/// 見つけたブロック 1 つぶんの分け前。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Round {
    /// ブロックハッシュ。
    pub hash: String,
    /// 高さ。
    pub height: u64,
    /// 報酬の総額 (atomic)。
    pub reward: String,
    /// 分け前。アドレス → atomic。
    pub credits: BTreeMap<String, String>,
    /// 運営の取り分と端数 (atomic)。
    pub fee: String,
}

/// 支払い 1 本。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Payout {
    /// 取引 ID。
    pub txid: String,
    /// 出した時点の先端の高さ。
    pub height: u64,
    /// 取引そのもの (16 進)。承認されなければこれを出し直す。
    pub raw: String,
    /// 使った入力 (`txid:index`)。承認されるまで次の支払いに使わない。
    pub inputs: Vec<String>,
    /// 台帳から引いた額。アドレス → atomic。送金手数料を引く前の額である。
    pub owed: BTreeMap<String, String>,
    /// 承認されたか。
    pub confirmed: bool,
}

/// プール。
pub struct Pool {
    handle: NodeHandle,
    network: Network,
    key: SecretKey,
    lock: Lock,
    address: Address,
    config: PoolConfig,
    path: PathBuf,
    state: Mutex<State>,
}

struct State {
    ledger: Ledger,
    /// 直近のシェア (アドレス, 難易度)。古いものが前。
    window: VecDeque<(String, u64)>,
    window_weight: u128,
    last_payout: Option<Instant>,
}

/// 報酬を分けた結果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Split {
    /// アドレスごとの分け前 (atomic)。
    pub credits: BTreeMap<String, u128>,
    /// 運営の取り分と、割り切れなかった端数 (atomic)。
    pub fee: u128,
}

impl Pool {
    /// 鍵と台帳を開く。無ければ作る。
    ///
    /// 鍵は `<datadir>/pool.key` (16 進の 1 行、`oag-node keygen` と同じ形)、
    /// 台帳は `<datadir>/pool.json` である。
    pub fn open(
        handle: NodeHandle,
        datadir: &Path,
        config: PoolConfig,
    ) -> Result<Arc<Pool>, String> {
        let network = handle.network();
        let key = load_or_create_key(&datadir.join("pool.key"))?;
        let address = Address::from_pubkey(network, &key.public_key());
        let lock = Lock::from_address(&address);

        let path = datadir.join("pool.json");
        let ledger = match std::fs::read(&path) {
            Ok(bytes) => {
                let ledger: Ledger = serde_json::from_slice(&bytes)
                    .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
                if ledger.network != network.to_string() || ledger.pool_address != address.encode()
                {
                    return Err(format!(
                        "{} belongs to {} on {}, not to this pool's key on {network}",
                        path.display(),
                        ledger.pool_address,
                        ledger.network
                    ));
                }
                ledger
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ledger {
                network: network.to_string(),
                pool_address: address.encode(),
                fee_earned: "0".to_string(),
                ..Ledger::default()
            },
            Err(e) => return Err(format!("cannot read {}: {e}", path.display())),
        };

        let pool = Pool {
            handle,
            network,
            key,
            lock,
            address,
            config,
            path,
            state: Mutex::new(State {
                ledger,
                window: VecDeque::new(),
                window_weight: 0,
                last_payout: None,
            }),
        };
        Ok(Arc::new(pool))
    }

    /// 報酬の受取先 (プールの鍵)。
    pub fn address(&self) -> &Address {
        &self.address
    }

    /// 掘る土台の受取先。
    pub fn lock(&self) -> &Lock {
        &self.lock
    }

    /// 設定。
    pub fn config(&self) -> &PoolConfig {
        &self.config
    }

    /// 台帳の写し。
    pub async fn ledger(&self) -> Ledger {
        self.state.lock().await.ledger.clone()
    }

    /// 受け付けたシェアを窓に積む。
    pub async fn record_share(&self, payee: &str, difficulty: u64) {
        let mut state = self.state.lock().await;
        state.window.push_back((payee.to_string(), difficulty));
        state.window_weight += u128::from(difficulty);
    }

    /// ブロックを見つけた。窓のシェアで報酬を分け、成熟を待つ列に入れる。
    pub async fn block_found(
        &self,
        hash: Hash,
        height: u64,
        reward: Amount,
        block_difficulty: u64,
    ) -> Result<(), String> {
        let mut state = self.state.lock().await;
        let span = u128::from(block_difficulty.max(1)) * u128::from(PPLNS_FACTOR);
        let shares: Vec<(String, u64)> = state.window.iter().cloned().collect();
        let split = pplns_split(
            &shares,
            span,
            reward.to_atomic(),
            self.config.fee_basis_points,
        );

        // 窓は、次のブロックで使いうる分だけ残す。難易度が 4 倍に跳ねても
        // 足りるように、少し多めに持つ。
        let keep = span.saturating_mul(4);
        while state.window_weight > keep {
            let Some((_, d)) = state.window.pop_front() else {
                break;
            };
            state.window_weight -= u128::from(d);
        }

        let miners = split.credits.len();
        state.ledger.rounds.push(Round {
            hash: hash.to_string(),
            height,
            reward: reward.to_atomic().to_string(),
            credits: to_strings(&split.credits),
            fee: split.fee.to_string(),
        });
        crate::log_mine!(
            "pool: block {height} split among {miners} miners; paid out once it matures \
             ({} blocks)",
            params::COINBASE_MATURITY
        );
        self.save(&state.ledger)
    }

    /// 成熟を確かめ、未承認の支払いを見直し、時期が来ていれば払う。
    ///
    /// 定期的に呼ぶ ([`Pool::spawn`])。試験からも直接呼べる。
    pub async fn maintain(&self) -> Result<(), String> {
        let tip = self.handle.tip_height().await?;
        let mut state = self.state.lock().await;

        self.mature(&mut state, tip).await?;
        self.review_payouts(&mut state, tip).await?;

        let due = state
            .last_payout
            .is_none_or(|at| at.elapsed() >= self.config.payout_interval);
        if due {
            state.last_payout = Some(Instant::now());
            if let Err(e) = self.pay(&mut state, tip).await {
                crate::log_warn!("pool: cannot pay out yet: {e}");
            }
        }
        self.save(&state.ledger)
    }

    /// 先端の動きと時計で [`Pool::maintain`] を回し続ける。
    pub fn spawn(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut events = self.handle.subscribe();
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            loop {
                tokio::select! {
                    event = events.recv() => match event {
                        Ok(NodeEvent::NewTip { .. }) | Err(RecvError::Lagged(_)) => {}
                        Ok(_) => continue,
                        Err(RecvError::Closed) => return,
                    },
                    _ = tick.tick() => {}
                }
                if let Err(e) = self.maintain().await {
                    crate::log_warn!("pool: {e}");
                }
            }
        });
    }

    /// 成熟したブロックの分け前を残高へ移す。先端から外れたものは捨てる。
    async fn mature(&self, state: &mut State, tip: u64) -> Result<(), String> {
        let mut kept = Vec::new();
        for round in std::mem::take(&mut state.ledger.rounds) {
            // コインベースは高さ + COINBASE_MATURITY のブロックから使える。
            if tip + 1 < round.height + params::COINBASE_MATURITY {
                kept.push(round);
                continue;
            }
            let on_chain = self.handle.hash_at_height(round.height).await?;
            if on_chain.map(|h| h.to_string()) != Some(round.hash.clone()) {
                crate::log_warn!(
                    "pool: block {} ({}) left the chain, so its reward is gone",
                    round.height,
                    round.hash
                );
                continue;
            }
            for (address, amount) in &round.credits {
                add(&mut state.ledger.balances, address, parse(amount)?);
            }
            let round_fee = parse(&round.fee)?;
            let fee = parse(&state.ledger.fee_earned)? + round_fee;
            state.ledger.fee_earned = fee.to_string();
            if let Some(to) = &self.config.fee_address {
                add(&mut state.ledger.balances, &to.encode(), round_fee);
            }
        }
        state.ledger.rounds = kept;
        Ok(())
    }

    /// 承認されていない支払いを見直す。
    ///
    /// - mempool にあれば待つ。
    /// - 入力が使われていれば、承認されたとみなす。
    /// - どちらでもなく、しばらく経っていれば出し直す。出し直せなければ、
    ///   **台帳に戻す** (払っていないものを払ったことにしない)。
    async fn review_payouts(&self, state: &mut State, tip: u64) -> Result<(), String> {
        let unspent: HashSet<String> = self
            .handle
            .scan_utxos(vec![self.lock.clone()], usize::MAX)
            .await?
            .iter()
            .map(|u| outpoint_text(&u.outpoint))
            .collect();

        let mut restore = Vec::new();
        for payout in state.ledger.payouts.iter_mut().filter(|p| !p.confirmed) {
            let txid: Hash = payout.txid.parse().map_err(|_| "bad txid in the ledger")?;
            if self.handle.mempool_tx(txid).await?.is_some() {
                continue;
            }
            if !payout.inputs.iter().all(|i| unspent.contains(i)) {
                payout.confirmed = true;
                continue;
            }
            if tip < payout.height + RESEND_AFTER_BLOCKS {
                continue;
            }
            let raw = hex::decode(&payout.raw).map_err(|_| "bad transaction in the ledger")?;
            let tx = Transaction::decode(&raw).map_err(|e| e.to_string())?;
            match self.handle.submit_tx(tx).await {
                Ok(_) => {
                    crate::log_warn!(
                        "pool: payout {} was not mined, so sent it again",
                        payout.txid
                    );
                    payout.height = tip;
                }
                Err(e) => {
                    crate::log_warn!(
                        "pool: payout {} cannot be sent again ({e}); returning it to the balances",
                        payout.txid
                    );
                    restore.push(payout.txid.clone());
                }
            }
        }
        for txid in restore {
            let Some(at) = state.ledger.payouts.iter().position(|p| p.txid == txid) else {
                continue;
            };
            let payout = state.ledger.payouts.remove(at);
            for (address, amount) in &payout.owed {
                add(&mut state.ledger.balances, address, parse(amount)?);
                sub(&mut state.ledger.paid, address, parse(amount)?);
            }
        }
        Ok(())
    }

    /// たまった残高を 1 本の取引で払う。
    async fn pay(&self, state: &mut State, tip: u64) -> Result<(), String> {
        let min = self.config.min_payout.to_atomic();
        let mut payees: Vec<(String, u128)> = state
            .ledger
            .balances
            .iter()
            .filter_map(|(a, v)| parse(v).ok().map(|v| (a.clone(), v)))
            .filter(|(_, v)| *v >= min)
            .collect();
        if payees.is_empty() {
            return Ok(());
        }
        payees.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        payees.truncate(MAX_PAYEES);

        // 承認待ちの支払いが使っている入力は使わない。
        let pending: HashSet<String> = state
            .ledger
            .payouts
            .iter()
            .filter(|p| !p.confirmed)
            .flat_map(|p| p.inputs.iter().cloned())
            .collect();
        let next_height = tip + 1;
        let coins: Vec<Coin> = self
            .handle
            .scan_utxos(vec![self.lock.clone()], usize::MAX)
            .await?
            .into_iter()
            .filter(|u| !pending.contains(&outpoint_text(&u.outpoint)))
            .map(|u| Coin {
                outpoint: u.outpoint,
                output: u.entry.output,
                height: u.entry.height,
                is_coinbase: u.entry.is_coinbase,
            })
            .collect();

        let recipients: Vec<(Lock, u128)> = payees
            .iter()
            .map(|(a, v)| {
                Address::decode_on(self.network, a)
                    .map(|addr| (Lock::from_address(&addr), *v))
                    .map_err(|e| format!("bad address {a} in the ledger: {e}"))
            })
            .collect::<Result<_, _>>()?;
        let plan = plan_payout(
            &coins,
            &recipients,
            &self.lock,
            next_height,
            params::MIN_RELAY_FEE_RATE_PER_BYTE,
        )?;
        let key = self.key.clone();
        let lock = self.lock.clone();
        let tx = oag_wallet::build::sign(&plan.draft, |l| (*l == lock).then(|| key.clone()))
            .map_err(|e| e.to_string())?;
        let raw = hex::encode(tx.encode());
        let inputs = tx
            .inputs
            .iter()
            .map(|i| outpoint_text(&i.prev_out))
            .collect();
        let txid = self.handle.submit_tx(tx).await?;

        let mut owed = BTreeMap::new();
        let mut total = 0u128;
        for (address, amount) in payees.iter().take(plan.paid) {
            sub(&mut state.ledger.balances, address, *amount);
            add(&mut state.ledger.paid, address, *amount);
            owed.insert(address.clone(), amount.to_string());
            total += amount;
        }
        state.ledger.balances.retain(|_, v| v != "0");
        state.ledger.payouts.push(Payout {
            txid: txid.to_string(),
            height: tip,
            raw,
            inputs,
            owed,
            confirmed: false,
        });
        if state.ledger.payouts.len() > KEPT_PAYOUTS {
            let drop = state.ledger.payouts.len() - KEPT_PAYOUTS;
            // 承認待ちは消さない。消すと、その入力を次の支払いで使ってしまう。
            let mut dropped = 0;
            state.ledger.payouts.retain(|p| {
                if dropped < drop && p.confirmed {
                    dropped += 1;
                    false
                } else {
                    true
                }
            });
        }
        crate::log_mine!(
            "pool: paid {} to {} miners in {txid} (fee {})",
            atomic_text(total),
            plan.paid,
            plan.draft.fee
        );
        Ok(())
    }

    /// 台帳を書く。途中で落ちても壊れないよう、別名で書いてから置き換える。
    fn save(&self, ledger: &Ledger) -> Result<(), String> {
        let bytes = serde_json::to_vec_pretty(ledger).map_err(|e| e.to_string())?;
        let temp = self.path.with_extension("json.tmp");
        std::fs::write(&temp, bytes)
            .map_err(|e| format!("cannot write {}: {e}", temp.display()))?;
        std::fs::rename(&temp, &self.path)
            .map_err(|e| format!("cannot replace {}: {e}", self.path.display()))
    }
}

/// 報酬を窓のシェアで分ける (PPLNS)。
///
/// `shares` は古い順。新しい方から `span` に届くまで数え、最後の 1 件は
/// はみ出した分を削って数える。運営の取り分を先に引き、割り切れなかった
/// 端数も運営に回す。**分け前の合計が報酬を超えることは無い。**
pub fn pplns_split(shares: &[(String, u64)], span: u128, reward: u128, fee_bp: u32) -> Split {
    let fee = reward * u128::from(fee_bp.min(10_000)) / 10_000;
    let pot = reward - fee;

    let mut weights: BTreeMap<String, u128> = BTreeMap::new();
    let mut counted = 0u128;
    for (address, difficulty) in shares.iter().rev() {
        if counted >= span {
            break;
        }
        let weight = u128::from(*difficulty).min(span - counted);
        *weights.entry(address.clone()).or_default() += weight;
        counted += weight;
    }

    if counted == 0 {
        // シェアが 1 つも無い (再起動の直後など)。全部を運営に回す。
        return Split {
            credits: BTreeMap::new(),
            fee: reward,
        };
    }
    let mut credits = BTreeMap::new();
    let mut given = 0u128;
    for (address, weight) in weights {
        let amount = pot * weight / counted;
        if amount > 0 {
            credits.insert(address, amount);
            given += amount;
        }
    }
    Split {
        credits,
        fee: reward - given,
    }
}

/// 次のシェアの難易度。
///
/// 前に合わせてから届いたシェアの数と経過時間から、`SHARE_TARGET_SECS` に
/// 1 回になるよう合わせる。1 回に動かすのは 4 倍まで。ブロックの難易度を
/// 超えない。
pub fn next_share_difficulty(current: u64, shares: u32, elapsed: Duration, block: u64) -> u64 {
    let current = current.max(1);
    let elapsed = elapsed.as_secs_f64().max(1.0);
    let ideal = current as f64 * f64::from(shares) * SHARE_TARGET_SECS as f64 / elapsed;
    let low = (current / 4).max(1) as f64;
    let high = current.saturating_mul(4) as f64;
    (ideal.clamp(low, high) as u64).clamp(1, block.max(1))
}

/// 最初に配るシェアの難易度。ブロックの 100 分の 1 から始めて合わせていく。
pub fn initial_share_difficulty(block: u64) -> u64 {
    (block / 100).max(1)
}

/// 支払いの計画。
#[derive(Debug, Clone)]
pub struct PayoutPlan {
    /// 署名する前の取引。
    pub draft: Draft,
    /// 先頭から何件の宛先に払うか。
    pub paid: usize,
}

/// 支払いを組む。送金手数料は、受け取る側から額に比例して引く。
///
/// `recipients` は払う順 (大きい順)。手持ちで払い切れなければ、払える所
/// までにする。おつりはプールに戻す。
pub fn plan_payout(
    coins: &[Coin],
    recipients: &[(Lock, u128)],
    change_to: &Lock,
    next_height: u64,
    fee_rate: Amount,
) -> Result<PayoutPlan, String> {
    let mut usable: Vec<&Coin> = coins
        .iter()
        .filter(|c| c.is_spendable_at(next_height))
        .collect();
    usable.sort_by_key(|c| std::cmp::Reverse(c.output.amount));
    let available: u128 = usable.iter().map(|c| c.output.amount.to_atomic()).sum();

    // 払える所まで。
    let mut count = 0;
    let mut owed = 0u128;
    for (_, amount) in recipients {
        if owed + amount > available {
            break;
        }
        owed += amount;
        count += 1;
    }
    if count == 0 {
        return Err(format!(
            "the pool holds {} that can be spent, which does not cover the first payment",
            atomic_text(available)
        ));
    }
    let recipients = &recipients[..count];

    // 入力を選ぶ。手数料は受取側が持つので、払う額が賄えれば足りる。
    let mut selected = Vec::new();
    let mut total = 0u128;
    for coin in &usable {
        selected.push(*coin);
        total += coin.output.amount.to_atomic();
        if total >= owed {
            break;
        }
    }

    // 大きさを測る。額は符号化の長さが最大になるもので仮置きする。
    // 実際の額で縮むことはあっても、はみ出すことはない。
    let widest = Amount::from_atomic_const(MAX_SUPPLY_ATOMIC);
    let mut outputs: Vec<TxOutput> = recipients
        .iter()
        .map(|(lock, _)| TxOutput::new(widest, lock.clone()))
        .collect();
    outputs.push(TxOutput::new(widest, change_to.clone()));
    let inputs: Vec<TxInput> = selected
        .iter()
        .map(|c| {
            let mut input = TxInput::new(c.outpoint);
            input.signature = vec![0u8; 64];
            input
        })
        .collect();
    let mut tx = Transaction {
        version: CURRENT_TX_VERSION,
        inputs,
        outputs,
        locktime: 0,
    };
    let size = tx.encode().len();
    if size > params::MAX_TX_SIZE {
        return Err(format!("the payout would be too large ({size} bytes)"));
    }
    let fee = fee_rate
        .checked_mul(size as u64)
        .ok_or("the fee overflowed")?
        .to_atomic();

    // 手数料を額に比例して引く。端数は最も大きい宛先 (先頭) が持つ。
    let mut shares: Vec<u128> = recipients.iter().map(|(_, a)| fee * a / owed).collect();
    let assigned: u128 = shares.iter().sum();
    shares[0] += fee - assigned;
    for (i, (_, amount)) in recipients.iter().enumerate() {
        let net = amount
            .checked_sub(shares[i])
            .filter(|n| *n >= params::DUST_THRESHOLD.to_atomic())
            .ok_or("a payment would be below the dust threshold after the fee")?;
        tx.outputs[i].amount = Amount::from_atomic(net).map_err(|e| e.to_string())?;
    }

    // おつり。ダストなら作らず、手数料に回る (プールの負担)。
    let change = total - owed;
    if change >= params::DUST_THRESHOLD.to_atomic() {
        let last = tx.outputs.len() - 1;
        tx.outputs[last].amount = Amount::from_atomic(change).map_err(|e| e.to_string())?;
    } else {
        tx.outputs.pop();
    }
    for input in &mut tx.inputs {
        input.signature.clear();
    }

    let paid_fee = fee
        + if change >= params::DUST_THRESHOLD.to_atomic() {
            0
        } else {
            change
        };
    Ok(PayoutPlan {
        draft: Draft {
            tx,
            spent: selected.iter().map(|c| c.output.clone()).collect(),
            fee: Amount::from_atomic(paid_fee).map_err(|e| e.to_string())?,
            change: Amount::from_atomic(if change >= params::DUST_THRESHOLD.to_atomic() {
                change
            } else {
                0
            })
            .map_err(|e| e.to_string())?,
        },
        paid: count,
    })
}

fn load_or_create_key(path: &Path) -> Result<SecretKey, String> {
    match std::fs::read_to_string(path) {
        Ok(text) => {
            let bytes: [u8; 32] = hex::decode(text.trim())
                .ok()
                .and_then(|b| b.try_into().ok())
                .ok_or_else(|| format!("{} is not a 64-digit hex key", path.display()))?;
            SecretKey::from_bytes(bytes).map_err(|e| format!("{}: {e}", path.display()))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let key = SecretKey::generate();
            let text = format!("{}\n", hex::encode(key.to_bytes()));
            write_private(path, text.as_bytes())?;
            crate::log_warn!(
                "pool: created a new key in {}. It holds the miners' rewards until they are \
                 paid out: back it up",
                path.display()
            );
            Ok(key)
        }
        Err(e) => Err(format!("cannot read {}: {e}", path.display())),
    }
}

#[cfg(unix)]
fn write_private(path: &Path, bytes: &[u8]) -> Result<(), String> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| format!("cannot create {}: {e}", path.display()))?;
    file.write_all(bytes)
        .map_err(|e| format!("cannot write {}: {e}", path.display()))
}

#[cfg(not(unix))]
fn write_private(path: &Path, bytes: &[u8]) -> Result<(), String> {
    std::fs::write(path, bytes).map_err(|e| format!("cannot write {}: {e}", path.display()))
}

fn outpoint_text(outpoint: &OutPoint) -> String {
    format!("{}:{}", outpoint.txid, outpoint.index)
}

fn parse(text: &str) -> Result<u128, String> {
    text.parse()
        .map_err(|_| format!("{text} is not an amount in the pool ledger"))
}

fn to_strings(map: &BTreeMap<String, u128>) -> BTreeMap<String, String> {
    map.iter()
        .map(|(k, v)| (k.clone(), v.to_string()))
        .collect()
}

fn add(map: &mut BTreeMap<String, String>, key: &str, amount: u128) {
    let now = map
        .get(key)
        .and_then(|v| v.parse::<u128>().ok())
        .unwrap_or(0);
    map.insert(key.to_string(), (now + amount).to_string());
}

fn sub(map: &mut BTreeMap<String, String>, key: &str, amount: u128) {
    let now = map
        .get(key)
        .and_then(|v| v.parse::<u128>().ok())
        .unwrap_or(0);
    map.insert(key.to_string(), now.saturating_sub(amount).to_string());
}

fn atomic_text(atomic: u128) -> String {
    Amount::from_atomic(atomic).map_or_else(|_| format!("{atomic} atomic"), |a| format!("{a} OAG"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shares(list: &[(&str, u64)]) -> Vec<(String, u64)> {
        list.iter().map(|(a, d)| (a.to_string(), *d)).collect()
    }

    #[test]
    fn the_last_span_of_shares_splits_the_reward() {
        // 窓は 4。新しい方から a:2, b:2 で満ちる。古い c は数えない。
        let split = pplns_split(&shares(&[("c", 5), ("b", 2), ("a", 2)]), 4, 1_000, 0);
        assert_eq!(split.credits.get("a"), Some(&500));
        assert_eq!(split.credits.get("b"), Some(&500));
        assert_eq!(split.credits.get("c"), None);
        assert_eq!(split.fee, 0);
    }

    #[test]
    fn the_share_at_the_edge_counts_only_up_to_the_span() {
        // 窓 3。a:2 の次の b:5 は 1 だけ数える。
        let split = pplns_split(&shares(&[("b", 5), ("a", 2)]), 3, 900, 0);
        assert_eq!(split.credits.get("a"), Some(&600));
        assert_eq!(split.credits.get("b"), Some(&300));
    }

    #[test]
    fn the_fee_and_the_remainder_go_to_the_operator() {
        // 1 % を先に引く。990 を 3 人で分けると割り切れる。
        let split = pplns_split(&shares(&[("a", 1), ("b", 1), ("c", 1)]), 3, 1_000, 100);
        assert_eq!(split.credits.values().sum::<u128>() + split.fee, 1_000);
        assert_eq!(split.credits.get("a"), Some(&330));
        // 割り切れないときも、合計は報酬を超えない。
        let split = pplns_split(&shares(&[("a", 1), ("b", 1), ("c", 1)]), 3, 1_001, 0);
        assert_eq!(split.credits.values().sum::<u128>() + split.fee, 1_001);
        assert!(split.fee <= 2);
    }

    #[test]
    fn with_no_shares_nothing_is_credited() {
        let split = pplns_split(&[], 10, 1_000, 0);
        assert!(split.credits.is_empty());
        assert_eq!(split.fee, 1_000);
    }

    #[test]
    fn share_difficulty_follows_the_rate_but_moves_at_most_four_times() {
        // 60 秒で 4 本 (15 秒に 1 本) なら動かない。
        assert_eq!(
            next_share_difficulty(1_000, 4, Duration::from_secs(60), 1 << 40),
            1_000
        );
        // 60 秒で 8 本なら 2 倍。
        assert_eq!(
            next_share_difficulty(1_000, 8, Duration::from_secs(60), 1 << 40),
            2_000
        );
        // 1 秒で 100 本でも 4 倍まで。
        assert_eq!(
            next_share_difficulty(1_000, 100, Duration::from_secs(1), 1 << 40),
            4_000
        );
        // 来なければ 4 分の 1 まで。
        assert_eq!(
            next_share_difficulty(1_000, 0, Duration::from_secs(600), 1 << 40),
            250
        );
        // ブロックの難易度は超えない。
        assert_eq!(
            next_share_difficulty(1_000, 100, Duration::from_secs(1), 1_500),
            1_500
        );
    }
}
