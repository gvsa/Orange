//! プールとして動かし、見つけたブロックの報酬が採掘者に届くまでを通す。
//!
//! regtest の難易度は 1 なので、シェアはどれもブロックの当たりになる。
//! 2 人が 1 つずつ見つけ、成熟するまで掘り進め、支払いが 1 本の取引で
//! 出ることを確かめる。

use oag_consensus::lock::Lock;
use oag_node::pool::{Pool, PoolConfig};
use oag_node::service::{MiningMode, NodeHandle, NodeService};
use oag_node::stratum::{self, ALGO, NONCE_OFFSET};
use oag_pow::randomx::RandomXVerifier;
use oag_primitives::{Address, Amount, Hash, Network, SecretKey};
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

const NETWORK: Network = Network::Regtest;

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> TempDir {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("oag-pool-{tag}-{}-{n}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn address() -> Address {
    let secret = SecretKey::generate();
    Address::from_pubkey(NETWORK, &secret.public_key())
}

struct Miner {
    lines: Lines<BufReader<OwnedReadHalf>>,
    write: OwnedWriteHalf,
    next_id: u64,
    session: String,
    job: Value,
}

impl Miner {
    async fn login(addr: SocketAddr, payee: &Address) -> Miner {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (read, write) = stream.into_split();
        let mut miner = Miner {
            lines: BufReader::new(read).lines(),
            write,
            next_id: 1,
            session: String::new(),
            job: Value::Null,
        };
        let reply = miner
            .call("login", json!({"login": payee.encode(), "algo": [ALGO]}))
            .await;
        assert!(reply["error"].is_null(), "login failed: {reply}");
        miner.session = reply["result"]["id"].as_str().unwrap().to_string();
        miner.job = reply["result"]["job"].clone();
        miner
    }

    async fn read(&mut self) -> Value {
        let line = tokio::time::timeout(Duration::from_secs(60), self.lines.next_line())
            .await
            .expect("the node answers")
            .unwrap()
            .expect("the connection stays open");
        serde_json::from_str(&line).unwrap()
    }

    async fn call(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        let request = json!({"id": id, "jsonrpc": "2.0", "method": method, "params": params});
        let mut bytes = serde_json::to_vec(&request).unwrap();
        bytes.push(b'\n');
        self.write.write_all(&bytes).await.unwrap();
        loop {
            let message = self.read().await;
            if message.get("method") == Some(&json!("job")) {
                self.job = message["params"].clone();
            } else if message.get("id") == Some(&json!(id)) {
                return message;
            }
        }
    }

    /// 高さ `height` の仕事が届くまで待つ。
    async fn job_at(&mut self, height: u64) {
        while self.job["height"] != height {
            let message = self.read().await;
            if message.get("method") == Some(&json!("job")) {
                self.job = message["params"].clone();
            }
        }
    }

    /// 今の仕事を解いて出す。
    async fn submit(&mut self) -> Value {
        let mut blob = hex::decode(self.job["blob"].as_str().unwrap()).unwrap();
        blob[NONCE_OFFSET..NONCE_OFFSET + 4].copy_from_slice(&1u32.to_le_bytes());
        let seed: [u8; 32] = hex::decode(self.job["seed_hash"].as_str().unwrap())
            .unwrap()
            .try_into()
            .unwrap();
        let height = self.job["height"].as_u64().unwrap();
        let verifier =
            RandomXVerifier::new(&Hash::from_bytes(seed), oag_pow::seed_height(height)).unwrap();
        let hash = verifier.hash(&blob).unwrap();
        let params = json!({"id": self.session, "job_id": self.job["job_id"],
                            "nonce": hex::encode(1u32.to_le_bytes()),
                            "result": hex::encode(hash.as_bytes())});
        self.call("submit", params).await
    }
}

async fn wait_for_height(handle: &NodeHandle, height: u64) {
    let deadline = Instant::now() + Duration::from_secs(300);
    while handle.status().await.unwrap().height < height {
        assert!(
            Instant::now() < deadline,
            "the chain did not reach {height}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// 状況の頁を 1 回読む。本文だけを返す。
async fn get(addr: SocketAddr, path: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let request = format!("GET {path} HTTP/1.1\r\nHost: pool\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    body.to_string()
}

fn oag(text: &str) -> u128 {
    text.parse::<Amount>().unwrap().to_atomic()
}

#[tokio::test(flavor = "multi_thread")]
async fn rewards_are_split_by_shares_and_paid_once_mature() {
    let dir = TempDir::new("pays");
    let service = NodeService::start(NETWORK, &dir.0).expect("a node can be started");
    let handle = service.handle();
    let operator = address();
    let config = PoolConfig {
        fee_basis_points: 100, // 1 %
        min_payout: "0.01".parse().unwrap(),
        payout_interval: Duration::ZERO,
        fee_address: Some(operator.clone()),
    };
    let pool = Pool::open(handle.clone(), &dir.0, config.clone()).unwrap();
    let addr = stratum::start_stratum_with(
        handle.clone(),
        "127.0.0.1:0".parse().unwrap(),
        Some(pool.clone()),
    )
    .await
    .unwrap();

    let page = oag_node::pool_page::start_pool_page(
        pool.clone(),
        handle.clone(),
        "127.0.0.1:0".parse().unwrap(),
        addr.port(),
    )
    .await
    .unwrap();

    let (a, b) = (address(), address());
    let mut miner_a = Miner::login(addr, &a).await;
    let mut miner_b = Miner::login(addr, &b).await;

    // 報酬はプールの鍵に入る仕事が配られている。
    assert_eq!(miner_a.job["height"], 1);

    // A が高さ 1 を見つける。窓 (難易度 1 の 2 倍) には A の 1 本だけ。
    let reply = miner_a.submit().await;
    assert_eq!(reply["result"]["status"], "OK", "{reply}");
    wait_for_height(&handle, 1).await;

    // B が高さ 2 を見つける。窓には A と B が 1 本ずつ。
    miner_b.job_at(2).await;
    let reply = miner_b.submit().await;
    assert_eq!(reply["result"]["status"], "OK", "{reply}");
    wait_for_height(&handle, 2).await;

    let ledger = pool.ledger().await;
    assert_eq!(ledger.rounds.len(), 2, "both blocks wait to mature");
    assert!(
        ledger.balances.is_empty(),
        "nothing is payable before maturity"
    );

    // 成熟するまで、別の受取先で掘り進める。
    let other = Lock::from_address(&address());
    handle
        .start_mining(other, Some(125), MiningMode::light())
        .await
        .unwrap();
    wait_for_height(&handle, 127).await;

    // 成熟 → 残高 → 支払い。
    pool.maintain().await.unwrap();
    let ledger = pool.ledger().await;
    assert!(ledger.rounds.is_empty(), "both rounds matured");
    assert!(ledger.balances.is_empty(), "everything payable was paid");
    assert_eq!(ledger.payouts.len(), 1);

    // 10 OAG × 99 % = 9.9。高さ 1 は A だけ、高さ 2 は半分ずつ。
    let paid_a = oag("14.85");
    let paid_b = oag("4.95");
    assert_eq!(ledger.paid[&a.encode()], paid_a.to_string());
    assert_eq!(ledger.paid[&b.encode()], paid_b.to_string());
    assert_eq!(ledger.fee_earned, oag("0.2").to_string());
    // 運営の取り分も、採掘者と同じ支払いで送られる。
    assert_eq!(ledger.paid[&operator.encode()], oag("0.2").to_string());

    // 取引は mempool にあり、A と B に払っている。手数料は 2 人が持つ。
    let payout = &ledger.payouts[0];
    let txid: Hash = payout.txid.parse().unwrap();
    let tx = handle
        .mempool_tx(txid)
        .await
        .unwrap()
        .expect("the payout is in the mempool");
    let to = |who: &Address| {
        let lock = Lock::from_address(who);
        tx.outputs
            .iter()
            .filter(|o| o.lock == lock)
            .map(|o| o.amount.to_atomic())
            .sum::<u128>()
    };
    let (got_a, got_b) = (to(&a), to(&b));
    assert!(
        got_a < paid_a && got_a > paid_a - oag("0.01"),
        "A got {got_a}"
    );
    assert!(
        got_b < paid_b && got_b > paid_b - oag("0.01"),
        "B got {got_b}"
    );

    // 状況の頁に、つながっている 2 台、見つけた 2 つ、支払い 1 本が出る。
    let stats: Value = serde_json::from_str(&get(page, "/api/stats").await).unwrap();
    assert_eq!(stats["pool"]["workers"].as_array().unwrap().len(), 2);
    assert_eq!(stats["pool"]["found"].as_array().unwrap().len(), 2);
    assert_eq!(stats["pool"]["found"][0]["status"], "matured");
    assert_eq!(stats["pool"]["payouts"].as_array().unwrap().len(), 1);
    assert_eq!(stats["network"]["height"], 127);
    let html = get(page, "/").await;
    assert!(html.contains(&a.encode()) && html.contains(&b.encode()));
    assert!(html.contains(&payout.txid));

    // 1 つ掘れば承認される。
    let other = Lock::from_address(&address());
    handle
        .start_mining(other, Some(1), MiningMode::light())
        .await
        .unwrap();
    wait_for_height(&handle, 128).await;
    pool.maintain().await.unwrap();
    assert!(pool.ledger().await.payouts[0].confirmed);

    // 台帳は保存されていて、開き直しても残っている。
    let reopened = Pool::open(handle.clone(), &dir.0, config).unwrap();
    assert_eq!(reopened.ledger().await, pool.ledger().await);
    assert_eq!(reopened.address(), pool.address());
    drop(service);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_pool_refuses_a_ledger_that_belongs_to_another_key() {
    let dir = TempDir::new("ledger");
    let service = NodeService::start(NETWORK, &dir.0).expect("a node can be started");
    let handle = service.handle();
    let pool = Pool::open(handle.clone(), &dir.0, PoolConfig::default()).unwrap();
    pool.maintain().await.unwrap(); // 台帳を書く
    std::fs::remove_file(dir.0.join("pool.key")).unwrap();
    let error = Pool::open(handle, &dir.0, PoolConfig::default())
        .err()
        .expect("a ledger for another key is refused");
    assert!(error.contains("belongs to"), "{error}");
    let _: Arc<Pool> = pool;
    drop(service);
}
