//! Stratum の受け口に、外の採掘器のふりをして繋ぐ。
//!
//! 採掘器の側の手順 (`docs/STRATUM.md`) をそのまま踏む。仕事の blob の
//! 92 バイト目にナンスを書き、blob 全体を RandomX に通し、その答えを
//! 出す。regtest の難易度は 1 なので、どのナンスでも当たる。

use oag_node::service::{NodeHandle, NodeService};
use oag_node::stratum::{self, ALGO, NONCE_OFFSET};
use oag_pow::randomx::RandomXVerifier;
use oag_primitives::{Address, Hash, Network, SecretKey};
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
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
        let dir = std::env::temp_dir().join(format!(
            "oag-stratum-{tag}-{}-{n}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn address() -> String {
    let secret = SecretKey::generate();
    Address::from_pubkey(NETWORK, &secret.public_key()).encode()
}

async fn start(tag: &str) -> (TempDir, NodeService, NodeHandle, SocketAddr) {
    let dir = TempDir::new(tag);
    let service = NodeService::start(NETWORK, &dir.0).expect("a node can be started");
    let handle = service.handle();
    let addr = stratum::start_stratum(handle.clone(), "127.0.0.1:0".parse().unwrap())
        .await
        .expect("stratum can listen");
    (dir, service, handle, addr)
}

struct Miner {
    lines: Lines<BufReader<OwnedReadHalf>>,
    write: OwnedWriteHalf,
    next_id: u64,
}

impl Miner {
    async fn connect(addr: SocketAddr) -> Miner {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (read, write) = stream.into_split();
        Miner {
            lines: BufReader::new(read).lines(),
            write,
            next_id: 1,
        }
    }

    async fn read(&mut self) -> Value {
        let line = tokio::time::timeout(Duration::from_secs(60), self.lines.next_line())
            .await
            .expect("the node answers")
            .unwrap()
            .expect("the connection stays open");
        serde_json::from_str(&line).unwrap()
    }

    /// 要求を送り、同じ id の応答を待つ。途中の通知は飛ばす。
    async fn call(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        let request = json!({"id": id, "jsonrpc": "2.0", "method": method, "params": params});
        let mut bytes = serde_json::to_vec(&request).unwrap();
        bytes.push(b'\n');
        self.write.write_all(&bytes).await.unwrap();
        loop {
            let reply = self.read().await;
            if reply.get("id") == Some(&json!(id)) {
                return reply;
            }
        }
    }

    /// 次に届く `job` 通知。
    async fn next_job(&mut self) -> Value {
        loop {
            let message = self.read().await;
            if message.get("method") == Some(&json!("job")) {
                return message["params"].clone();
            }
        }
    }
}

fn error_of(reply: &Value) -> String {
    reply["error"]["message"].as_str().unwrap_or("").to_string()
}

/// 採掘器がやることをそのままやる。blob にナンスを書いて RandomX に通す。
fn solve(job: &Value, low: u32) -> (String, String) {
    let mut blob = hex::decode(job["blob"].as_str().unwrap()).unwrap();
    blob[NONCE_OFFSET..NONCE_OFFSET + 4].copy_from_slice(&low.to_le_bytes());
    let seed: [u8; 32] = hex::decode(job["seed_hash"].as_str().unwrap())
        .unwrap()
        .try_into()
        .unwrap();
    let height = job["height"].as_u64().unwrap();
    let verifier =
        RandomXVerifier::new(&Hash::from_bytes(seed), oag_pow::seed_height(height)).unwrap();
    let hash = verifier.hash(&blob).unwrap();
    (hex::encode(low.to_le_bytes()), hex::encode(hash.as_bytes()))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_miner_that_follows_the_spec_extends_the_chain() {
    let (_dir, _service, handle, addr) = start("extends").await;
    let mut miner = Miner::connect(addr).await;

    let reply = miner
        .call(
            "login",
            json!({"login": address(), "pass": "x", "agent": "test/1.0", "algo": [ALGO]}),
        )
        .await;
    assert!(reply["error"].is_null(), "login failed: {reply}");
    let session = reply["result"]["id"].as_str().unwrap().to_string();
    let job = reply["result"]["job"].clone();

    // 仕事の形。100 バイトのヘッダ、高さ 1、最初のエポックのシードは
    // ジェネシスのハッシュ。regtest の難易度は 1 なので目安は全部 1。
    assert_eq!(job["algo"], ALGO);
    assert_eq!(job["blob"].as_str().unwrap().len(), 200);
    assert_eq!(job["height"], 1);
    let genesis = handle.hash_at_height(0).await.unwrap().unwrap();
    assert_eq!(job["seed_hash"], hex::encode(genesis.as_bytes()));
    assert_eq!(job["target"], "ffffffffffffffff");

    // 違うハッシュを申告すると、ノードが自分で計算した値と一緒に断る。
    let (nonce, _) = solve(&job, 7);
    let wrong = json!({"id": session, "job_id": job["job_id"], "nonce": nonce,
                       "result": "00".repeat(32)});
    let reply = miner.call("submit", wrong).await;
    assert!(
        error_of(&reply).contains("does not match"),
        "a wrong result was not called out: {reply}"
    );

    // 正しく計算した答えは通り、チェーンが伸びる。
    let (nonce, result) = solve(&job, 8);
    let right = json!({"id": session, "job_id": job["job_id"], "nonce": nonce,
                       "result": result});
    let reply = miner.call("submit", right.clone()).await;
    assert_eq!(
        reply["result"]["status"], "OK",
        "the share was refused: {reply}"
    );
    assert_eq!(handle.status().await.unwrap().height, 1);

    // 先端が動いたので、次の高さの仕事が届く。
    let next = miner.next_job().await;
    assert_eq!(next["height"], 2);
    assert_ne!(next["job_id"], job["job_id"]);

    // 同じ答えをもう一度出しても受け取らない。
    let reply = miner.call("submit", right).await;
    assert_eq!(error_of(&reply), "duplicate share");
}

#[tokio::test(flavor = "multi_thread")]
async fn two_miners_on_one_address_search_different_nonces() {
    let (_dir, _service, _handle, addr) = start("two").await;
    let payout = address();
    let mut a = Miner::connect(addr).await;
    let mut b = Miner::connect(addr).await;
    let ja = a.call("login", json!({"login": payout})).await["result"]["job"].clone();
    let jb = b.call("login", json!({"login": payout})).await["result"]["job"].clone();

    // ナンスの上位 4 バイトはノードが接続ごとに変えてある。
    let hi = |job: &Value| hex::decode(job["blob"].as_str().unwrap()).unwrap()[96..100].to_vec();
    assert_ne!(hi(&ja), hi(&jb));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_miner_that_cannot_do_rx_oag_is_turned_away_at_login() {
    let (_dir, _service, _handle, addr) = start("algo").await;
    let mut miner = Miner::connect(addr).await;
    let reply = miner
        .call(
            "login",
            json!({"login": address(), "algo": ["rx/0", "cn/r"]}),
        )
        .await;
    assert!(error_of(&reply).contains(ALGO), "not refused: {reply}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_login_must_be_an_address_on_this_network() {
    let (_dir, _service, _handle, addr) = start("address").await;
    let mut miner = Miner::connect(addr).await;
    let reply = miner
        .call("login", json!({"login": "not-an-address"}))
        .await;
    assert!(
        error_of(&reply).contains("OAG address"),
        "not refused: {reply}"
    );

    // 本番のアドレスは regtest では使えない。
    let secret = SecretKey::generate();
    let mainnet = Address::from_pubkey(Network::Mainnet, &secret.public_key()).encode();
    let reply = miner.call("login", json!({"login": mainnet})).await;
    assert!(
        !reply["error"].is_null(),
        "a mainnet address was accepted: {reply}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_worker_name_after_a_dot_is_allowed() {
    let (_dir, _service, _handle, addr) = start("worker").await;
    let mut miner = Miner::connect(addr).await;
    let reply = miner
        .call("login", json!({"login": format!("{}.rig1", address())}))
        .await;
    assert!(reply["error"].is_null(), "login failed: {reply}");
}

#[tokio::test(flavor = "multi_thread")]
async fn nothing_but_login_and_keepalive_works_before_logging_in() {
    let (_dir, _service, _handle, addr) = start("order").await;
    let mut miner = Miner::connect(addr).await;
    let reply = miner.call("getjob", json!({})).await;
    assert_eq!(error_of(&reply), "log in first");
    let reply = miner.call("keepalived", json!({})).await;
    assert_eq!(reply["result"]["status"], "KEEPALIVED");
}

/// `docs/STRATUM.md` に載せた検算用の値。本番のブロック 1 と 16965 である。
///
/// **仕様書の値が本物であることを、ここで確かめ続ける。** 採掘器を作る側は
/// この値で自分の計算を確かめる。仕様書が間違っていれば、正しい採掘器が
/// 間違っていると言われる。
#[test]
fn the_test_vectors_in_the_spec_are_real_mainnet_blocks() {
    use oag_consensus::{BlockHeader, Decode};
    let h = |s: &str| Hash::from_bytes(hex::decode(s).unwrap().try_into().unwrap());
    // (blob, seed_hash, seed height, pow hash, share target, block hash)
    let vectors = [
        (
            "000000007511b77a9fb2aac8d7ba4655ed372c6fc3fbf800e1872a5c907bb64a775f0c40\
             1c52530a718baff7d389da1afff96a5af3e89529ef2bb3180bf63ad4bbc2c9ad\
             2fc8ab6a00000000e80300000000000001000000000000004100000000000000",
            "7511b77a9fb2aac8d7ba4655ed372c6fc3fbf800e1872a5c907bb64a775f0c40",
            0,
            "002d5ca362a5364c6a63fc3e87a85401468846f084f44b0f680acb73559d3da3",
            "efa7c64b37894100",
            "4fa64efcd444f629ec9e3fbc057ed61613fb8b22026b201a623f792abb569915",
        ),
        (
            "00000000dd5580b564b8e2eb22f8d597b1bf062b6fd3930837461c1bb882bd423cd6642a\
             bbae524eacd6500682f58c38c653bd5526a52622927fd722bb80d25e9e42f743\
             56aabb6a00000000e4cf0f00000000004542000000000000bc060000000000c0",
            "7ddfebd83d6ac2a58c4987d9034d22228e17aae5f65ef5746708b97a16a8cfdb",
            16384,
            "00000ce135fb6f2acaa1bf01fe142afae302e35a0e93787bd56ff9507fb1bd07",
            "1e5260ae30100000",
            "7a36b423992e24093f0bbfac9fe513858d2b4075bc019818e722ff7bb43a6d14",
        ),
    ];
    for (blob, seed, seed_height, pow, target, block_hash) in vectors {
        let blob = hex::decode(blob).unwrap();
        let header = BlockHeader::decode(&blob).unwrap();
        assert_eq!(
            header.hash(),
            h(block_hash),
            "the blob is not that block's header"
        );
        assert_eq!(oag_pow::seed_height(header.height), seed_height);

        let verifier = RandomXVerifier::new(&h(seed), seed_height).unwrap();
        let hash = verifier.hash(&blob).unwrap();
        assert_eq!(hash, h(pow));

        let t64 = stratum::share_target(header.difficulty);
        assert_eq!(hex::encode(t64.to_le_bytes()), target);
        assert!(stratum::passes_share_target(hash.as_bytes(), t64));
        assert!(oag_pow::meets_difficulty(&hash, header.difficulty).unwrap());
    }
}
