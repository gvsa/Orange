//! 自分自身の住所を住所帳から外すことを、実際に繋いで確かめる。
//!
//! 0.4.0 までは、住所帳に自分の住所が入ると外れなかった。自分への TCP
//! 接続は成功するので「到達できる住所」として tried に上がり、ハンドシェイク
//! で自分だと分かって切れたあとも、何度でも引かれた。
//!
//! ```text
//! [warn] dropped the exchange with 68.233.113.1:9444: the handshake with
//!        68.233.113.1:9444 failed: detected a connection to ourselves
//! ```
//!
//! `--external-addr` を渡しても止まらなかった。渡すと**以後は**覚えなくなる
//! が、すでに `peers.json` にある分は残っていたからである。

use oag_net::magic::magic_for;
use oag_net::message::NetAddress;
use oag_net::transport::Listener;
use oag_node::accept_loop;
use oag_node::connect::{self, Outbound};
use oag_node::service::{NodeHandle, NodeService};
use oag_primitives::Network;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const NETWORK: Network = Network::Regtest;

/// 待つ上限。
const TIMEOUT: Duration = Duration::from_secs(60);

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
            std::env::temp_dir().join(format!("oag-self-{tag}-{}-{n}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn start(tag: &str) -> (TempDir, NodeService) {
    let dir = TempDir::new(tag);
    let service = NodeService::start(NETWORK, &dir.0).expect("a node can be started");
    (dir, service)
}

async fn listen(handle: NodeHandle) -> SocketAddr {
    let listener = Listener::bind(magic_for(NETWORK), "127.0.0.1:0".parse().unwrap())
        .await
        .expect("it can listen");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(accept_loop(handle, listener));
    addr
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

async fn known(handle: &NodeHandle) -> usize {
    handle.status().await.unwrap().known_addresses
}

/// 他のノードから自分の住所を聞かされたとする。
async fn hear_about(handle: &NodeHandle, addr: SocketAddr) {
    handle
        .add_addresses(vec![NetAddress::from_socket(addr, 0, now())], None)
        .await
        .unwrap();
    // 住所帳の更新は依頼を投げるだけなので、届いたことを確かめる。
    let deadline = Instant::now() + TIMEOUT;
    while known(handle).await == 0 {
        assert!(Instant::now() < deadline, "{addr} was not learned");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// 自分の住所を知らないノードが、聞かされた自分の住所に繋ぎに行っても、
/// 1 度で外すこと。
#[test]
fn a_node_that_dials_itself_forgets_the_address() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let (_dir, service) = start("dial");
    let node = service.handle();

    runtime.block_on(async {
        let own = listen(node.clone()).await;
        hear_about(&node, own).await;

        // `--external-addr` 無し。住所帳から選んで繋ぎに行く。
        tokio::spawn(connect::maintain(node.clone(), Outbound::new(), Vec::new()));

        let deadline = Instant::now() + TIMEOUT;
        while known(&node).await > 0 {
            assert!(
                Instant::now() < deadline,
                "{own} is still in the address book after dialling ourselves"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // 他のノードからまた聞かされても、覚え直さない。
        node.add_addresses(vec![NetAddress::from_socket(own, 0, now())], None)
            .await
            .unwrap();
        assert_eq!(known(&node).await, 0, "learned our own address again");
        let picked = node.address_candidates(8, Vec::new()).await.unwrap();
        assert!(!picked.contains(&own), "{own} is still a candidate");
    });
}

/// `--external-addr` で名乗ったら、すでに覚えていた自分の住所を外すこと。
#[test]
fn naming_our_own_address_forgets_it() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let (_dir, service) = start("name");
    let node = service.handle();

    runtime.block_on(async {
        let own = listen(node.clone()).await;
        hear_about(&node, own).await;

        node.set_own_addresses(vec![own]).await.unwrap();

        assert_eq!(known(&node).await, 0, "{own} survived --external-addr");
        let picked = node.address_candidates(8, Vec::new()).await.unwrap();
        assert!(!picked.contains(&own), "{own} is still a candidate");
    });
}
