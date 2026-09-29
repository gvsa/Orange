//! Stratum の受け口。XMRig などの外の採掘器に仕事を配り、答えを受け取る。
//!
//! 仕様は `docs/STRATUM.md` にある。ここに書くのは実装の都合だけである。
//!
//! # 何を配るのか
//!
//! 配るのは**ブロックヘッダそのもの** (100 バイト) である。PoW の入力は
//! ヘッダの符号化そのものなので (`docs/SPEC.md` §11)、採掘器はこれに
//! ナンスを書き込んで RandomX に通すだけでよい。
//!
//! ```text
//!  0        4                  36                 68      76      84      92   96   100
//!  version  prev_hash          merkle_root        time    diff    height  nonce
//!                                                                         ├lo──┼hi──┤
//!                                                                         採掘器 ノード
//! ```
//!
//! ナンスは 8 バイトのうち**下位 4 バイトを採掘器が、上位 4 バイトを
//! ノードが**決める。上位には接続ごとに違う値を入れておくので、同じ
//! 受取先で何台つないでも探す範囲が重ならない。
//!
//! # なぜ素の XMRig では掘れないのか
//!
//! Monero の形に合わせてあるからである。素の XMRig は 39 バイト目に
//! ナンスを書き、ハッシュの末尾 8 バイトをリトルエンディアンで読んで
//! 当たりを判定する。OAG のヘッダでは 39 バイト目はマークルルートの
//! 途中であり、当たりの判定はハッシュをビッグエンディアンの数として
//! 読む (`oag_pow::target`)。**合意ルールは変えず**、採掘器の側で
//! この 2 点を合わせてもらう。アルゴリズム名 [`ALGO`] がその目印である。

use crate::pool::{self, Pool};
use crate::service::{MiningJob, NodeEvent, NodeHandle};
use oag_consensus::lock::Lock;
use oag_consensus::{Block, BlockHeader, Encode, Transaction};
use oag_primitives::{Address, Amount, Network};
use serde_json::{json, Value};
use std::collections::{HashSet, VecDeque};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;

/// 配る仕事のアルゴリズム名。
///
/// RandomX の設定は Monero と同じ (`rx/0`) だが、**ナンスの位置と当たりの
/// 読み方が違う**。同じ名前を名乗ると、素の XMRig が黙って無効な答えを
/// 掘り続ける。別の名前にして、分からない採掘器には最初に断る。
pub const ALGO: &str = "rx/oag";

/// ヘッダの中で採掘器がナンスを書き込む位置 (バイト)。
pub const NONCE_OFFSET: usize = 92;

/// 採掘器が書き込むナンスの長さ (バイト)。リトルエンディアン。
pub const NONCE_BYTES: usize = 4;

/// 1 行の長さの上限。仕事の提出は 200 バイトほどである。
const MAX_LINE: usize = 16 * 1024;

/// 先端が動かなくても仕事を配り直す間隔。
///
/// mempool に入った取引を詰め直し、ヘッダの時刻を新しくする。
const JOB_REFRESH: Duration = Duration::from_secs(30);

/// これだけ何も言ってこなければ切る。XMRig の keepalive は 60 秒ごとである。
const IDLE_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// 接続ごとに覚えておく仕事の数。
///
/// 配り直した直後に届いた答えも受け取れるように、直近の数件は残す。
const KEPT_JOBS: usize = 4;

/// 誤った答えをこれだけ出した接続は切る。
///
/// 答えを確かめるたびに RandomX を 1 回まわす。でたらめを送り続けて
/// ノードの手を塞ぐことはさせない。
const MAX_INVALID: u32 = 16;

/// 同時に受け付ける接続の数。
const MAX_SESSIONS: usize = 256;

/// ネットワークごとの既定のポート ([`Network::mining_port`])。本番は 1919。
pub fn default_port(network: Network) -> u16 {
    network.mining_port()
}

/// 既定の待ち受け先。**この機械の中だけ**である。
pub fn default_addr(network: Network) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), default_port(network))
}

/// 採掘器に渡す、当たりの目安 (64 ビット)。
///
/// 本当のターゲットは 256 ビットある (`(2^256 − 1) / difficulty`)。
/// その**上位 64 ビット**を渡す。採掘器はハッシュの先頭 8 バイトを
/// ビッグエンディアンで読み、これ**以下**なら提出する
/// ([`passes_share_target`])。
///
/// 上位 64 ビットだけで比べるので、ちょうど境目のハッシュは 256 ビットで
/// 比べると外れていることがある。それはノードが弾く。**当たりを取りこぼす
/// ことは無い。**
pub fn share_target(difficulty: u64) -> u64 {
    match oag_pow::target_from_difficulty(difficulty) {
        Ok(target) => u64::from_be_bytes(target[..8].try_into().expect("32 bytes")),
        Err(_) => 0,
    }
}

/// 採掘器の側の判定。[`share_target`] と対にして仕様に書いてある。
pub fn passes_share_target(hash: &[u8; 32], target: u64) -> bool {
    u64::from_be_bytes(hash[..8].try_into().expect("32 bytes")) <= target
}

/// ヘッダに入るナンス。上位 32 ビットが接続、下位 32 ビットが採掘器。
pub fn nonce_of(session: u32, low: u32) -> u64 {
    (u64::from(session) << 32) | u64::from(low)
}

/// シェアの難易度を見直すまでに待つシェアの数。
const RETARGET_SHARES: u32 = 6;

/// シェアが少なくても、これだけ経てば難易度を見直す。
const RETARGET_AFTER: Duration = Duration::from_secs(120);

/// ソロの Stratum を始める。実際に待ち受けた住所を返す。
///
/// 見つけたブロックの報酬は、採掘器がログインしたアドレスに直接入る。
pub async fn start_stratum(handle: NodeHandle, addr: SocketAddr) -> Result<SocketAddr, String> {
    start_stratum_with(handle, addr, None).await
}

/// Stratum を始める。`pool` を渡すとプールとして動く ([`crate::pool`])。
pub async fn start_stratum_with(
    handle: NodeHandle,
    addr: SocketAddr,
    pool: Option<Arc<Pool>>,
) -> Result<SocketAddr, String> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| format!("cannot listen for stratum on {addr}: {e}"))?;
    let bound = listener
        .local_addr()
        .map_err(|e| format!("cannot determine the address: {e}"))?;

    let shared = Arc::new(Shared {
        handle,
        pool,
        next_session: AtomicU32::new(1),
        open: AtomicUsize::new(0),
    });

    tokio::spawn(async move {
        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    crate::log_warn!("cannot accept a stratum connection: {e}");
                    continue;
                }
            };
            if shared.open.fetch_add(1, Ordering::SeqCst) >= MAX_SESSIONS {
                shared.open.fetch_sub(1, Ordering::SeqCst);
                drop(stream);
                continue;
            }
            let shared = shared.clone();
            tokio::spawn(async move {
                serve(&shared, stream, peer).await;
                shared.open.fetch_sub(1, Ordering::SeqCst);
            });
        }
    });
    Ok(bound)
}

struct Shared {
    handle: NodeHandle,
    /// プールとして動くなら、その台帳。`None` ならソロ。
    pool: Option<Arc<Pool>>,
    /// 次の接続に渡すナンスの上位 32 ビット。
    next_session: AtomicU32,
    /// いま開いている接続の数。
    open: AtomicUsize,
}

/// 配った仕事。答えが来たときにブロックへ仕上げるために残す。
struct Job {
    id: u64,
    header: BlockHeader,
    transactions: Vec<Transaction>,
    /// この仕事で配ったシェアの難易度。ソロならブロックの難易度と同じ。
    share_difficulty: u64,
}

/// ログインを済ませた接続。
struct Session {
    /// ナンスの上位 32 ビット。
    id: u32,
    /// ログインしたアドレス。ソロでは報酬の受取先、プールでは支払い先。
    payout: Lock,
    /// ログインしたアドレスの文字列 (プールの台帳の鍵)。
    payee: String,
    /// プールで配るシェアの難易度。0 はまだ決めていない。
    share_difficulty: u64,
    /// 難易度を見直してからのシェアの数と、その起点。
    shares_since: u32,
    since: Instant,
    /// 難易度を変えたので、すぐに仕事を配り直す。
    retargeted: bool,
    /// プールの状況の頁に出すための番号。ソロでは `None`。
    worker_id: Option<u64>,
    /// 記録に出す名前。
    worker: String,
    jobs: VecDeque<Job>,
    next_job: u64,
    /// 受け取ったことのある (仕事, ナンス)。二重の提出を断る。
    seen: HashSet<(u64, u32)>,
    invalid: u32,
}

/// 1 本の接続を捌く。
async fn serve(shared: &Shared, stream: TcpStream, peer: SocketAddr) {
    let (read, mut write) = stream.into_split();

    // 読むのは別の仕事に任せる。**行の途中で select! に割り込まれると、
    // 読みかけの分を失う。** 行が揃ってから渡してもらう。
    let (lines_tx, mut lines) = mpsc::channel::<Result<Vec<u8>, String>>(16);
    let reader = tokio::spawn(async move {
        let mut reader = BufReader::new(read);
        loop {
            let mut buf = Vec::new();
            let limit = (MAX_LINE + 1) as u64;
            match (&mut reader).take(limit).read_until(b'\n', &mut buf).await {
                Ok(0) => return,
                Ok(_) if buf.len() > MAX_LINE => {
                    let _ = lines_tx.send(Err("the line is too long".to_string())).await;
                    return;
                }
                Ok(_) => {
                    if lines_tx.send(Ok(buf)).await.is_err() {
                        return;
                    }
                }
                Err(_) => return,
            }
        }
    });

    let mut events = shared.handle.subscribe();
    let mut refresh = tokio::time::interval(JOB_REFRESH);
    refresh.tick().await;
    let idle = tokio::time::sleep(IDLE_TIMEOUT);
    tokio::pin!(idle);
    let mut session: Option<Session> = None;

    loop {
        tokio::select! {
            line = lines.recv() => {
                let Some(line) = line else { break };
                idle.as_mut().reset(tokio::time::Instant::now() + IDLE_TIMEOUT);
                let line = match line {
                    Ok(line) => line,
                    Err(e) => {
                        let _ = send(&mut write, &error_reply(Value::Null, &e)).await;
                        break;
                    }
                };
                if line.iter().all(u8::is_ascii_whitespace) {
                    continue;
                }
                let reply = handle_line(shared, &mut session, &line, peer).await;
                if send(&mut write, &reply).await.is_err() {
                    break;
                }
                // シェアの難易度が変わった。古い目安のまま掘らせ続けない。
                if session.as_ref().is_some_and(|s| s.retargeted) {
                    if let Some(s) = session.as_mut() {
                        s.retargeted = false;
                    }
                    if push_job(shared, &mut session, &mut write).await.is_err() {
                        break;
                    }
                }
                if session.as_ref().is_some_and(|s| s.invalid >= MAX_INVALID) {
                    crate::log_warn!("stratum: closing {peer} after {MAX_INVALID} invalid shares");
                    break;
                }
            }
            event = events.recv() => {
                let moved = match event {
                    Ok(NodeEvent::NewTip { .. }) => true,
                    // 取り落とした報せの中に先端の移動があったかもしれない。
                    Err(RecvError::Lagged(_)) => true,
                    Err(RecvError::Closed) => break,
                    Ok(_) => false,
                };
                if moved && push_job(shared, &mut session, &mut write).await.is_err() {
                    break;
                }
            }
            _ = refresh.tick() => {
                if push_job(shared, &mut session, &mut write).await.is_err() {
                    break;
                }
            }
            _ = &mut idle => break,
        }
    }

    reader.abort();
    if let Some(session) = session {
        crate::log_mine!("stratum: {} disconnected", session.worker);
        if let (Some(pool), Some(id)) = (&shared.pool, session.worker_id) {
            pool.worker_disconnected(id).await;
        }
    }
}

/// 新しい仕事を組んで送る。ログイン前なら何もしない。
///
/// 組めないとき (同期の最中など) は、前の仕事を掘らせたままにする。
async fn push_job(
    shared: &Shared,
    session: &mut Option<Session>,
    write: &mut OwnedWriteHalf,
) -> std::io::Result<()> {
    let Some(session) = session.as_mut() else {
        return Ok(());
    };
    match make_job(shared, session).await {
        Ok(job) => {
            let message = json!({"jsonrpc": "2.0", "method": "job", "params": job});
            send(write, &message).await
        }
        Err(_) => Ok(()),
    }
}

/// 1 行の要求を捌いて、返す内容を組む。
async fn handle_line(
    shared: &Shared,
    session: &mut Option<Session>,
    line: &[u8],
    peer: SocketAddr,
) -> Value {
    let request: Value = match serde_json::from_slice(line) {
        Ok(value) => value,
        Err(e) => return error_reply(Value::Null, &format!("not JSON: {e}")),
    };
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let method = request.get("method").and_then(Value::as_str).unwrap_or("");
    let params = request.get("params").cloned().unwrap_or(Value::Null);

    let result = match method {
        "login" => login(shared, session, &params, peer).await,
        "getjob" => match session.as_mut() {
            Some(s) => make_job(shared, s).await,
            None => Err("log in first".to_string()),
        },
        "submit" => match session.as_mut() {
            Some(s) => submit(shared, s, &params).await,
            None => Err("log in first".to_string()),
        },
        "keepalived" => Ok(json!({"status": "KEEPALIVED"})),
        "" => Err("no method".to_string()),
        other => Err(format!("unknown method {other}")),
    };
    match result {
        Ok(result) => json!({"id": id, "jsonrpc": "2.0", "error": null, "result": result}),
        Err(message) => error_reply(id, &message),
    }
}

fn error_reply(id: Value, message: &str) -> Value {
    json!({
        "id": id,
        "jsonrpc": "2.0",
        "error": {"code": -1, "message": message},
        "result": null,
    })
}

async fn login(
    shared: &Shared,
    session: &mut Option<Session>,
    params: &Value,
    peer: SocketAddr,
) -> Result<Value, String> {
    // 分からない採掘器には最初に断る。**素の XMRig に掘らせると、
    // 無効な答えを延々と計算させることになる。**
    if let Some(algos) = params.get("algo").and_then(Value::as_array) {
        if !algos.iter().any(|a| a.as_str() == Some(ALGO)) {
            return Err(format!(
                "this node hands out {ALGO} jobs only, and this miner does not offer it. \
                 Stock XMRig cannot mine OAG; use the OAG build (see docs/STRATUM.md)"
            ));
        }
    }

    let login = params.get("login").and_then(Value::as_str).unwrap_or("");
    // 「アドレス.名前」の形を許す。bech32 の文字に `.` は無いので紛れない。
    let (address, suffix) = match login.split_once('.') {
        Some((address, name)) => (address, Some(name)),
        None => (login, None),
    };
    let network = shared.handle.network();
    let address = Address::decode_on(network, address.trim())
        .map_err(|e| format!("the login must be an OAG address to pay to: {e}"))?;

    let worker = params
        .get("rigid")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .or(suffix)
        .map(str::to_string)
        .unwrap_or_else(|| peer.to_string());

    let mut fresh = Session {
        id: shared.next_session.fetch_add(1, Ordering::SeqCst),
        payout: Lock::from_address(&address),
        payee: address.encode(),
        share_difficulty: 0,
        shares_since: 0,
        since: Instant::now(),
        retargeted: false,
        worker_id: None,
        worker,
        jobs: VecDeque::new(),
        next_job: 0,
        seen: HashSet::new(),
        invalid: 0,
    };
    let job = make_job(shared, &mut fresh).await?;

    let agent = params
        .get("agent")
        .and_then(Value::as_str)
        .unwrap_or("unknown miner");
    crate::log_mine!(
        "stratum: {} connected from {peer} ({agent}), paying to {}",
        fresh.worker,
        short_address(&address.encode())
    );

    if let Some(pool) = &shared.pool {
        fresh.worker_id = Some(pool.worker_connected(&fresh.worker, &fresh.payee).await);
    }

    let result = json!({
        "id": format!("{:08x}", fresh.id),
        "job": job,
        "extensions": ["algo", "keepalive"],
        "status": "OK",
    });
    // つなぎ直し (同じ接続での二度目のログイン) なら、前の採掘器を外す。
    if let (Some(pool), Some(old)) = (&shared.pool, session.as_ref().and_then(|s| s.worker_id)) {
        pool.worker_disconnected(old).await;
    }
    *session = Some(fresh);
    Ok(result)
}

/// 仕事を組み、覚えて、送る形にする。
async fn make_job(shared: &Shared, session: &mut Session) -> Result<Value, String> {
    // 追いついていない先端の上に積んでも、捨てられるブロックにしかならない。
    if shared.handle.is_syncing().await? {
        return Err("the node is still catching up with the chain; try again shortly".to_string());
    }
    // プールなら報酬はプールの鍵へ。ソロなら採掘器のアドレスへ直接。
    let payout = match &shared.pool {
        Some(pool) => pool.lock().clone(),
        None => session.payout.clone(),
    };
    let MiningJob { template, seed, .. } = shared
        .handle
        .mining_job(payout, u64::from(session.id))
        .await?;
    let mut header = template.header;
    header.nonce = nonce_of(session.id, 0);

    let share_difficulty = if shared.pool.is_some() {
        if session.share_difficulty == 0 {
            session.share_difficulty = pool::initial_share_difficulty(header.difficulty);
            session.since = Instant::now();
        } else if session.since.elapsed() >= RETARGET_AFTER {
            // 長いあいだ当たらない。易しくする。
            retarget(session, header.difficulty);
        }
        // この仕事に今の難易度を載せたので、配り直しの印は要らない。
        session.retargeted = false;
        session.share_difficulty.min(header.difficulty).max(1)
    } else {
        header.difficulty
    };

    session.next_job += 1;
    let id = session.next_job;
    let job = json!({
        "job_id": format!("{id:x}"),
        "blob": hex::encode(header.encode()),
        "target": hex::encode(share_target(share_difficulty).to_le_bytes()),
        "algo": ALGO,
        "height": header.height,
        "seed_hash": hex::encode(seed.as_bytes()),
    });

    session.jobs.push_back(Job {
        id,
        header,
        transactions: template.transactions,
        share_difficulty,
    });
    while session.jobs.len() > KEPT_JOBS {
        session.jobs.pop_front();
    }
    let oldest = session.jobs.front().map_or(id, |j| j.id);
    session.seen.retain(|(job, _)| *job >= oldest);
    Ok(job)
}

/// 答えを確かめ、当たりならブロックにして流す。
async fn submit(shared: &Shared, session: &mut Session, params: &Value) -> Result<Value, String> {
    let text = |key: &str| params.get(key).and_then(Value::as_str).unwrap_or("");

    let job_id = u64::from_str_radix(text("job_id"), 16)
        .map_err(|_| "job_id is not a job this node handed out".to_string())?;
    let nonce: [u8; NONCE_BYTES] = decode_fixed(text("nonce"))
        .ok_or_else(|| format!("nonce must be {} hex digits", NONCE_BYTES * 2))?;
    let result: [u8; 32] =
        decode_fixed(text("result")).ok_or_else(|| "result must be 64 hex digits".to_string())?;
    let low = u32::from_le_bytes(nonce);

    let (mut header, transactions, share_difficulty) =
        match session.jobs.iter().find(|j| j.id == job_id) {
            Some(job) => (job.header, job.transactions.clone(), job.share_difficulty),
            None => return Err("unknown job; it has been replaced by a newer one".to_string()),
        };
    if !session.seen.insert((job_id, low)) {
        return Err("duplicate share".to_string());
    }
    header.nonce = nonce_of(session.id, low);

    // **申告されたハッシュを信じない。** 自分で計算して突き合わせる。
    // ずれていれば、採掘器がナンスを違う場所に書いたか、違うものを
    // ハッシュしている。それを言えば、直す側が迷わない。
    let hash = shared.handle.pow_hash(header).await?;
    if hash.as_bytes() != &result {
        session.invalid += 1;
        return Err(format!(
            "the result does not match: this node computed {hash} for that nonce. \
             Check that the nonce is written little-endian at byte {NONCE_OFFSET} of the \
             blob and that all {} bytes of the blob are hashed",
            header.encode().len()
        ));
    }
    if !oag_pow::meets_difficulty(&hash, share_difficulty).unwrap_or(false) {
        session.invalid += 1;
        return Err("low difficulty share: the hash does not meet the target \
             (compare the first 8 bytes of the hash, read big-endian, with the target)"
            .to_string());
    }

    if let Some(pool) = &shared.pool {
        pool.record_share(
            session.worker_id.unwrap_or(0),
            &session.payee,
            share_difficulty,
        )
        .await;
        session.shares_since += 1;
        if session.shares_since >= RETARGET_SHARES || session.since.elapsed() >= RETARGET_AFTER {
            retarget(session, header.difficulty);
        }
    }

    // ブロックの当たりでなければ、ここまで (プールのシェア)。
    if !oag_pow::meets_difficulty(&hash, header.difficulty).unwrap_or(false) {
        return Ok(json!({"status": "OK"}));
    }

    let reward = Amount::sum(
        transactions
            .first()
            .into_iter()
            .flat_map(|cb| cb.outputs.iter().map(|o| o.amount)),
    )
    .unwrap_or(Amount::ZERO);
    let block = Block {
        header,
        transactions,
    };
    let accepted = shared.handle.accept_block(block).await?;
    if !accepted.moved_tip {
        // プールではシェアとしては数えてある。ブロックが間に合わなかった
        // だけで、掘った事実は変わらない。
        if shared.pool.is_some() {
            crate::log_mine!(
                "stale  height {}  (stratum, {}): another block came first",
                header.height,
                session.worker
            );
            return Ok(json!({"status": "OK"}));
        }
        return Err("stale: another block extended the chain first".to_string());
    }
    crate::log_mine!(
        "found  height {}  {}  (stratum, {})",
        header.height,
        header.hash(),
        session.worker
    );
    if let Some(pool) = &shared.pool {
        pool.block_found(
            header.hash(),
            header.height,
            reward,
            header.difficulty,
            &session.payee,
        )
        .await?;
    }
    Ok(json!({"status": "OK"}))
}

/// シェアの難易度を見直す。変わったら、すぐ仕事を配り直す印を付ける。
fn retarget(session: &mut Session, block_difficulty: u64) {
    let next = pool::next_share_difficulty(
        session.share_difficulty,
        session.shares_since,
        session.since.elapsed(),
        block_difficulty,
    );
    if next != session.share_difficulty {
        session.share_difficulty = next;
        session.retargeted = true;
    }
    session.shares_since = 0;
    session.since = Instant::now();
}

/// 記録に出すアドレスの略記。`oag1qz56xr…fserdd` の形。
fn short_address(text: &str) -> String {
    if text.len() <= 20 {
        return text.to_string();
    }
    format!("{}…{}", &text[..10], &text[text.len() - 6..])
}

/// 16 進の文字列を、ちょうど `N` バイトとして読む。
fn decode_fixed<const N: usize>(text: &str) -> Option<[u8; N]> {
    hex::decode(text).ok()?.try_into().ok()
}

async fn send(write: &mut OwnedWriteHalf, message: &Value) -> std::io::Result<()> {
    let mut bytes = serde_json::to_vec(message).expect("a JSON value always serialises");
    bytes.push(b'\n');
    write.write_all(&bytes).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_share_target_is_the_top_of_the_full_target() {
        // 難易度 1 はターゲットの上限 (全部 1) である。
        assert_eq!(share_target(1), u64::MAX);
        assert_eq!(share_target(2), u64::MAX / 2);
        let d = 1_000_000;
        let full = oag_pow::target_from_difficulty(d).unwrap();
        assert_eq!(share_target(d).to_be_bytes(), full[..8]);
    }

    #[test]
    fn a_hash_that_meets_the_block_target_always_passes_the_share_check() {
        // 取りこぼしが無いこと。ターゲットちょうどのハッシュは通る。
        let d = 1_234_567;
        let full = oag_pow::target_from_difficulty(d).unwrap();
        assert!(passes_share_target(&full, share_target(d)));
        let mut above = full;
        above[0] = above[0].wrapping_add(1);
        assert!(!passes_share_target(&above, share_target(d)));
    }

    #[test]
    fn the_miner_writes_the_low_half_of_the_nonce() {
        // ヘッダの nonce はリトルエンディアンで 92 バイト目から。採掘器が
        // 書く 4 バイトが、そのまま下位 32 ビットになる。
        let header = BlockHeader {
            version: 1,
            prev_hash: oag_primitives::Hash::ZERO,
            merkle_root: oag_primitives::Hash::ZERO,
            timestamp: 0,
            difficulty: 1,
            height: 0,
            nonce: nonce_of(0xAABB_CCDD, 0x1122_3344),
        };
        let bytes = header.encode();
        assert_eq!(bytes.len(), 100);
        assert_eq!(
            bytes[NONCE_OFFSET..NONCE_OFFSET + NONCE_BYTES],
            0x1122_3344u32.to_le_bytes()
        );
        assert_eq!(bytes[NONCE_OFFSET + 4..], 0xAABB_CCDDu32.to_le_bytes());
    }

    #[test]
    fn default_ports_do_not_collide() {
        assert_eq!(default_port(Network::Mainnet), 1919);
        assert_ne!(default_port(Network::Testnet), 1919);
        assert_ne!(
            default_port(Network::Regtest),
            default_port(Network::Testnet)
        );
        assert!(default_addr(Network::Mainnet).ip().is_loopback());
    }
}
