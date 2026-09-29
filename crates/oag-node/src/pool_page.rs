//! プールの状況の頁。`--pool` を付けると、既定でポート 8000 に開く。
//!
//! 採掘者が「つながっているか」「どれだけ掘れているか」「いくら貯まって
//! いて、いつ払われるか」を確かめるためのものである。**読むだけ**で、
//! 何も変えられない。
//!
//! `/` が人の読む頁、`/api/stats` が同じ中身の JSON である。

use crate::explorer::{atomic_to_oag, esc, group, read_target, shorten, utc, CSS};
use crate::pool::{Pool, PoolView};
use crate::service::NodeHandle;
use serde::Serialize;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

/// 既定のポート。
pub const DEFAULT_PORT: u16 = 8000;

/// 頁を何秒ごとに読み直させるか。
const REFRESH_SECS: u32 = 30;

/// 表に出す行の上限。
const MAX_ROWS: usize = 50;

struct Shared {
    pool: Arc<Pool>,
    handle: NodeHandle,
    /// 採掘器の接続先として案内する Stratum のポート。
    stratum_port: u16,
}

/// JSON で返すもの。
#[derive(Serialize)]
struct Api<'a> {
    network: NetworkView,
    pool: &'a PoolView,
}

#[derive(Serialize)]
struct NetworkView {
    name: String,
    height: u64,
    difficulty: u64,
    /// 難易度から見積もったネットワーク全体のハッシュレート (H/s)。
    hashrate: f64,
}

/// 状況の頁を開く。実際に待ち受けた住所を返す。
pub async fn start_pool_page(
    pool: Arc<Pool>,
    handle: NodeHandle,
    addr: SocketAddr,
    stratum_port: u16,
) -> Result<SocketAddr, String> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| format!("cannot listen for the pool page on {addr}: {e}"))?;
    let bound = listener
        .local_addr()
        .map_err(|e| format!("cannot determine the address: {e}"))?;
    let shared = Arc::new(Shared {
        pool,
        handle,
        stratum_port,
    });
    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    crate::log_warn!("cannot accept a pool page connection: {e}");
                    continue;
                }
            };
            let shared = shared.clone();
            tokio::spawn(async move {
                let _ = serve(stream, &shared).await;
            });
        }
    });
    Ok(bound)
}

async fn serve(mut stream: TcpStream, shared: &Shared) -> std::io::Result<()> {
    stream.set_nodelay(true)?;
    let mut buffer = Vec::with_capacity(1024);
    let Some(target) = read_target(&mut stream, &mut buffer).await else {
        return respond(&mut stream, "400 Bad Request", "text/plain", "bad request").await;
    };
    let path = target.split('?').next().unwrap_or("");
    match path {
        "/" => {
            let (view, network) = gather(shared).await;
            let body = render(&view, &network, shared.stratum_port);
            respond(&mut stream, "200 OK", "text/html; charset=utf-8", &body).await
        }
        "/api/stats" => {
            let (view, network) = gather(shared).await;
            let body = serde_json::to_string(&Api {
                network,
                pool: &view,
            })
            .unwrap_or_else(|_| "{}".to_string());
            respond(&mut stream, "200 OK", "application/json", &body).await
        }
        _ => respond(&mut stream, "404 Not Found", "text/plain", "not found").await,
    }
}

async fn gather(shared: &Shared) -> (PoolView, NetworkView) {
    let view = shared.pool.snapshot().await;
    let network = match shared.handle.status().await {
        Ok(status) => NetworkView {
            name: status.network.to_string(),
            height: status.height,
            difficulty: status.next_difficulty,
            hashrate: status.next_difficulty as f64
                / oag_consensus::params::TARGET_BLOCK_TIME_SECS as f64,
        },
        Err(_) => NetworkView {
            name: shared.handle.network().to_string(),
            height: 0,
            difficulty: 0,
            hashrate: 0.0,
        },
    };
    (view, network)
}

async fn respond(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &str,
) -> std::io::Result<()> {
    let head = format!(
        "HTTP/1.1 {status}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\
         X-Content-Type-Options: nosniff\r\n\
         Referrer-Policy: no-referrer\r\n\
         Cache-Control: no-store\r\n\
         Connection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    stream.flush().await
}

/// 頁を組む。**採掘器の名前は採掘器が名乗ったもの**なので、すべて逃がす。
fn render(view: &PoolView, network: &NetworkView, stratum_port: u16) -> String {
    let mut body = String::new();
    let share = if network.hashrate > 0.0 {
        format!("{:.1}%", view.hashrate / network.hashrate * 100.0)
    } else {
        "—".to_string()
    };
    let active = view.miners.iter().filter(|m| m.hashrate > 0.0).count();

    body.push_str("<h1>pool</h1><div class=\"grid\">");
    stat(&mut body, "pool hashrate (10 min)", &rate(view.hashrate));
    stat(&mut body, "network hashrate", &rate(network.hashrate));
    stat(&mut body, "share of network", &share);
    stat(&mut body, "workers online", &view.workers.len().to_string());
    stat(&mut body, "addresses mining", &active.to_string());
    stat(&mut body, "blocks found", &view.found.len().to_string());
    stat(&mut body, "height", &group(network.height));
    stat(&mut body, "fee", &format!("{}%", view.fee_percent));
    stat(
        &mut body,
        "minimum payout",
        &format!("{} OAG", view.min_payout),
    );
    stat(&mut body, "up for", &ago(view.uptime_secs));
    body.push_str("</div>");

    let _ = write!(
        body,
        "<h2>connect</h2>\
         <p>Use the OAG build of XMRig (algorithm <code>rx/oag</code>), with your OAG address \
         as the user:</p>\
         <p><code>xmrig -o &lt;this host&gt;:{stratum_port} -u &lt;your OAG address&gt; \
         -a rx/oag</code></p>\
         <p class=\"note\">Rewards are split among the recent shares (PPLNS, twice the block \
         difficulty) and sent once the block matures, {maturity} blocks later. Until then they \
         are held by the pool's address <span class=\"mono break\">{pool}</span>. \
         Network: {network}.</p>",
        maturity = oag_consensus::params::COINBASE_MATURITY,
        pool = esc(&view.pool_address),
        network = esc(&network.name),
    );

    body.push_str("<h2>workers</h2>");
    if view.workers.is_empty() {
        body.push_str("<p class=\"note\">no miner is connected</p>");
    } else {
        body.push_str(
            "<div class=\"wrap\"><table class=\"list\"><tr class=\"head\"><th>worker</th>\
             <th>address</th><th class=\"num\">hashrate</th><th class=\"num\">shares</th>\
             <th class=\"num\">last share</th><th class=\"num\">connected</th></tr>",
        );
        for w in view.workers.iter().take(MAX_ROWS) {
            let _ = write!(
                body,
                "<tr><td class=\"trunc\">{name}</td>\
                 <td class=\"mono\" data-label=\"address\" title=\"{full}\">{short}</td>\
                 <td class=\"num\" data-label=\"hashrate\">{rate}</td>\
                 <td class=\"num\" data-label=\"shares\">{shares}</td>\
                 <td class=\"num\" data-label=\"last share\">{last}</td>\
                 <td class=\"num\" data-label=\"connected\">{since}</td></tr>",
                name = esc(&w.name),
                full = esc(&w.address),
                short = esc(&shorten(&w.address)),
                rate = rate(w.hashrate),
                shares = group(w.shares),
                last = w
                    .last_share_secs
                    .map_or("—".to_string(), |s| format!("{} ago", ago(s))),
                since = ago(w.connected_secs),
            );
        }
        body.push_str("</table></div>");
    }

    body.push_str("<h2>miners</h2>");
    if view.miners.is_empty() {
        body.push_str("<p class=\"note\">nobody has mined here yet</p>");
    } else {
        body.push_str(
            "<div class=\"wrap\"><table class=\"list\"><tr class=\"head\"><th>address</th>\
             <th class=\"num\">hashrate</th><th class=\"num\">immature</th>\
             <th class=\"num\">balance</th><th class=\"num\">paid</th></tr>",
        );
        for m in view.miners.iter().take(MAX_ROWS) {
            let _ = write!(
                body,
                "<tr><td class=\"mono\" title=\"{full}\">{short}</td>\
                 <td class=\"num\" data-label=\"hashrate\">{rate}</td>\
                 <td class=\"num\" data-label=\"immature\">{immature}</td>\
                 <td class=\"num\" data-label=\"balance\">{balance}</td>\
                 <td class=\"num\" data-label=\"paid\">{paid}</td></tr>",
                full = esc(&m.address),
                short = esc(&shorten(&m.address)),
                rate = rate(m.hashrate),
                immature = esc(&m.immature),
                balance = esc(&m.balance),
                paid = esc(&m.paid),
            );
        }
        body.push_str("</table></div>");
        body.push_str(
            "<p class=\"note\">immature: waiting for the block to mature · balance: matured, \
             paid in the next round once it reaches the minimum</p>",
        );
    }

    body.push_str("<h2>blocks found</h2>");
    if view.found.is_empty() {
        body.push_str("<p class=\"note\">none yet</p>");
    } else {
        body.push_str(
            "<div class=\"wrap\"><table class=\"list\"><tr class=\"head\"><th>height</th>\
             <th>time (UTC)</th><th>found by</th><th class=\"num\">reward</th><th>status</th>\
             <th class=\"wide\">hash</th></tr>",
        );
        for f in view.found.iter().take(MAX_ROWS) {
            let _ = write!(
                body,
                "<tr><td>{height}</td><td data-label=\"time\">{time}</td>\
                 <td class=\"mono\" data-label=\"found by\">{finder}</td>\
                 <td class=\"num\" data-label=\"reward\">{reward} OAG</td>\
                 <td data-label=\"status\">{status}</td>\
                 <td class=\"mono wide\">{hash}</td></tr>",
                height = group(f.height),
                time = utc(f.time),
                finder = esc(&shorten(&f.finder)),
                reward = esc(&atomic_to_oag(f.reward.parse().unwrap_or(0))),
                status = status(&f.status),
                hash = esc(&shorten(&f.hash)),
            );
        }
        body.push_str("</table></div>");
    }

    body.push_str("<h2>payouts</h2>");
    if view.payouts.is_empty() {
        body.push_str("<p class=\"note\">none yet</p>");
    } else {
        body.push_str(
            "<div class=\"wrap\"><table class=\"list\"><tr class=\"head\"><th>height</th>\
             <th>transaction</th><th class=\"num\">recipients</th><th class=\"num\">total</th>\
             <th>status</th></tr>",
        );
        for p in view.payouts.iter().take(MAX_ROWS) {
            let _ = write!(
                body,
                "<tr><td>{height}</td><td class=\"mono\" data-label=\"transaction\" \
                 title=\"{full}\">{short}</td>\
                 <td class=\"num\" data-label=\"recipients\">{count}</td>\
                 <td class=\"num\" data-label=\"total\">{total} OAG</td>\
                 <td data-label=\"status\">{status}</td></tr>",
                height = group(p.height),
                full = esc(&p.txid),
                short = esc(&shorten(&p.txid)),
                count = p.recipients,
                total = esc(&p.total),
                status = if p.confirmed {
                    "<span class=\"ok\">confirmed</span>"
                } else {
                    "<span class=\"warn\">pending</span>"
                },
            );
        }
        body.push_str("</table></div>");
    }

    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
         <meta http-equiv=\"refresh\" content=\"{REFRESH_SECS}\">\
         <title>pool · Orange</title><style>{CSS}</style></head>\
         <body><header><a class=\"brand\" href=\"/\">Orange <span>pool</span></a>\
         <nav><a href=\"/\">status</a> <a href=\"/api/stats\">json</a></nav></header>\
         <main>{body}</main>\
         <footer>The status of this pool, refreshed every {REFRESH_SECS} seconds. \
         It is read-only.</footer></body></html>"
    )
}

fn stat(out: &mut String, label: &str, value: &str) {
    let _ = write!(
        out,
        "<div class=\"stat\"><div class=\"label\">{}</div><div class=\"value\">{}</div></div>",
        esc(label),
        esc(value)
    );
}

fn status(text: &str) -> &'static str {
    match text {
        "matured" => "<span class=\"ok\">matured</span>",
        "orphaned" => "<span class=\"warn\">orphaned</span>",
        _ => "immature",
    }
}

/// ハッシュレートの表記。
fn rate(hashes_per_second: f64) -> String {
    let h = hashes_per_second.max(0.0);
    if h >= 1e9 {
        format!("{:.2} GH/s", h / 1e9)
    } else if h >= 1e6 {
        format!("{:.2} MH/s", h / 1e6)
    } else if h >= 1e3 {
        format!("{:.2} kH/s", h / 1e3)
    } else {
        format!("{h:.0} H/s")
    }
}

/// 経過時間の表記。
fn ago(secs: u64) -> String {
    if secs < 60 {
        format!("{secs} s")
    } else if secs < 3_600 {
        format!("{} min", secs / 60)
    } else if secs < 86_400 {
        format!("{} h {} min", secs / 3_600, secs % 3_600 / 60)
    } else {
        format!("{} d {} h", secs / 86_400, secs % 86_400 / 3_600)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::WorkerView;

    fn view() -> PoolView {
        PoolView {
            pool_address: "oag1qpool".to_string(),
            fee_percent: 1.0,
            min_payout: "1".to_string(),
            hashrate: 1_500.0,
            uptime_secs: 90,
            workers: vec![WorkerView {
                name: "<script>alert(1)</script>".to_string(),
                address: "oag1qminer".to_string(),
                hashrate: 1_500.0,
                shares: 3,
                last_share_secs: Some(5),
                connected_secs: 90,
            }],
            miners: Vec::new(),
            found: Vec::new(),
            payouts: Vec::new(),
            fee_earned: "0".to_string(),
        }
    }

    fn network() -> NetworkView {
        NetworkView {
            name: "mainnet".to_string(),
            height: 17_000,
            difficulty: 600_000,
            hashrate: 10_000.0,
        }
    }

    #[test]
    fn a_worker_name_cannot_inject_markup() {
        // 名前は採掘器が好きに名乗れる。そのまま埋めると頁に script が入る。
        let html = render(&view(), &network(), 1919);
        assert!(
            !html.contains("<script>"),
            "the worker name was not escaped"
        );
        assert!(html.contains("&lt;script&gt;"));
    }

    #[test]
    fn the_page_tells_miners_how_to_connect() {
        let html = render(&view(), &network(), 1919);
        assert!(html.contains(":1919"));
        assert!(html.contains("rx/oag"));
        assert!(html.contains("1.50 kH/s"));
        assert!(
            html.contains("15.0%"),
            "the pool's share of the network is shown"
        );
    }

    #[test]
    fn rates_and_durations_read_naturally() {
        assert_eq!(rate(950.0), "950 H/s");
        assert_eq!(rate(12_340.0), "12.34 kH/s");
        assert_eq!(rate(2_500_000.0), "2.50 MH/s");
        assert_eq!(ago(42), "42 s");
        assert_eq!(ago(125), "2 min");
        assert_eq!(ago(3_700), "1 h 1 min");
        assert_eq!(ago(90_000), "1 d 1 h");
    }
}
