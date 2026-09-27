//! ルーターに頼んで、待ち受けのポートを外へ開けてもらう (UPnP IGD / NAT-PMP)。
//!
//! # なぜ要るのか
//!
//! 家で動くノードの大半はルーターの内側にいる。**こちらから外へは
//! 繋げるが、外からは繋がれない。** 新しく入ってきたノードの繋ぎ先には
//! なれず、ネットワークに参加していても数には入らない。
//!
//! ルーターの多くは、内側の機器から「このポートを自分へ回してくれ」と
//! 頼まれれば応じる。頼み方が UPnP IGD と NAT-PMP の 2 通りある。
//!
//! # 何をするか
//!
//! 1. UPnP で頼む。答えるルーターが無ければ NAT-PMP で頼む
//! 2. 開けてもらえたら、ルーターの外側の住所を**自分の住所として名乗る**
//!    (`--external-addr` を渡したのと同じになる)
//! 3. 期限が切れる前に頼み直す。外側の住所が変わっていれば名乗り直す
//! 4. 終了するときに閉じてもらう
//!
//! **外側の住所が公開の住所でなければ名乗らない。** ルーターの外にもう
//! 1 段 NAT がある (CGNAT) と、ルーターが開けても外からは届かない。
//! 届かない住所を名乗れば、受け取った側に無駄足を踏ませる。
//!
//! # 何をしないか
//!
//! - `--external-addr` が渡されていれば何もしない。運用者の言うことが優先する
//! - 自分の住所がもともと公開の住所 (VPS など) なら何もしない。NAT が無い
//! - regtest では何もしない。手元で試すたびにルーターを触るのは筋が悪い
//!
//! # 信用について
//!
//! ルーターの答えは同じ LAN の誰でも偽れる。偽られて起きるのは
//! 「違う住所を名乗る」ことだけで、名乗った住所は繋がって初めて他の
//! ノードに配られる (`docs/SPEC.md` §14.6)。届かない住所は広まらない。

use crate::addrbook::is_storable;
use crate::service::NodeHandle;
use oag_primitives::Network;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::oneshot;
use tokio::time::timeout;

/// UPnP で頼む期限 (秒)。切れる前に頼み直す。
const UPNP_LEASE: u32 = 3600;
/// NAT-PMP で頼む期限 (秒)。RFC 6886 の推奨値。
const NATPMP_LEASE: u32 = 7200;
/// 期限を持たない (永続の) 割り当てを見直す間隔。ルーターの再起動で
/// 消えることがあり、外側の住所も変わりうる。
const PERMANENT_RECHECK: Duration = Duration::from_secs(30 * 60);
/// 開けられなかったとき、もう一度試すまでの間隔。
const RETRY_AFTER: Duration = Duration::from_secs(30 * 60);
/// SSDP の答えを待つ時間。
const SSDP_WAIT: Duration = Duration::from_millis(2500);
/// HTTP の 1 往復にかける時間の上限。
const HTTP_TIMEOUT: Duration = Duration::from_secs(5);
/// ルーターから読む量の上限。機器の説明は数 KB で足りる。
const HTTP_MAX: usize = 256 * 1024;
/// 割り当てに付ける説明。ルーターの管理画面に出る。
const DESCRIPTION: &str = "Orange (OAG) node";

/// UPnP IGD の SSDP の宛先。
const SSDP_ADDR: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(239, 255, 255, 250), 1900);
/// NAT-PMP のポート。
const NATPMP_PORT: u16 = 5351;

// ======== 起動と停止 ========

/// 動いている割り当ての面倒を見るタスクへの手綱。
pub struct PortMap {
    stop: Option<oneshot::Sender<()>>,
    done: Option<oneshot::Receiver<()>>,
}

impl PortMap {
    /// 割り当てを閉じてもらってから戻る。
    ///
    /// ルーターが答えなくても数秒で諦める。**終了を待たせない。**
    /// 閉じ損ねた割り当ても、期限付きなら 1 時間ほどで消える。
    pub async fn stop(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(done) = self.done.take() {
            let _ = timeout(Duration::from_secs(5), done).await;
        }
    }
}

/// 割り当てを試みるべきか。試みないなら、その理由を返す。
///
/// 理由は利用者に見せる。**黙って何もしないことはしない。**
pub fn should_try(network: Network, listen: Option<SocketAddr>) -> Result<(), &'static str> {
    if network == Network::Regtest {
        return Err("regtest");
    }
    if let Some(addr) = listen {
        match addr.ip() {
            IpAddr::V4(v4) if v4.is_loopback() => return Err("listening on loopback only"),
            IpAddr::V6(_) => return Err("listening on IPv6 only"),
            _ => {}
        }
    }
    Ok(())
}

/// ポートの割り当てを始める。
///
/// 待ち受けている `port` を、外側の同じ番号で開けてもらう。すぐ戻り、
/// 頼むのは裏で行う。結果は記録に出る。
pub fn start(
    handle: NodeHandle,
    network: Network,
    listen: Option<SocketAddr>,
    port: u16,
) -> PortMap {
    let bind = match listen.map(|a| a.ip()) {
        Some(IpAddr::V4(v4)) if !v4.is_unspecified() => Some(v4),
        _ => None,
    };
    spawn(move |stop| async move {
        let Some(local) = local_address(network, bind, port).await else {
            return;
        };
        run(handle, network, local, port, None, stop).await;
    })
}

/// 頼む相手を決め打ちにして始める。
///
/// 探索 (SSDP、経路表) を通らない。自分の住所も確かめない。試験で偽の
/// ルーターを相手にするためのものである。
pub fn start_with(
    handle: NodeHandle,
    network: Network,
    local: Ipv4Addr,
    port: u16,
    method: Method,
) -> PortMap {
    spawn(move |stop| run(handle, network, local, port, Some(method), stop))
}

fn spawn<F, Fut>(task: F) -> PortMap
where
    F: FnOnce(oneshot::Receiver<()>) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let (stop_tx, stop_rx) = oneshot::channel();
    let (done_tx, done_rx) = oneshot::channel();
    let fut = task(stop_rx);
    tokio::spawn(async move {
        fut.await;
        let _ = done_tx.send(());
    });
    PortMap {
        stop: Some(stop_tx),
        done: Some(done_rx),
    }
}

/// ルーターに告げる自分の住所。頼む意味が無ければ `None`。
async fn local_address(network: Network, bind: Option<Ipv4Addr>, port: u16) -> Option<Ipv4Addr> {
    let local = match bind {
        Some(ip) => ip,
        None => match local_ipv4().await {
            Some(ip) => ip,
            None => {
                crate::log_peer!("port mapping: no IPv4 network found, so not asking the router");
                return None;
            }
        },
    };
    // **NAT の内側にいなければ頼む相手がいない。** VPS がこれに当たる。
    // 自分の住所が公開のものなら、それを名乗る方法を教えて終わる。
    // 勝手には名乗らない。口が防火壁で塞がれていても、ここからは見えない。
    if !local.is_private() && !is_cgnat(local) {
        if is_storable(network, &SocketAddr::new(IpAddr::V4(local), port)) {
            crate::log_peer!(
                "port mapping: this machine has a public address, so there is no router to ask. \
                 To be found by other nodes, pass --external-addr {local}:{port}"
            );
        }
        return None;
    }
    Some(local)
}

async fn run(
    handle: NodeHandle,
    network: Network,
    local: Ipv4Addr,
    port: u16,
    fixed: Option<Method>,
    mut stop: oneshot::Receiver<()>,
) {
    let mut current: Option<Mapping> = None;
    let mut announced: Option<SocketAddr> = None;
    let mut reported_failure = false;
    loop {
        let wait = match refresh(current.as_ref(), fixed.as_ref(), local, port).await {
            Ok(mapping) => {
                reported_failure = false;
                let external = mapping.external;
                if is_storable(network, &SocketAddr::V4(external)) {
                    if announced != Some(SocketAddr::V4(external)) {
                        crate::log_peer!(
                            "the router ({}) forwards {} to this node; announcing it as our address \
                             (pass --no-portmap to stop this)",
                            mapping.method.name(),
                            external
                        );
                        if handle
                            .set_own_addresses(vec![SocketAddr::V4(external)])
                            .await
                            .is_err()
                        {
                            break;
                        }
                        announced = Some(SocketAddr::V4(external));
                    }
                } else {
                    // ルーターの外にもう 1 段 NAT がある。開けても届かない。
                    crate::log_warn!(
                        "the router's outside address is {}, which is not public (the line is \
                         probably behind the provider's NAT). Other nodes cannot reach this one; \
                         it still works, it just will not be found",
                        external.ip()
                    );
                    mapping.remove().await;
                    if announced.take().is_some()
                        && handle.set_own_addresses(Vec::new()).await.is_err()
                    {
                        break;
                    }
                    current = None;
                    tokio::select! {
                        _ = tokio::time::sleep(RETRY_AFTER) => continue,
                        _ = &mut stop => return,
                    }
                }
                let wait = mapping.renew_after();
                current = Some(mapping);
                wait
            }
            Err(e) => {
                if !reported_failure {
                    crate::log_peer!(
                        "port mapping: {e}. This node still works, but others cannot connect to it \
                         unless port {port} is forwarded by hand"
                    );
                    reported_failure = true;
                }
                if announced.take().is_some() && handle.set_own_addresses(Vec::new()).await.is_err()
                {
                    break;
                }
                current = None;
                RETRY_AFTER
            }
        };
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = &mut stop => break,
        }
    }
    if let Some(mapping) = current {
        mapping.remove().await;
    }
}

/// 割り当てを取る。前回の相手が答えればそこで頼み直し、答えなければ
/// 探し直す。
async fn refresh(
    previous: Option<&Mapping>,
    fixed: Option<&Method>,
    local: Ipv4Addr,
    port: u16,
) -> Result<Mapping, String> {
    if let Some(method) = fixed {
        return method.map(local, port).await;
    }
    if let Some(mapping) = previous {
        if let Ok(renewed) = mapping.method.map(local, port).await {
            return Ok(renewed);
        }
    }
    let upnp = match Upnp::discover(local).await {
        Ok(upnp) => match Method::Upnp(upnp).map(local, port).await {
            Ok(mapping) => return Ok(mapping),
            Err(e) => e,
        },
        Err(e) => e,
    };
    let gateway = default_gateway(local);
    match Method::NatPmp(SocketAddr::V4(SocketAddrV4::new(gateway, NATPMP_PORT)))
        .map(local, port)
        .await
    {
        Ok(mapping) => Ok(mapping),
        Err(natpmp) => Err(format!("UPnP: {upnp}; NAT-PMP: {natpmp}")),
    }
}

// ======== 割り当て ========

/// ルーターが引き受けた割り当て。
#[derive(Debug, Clone)]
pub struct Mapping {
    /// 頼んだ相手と頼み方。
    pub method: Method,
    /// ルーターの外側の住所と、そこで開いたポート。
    pub external: SocketAddrV4,
    /// こちら側のポート。
    pub internal: u16,
    /// 期限 (秒)。0 は期限なし。
    pub lease: u32,
}

impl Mapping {
    /// 次に頼み直すまでの時間。
    fn renew_after(&self) -> Duration {
        if self.lease == 0 {
            PERMANENT_RECHECK
        } else {
            // 期限の半分で頼み直す。1 度しくじっても間に合う。
            Duration::from_secs(u64::from(self.lease / 2).max(60))
        }
    }

    /// 閉じてもらう。答えなくても構わない。
    pub async fn remove(&self) {
        let result = match &self.method {
            Method::Upnp(upnp) => upnp.delete(self.external.port()).await,
            Method::NatPmp(gateway) => natpmp_map(*gateway, self.internal, 0, 0).await.map(|_| ()),
        };
        if result.is_ok() {
            crate::log_peer!("asked the router to close port {}", self.external.port());
        }
    }
}

/// 頼む相手と頼み方。
#[derive(Debug, Clone)]
pub enum Method {
    /// UPnP IGD。
    Upnp(Upnp),
    /// NAT-PMP。ルーターの住所 (ポート 5351)。
    NatPmp(SocketAddr),
}

impl Method {
    fn name(&self) -> &'static str {
        match self {
            Method::Upnp(_) => "UPnP",
            Method::NatPmp(_) => "NAT-PMP",
        }
    }

    /// `port` を外側の同じ番号で開けてもらう。
    pub async fn map(&self, local: Ipv4Addr, port: u16) -> Result<Mapping, String> {
        match self {
            Method::Upnp(upnp) => {
                let lease = upnp.add(local, port).await?;
                let ip = upnp.external_ip().await?;
                Ok(Mapping {
                    method: self.clone(),
                    external: SocketAddrV4::new(ip, port),
                    internal: port,
                    lease,
                })
            }
            Method::NatPmp(gateway) => {
                let ip = natpmp_external_ip(*gateway).await?;
                let (external_port, lease) = natpmp_map(*gateway, port, port, NATPMP_LEASE).await?;
                Ok(Mapping {
                    method: self.clone(),
                    external: SocketAddrV4::new(ip, external_port),
                    internal: port,
                    lease,
                })
            }
        }
    }
}

// ======== UPnP IGD ========

/// 見つけたルーターの、ポートを開ける口。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Upnp {
    /// 口の住所。
    pub host: SocketAddr,
    /// 口のパス。
    pub path: String,
    /// サービスの種類 (`urn:schemas-upnp-org:service:WANIPConnection:1` など)。
    pub service: String,
}

/// 頼めるサービスの種類。**上から順に選ぶ。**
const WAN_SERVICES: &[&str] = &[
    "urn:schemas-upnp-org:service:WANIPConnection:2",
    "urn:schemas-upnp-org:service:WANIPConnection:1",
    "urn:schemas-upnp-org:service:WANPPPConnection:1",
];

/// UPnP の「期限付きの割り当てには応じない」。永続で頼み直す。
const ONLY_PERMANENT_LEASES: u32 = 725;

impl Upnp {
    /// SSDP で LAN にルーターを探し、ポートを開ける口を見つける。
    pub async fn discover(local: Ipv4Addr) -> Result<Upnp, String> {
        let socket = UdpSocket::bind(SocketAddrV4::new(local, 0))
            .await
            .map_err(|e| format!("cannot open a UDP socket: {e}"))?;
        for target in [
            "urn:schemas-upnp-org:device:InternetGatewayDevice:2",
            "urn:schemas-upnp-org:device:InternetGatewayDevice:1",
        ] {
            let search = format!(
                "M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\n\
                 MAN: \"ssdp:discover\"\r\nMX: 2\r\nST: {target}\r\n\r\n"
            );
            socket
                .send_to(search.as_bytes(), SSDP_ADDR)
                .await
                .map_err(|e| format!("cannot send the search: {e}"))?;
        }

        let mut seen = Vec::new();
        let mut last_error = String::from("no router answered");
        let deadline = tokio::time::Instant::now() + SSDP_WAIT;
        let mut buf = [0u8; 2048];
        loop {
            let received = tokio::time::timeout_at(deadline, socket.recv_from(&mut buf)).await;
            let Ok(Ok((n, _from))) = received else { break };
            let Some(location) = ssdp_location(&buf[..n]) else {
                continue;
            };
            if seen.contains(&location) {
                continue;
            }
            seen.push(location.clone());
            match Upnp::from_location(&location).await {
                Ok(upnp) => return Ok(upnp),
                Err(e) => last_error = e,
            }
        }
        Err(last_error)
    }

    /// 機器の説明を読み、ポートを開ける口を探す。
    pub async fn from_location(location: &str) -> Result<Upnp, String> {
        let (host, path) = parse_url(location)?;
        // ルーターは LAN の中にいる。外の住所を名乗る答えは信じない。
        if is_storable(Network::Mainnet, &host) {
            return Err(format!("{host} is not on the local network"));
        }
        let (status, body) = http(
            host,
            &format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"),
        )
        .await?;
        if status != 200 {
            return Err(format!("the device description answered {status}"));
        }
        let description = String::from_utf8_lossy(&body);
        let (service, control) =
            find_wan_service(&description).ok_or("the router offers no WAN connection service")?;
        let base = tag_text(&description, "URLBase").unwrap_or_else(|| location.to_string());
        let (host, path) = resolve(&base, host, &control)?;
        if is_storable(Network::Mainnet, &host) {
            return Err(format!("{host} is not on the local network"));
        }
        Ok(Upnp {
            host,
            path,
            service,
        })
    }

    /// SOAP の手続きを 1 つ呼ぶ。
    async fn call(&self, action: &str, args: &[(&str, String)]) -> Result<String, UpnpError> {
        let mut inner = String::new();
        for (name, value) in args {
            inner.push_str(&format!("<{name}>{}</{name}>", xml_escape(value)));
        }
        let body = format!(
            "<?xml version=\"1.0\"?>\r\n\
             <s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" \
             s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">\
             <s:Body><u:{action} xmlns:u=\"{service}\">{inner}</u:{action}></s:Body></s:Envelope>\r\n",
            service = self.service
        );
        let request = format!(
            "POST {path} HTTP/1.1\r\nHost: {host}\r\n\
             Content-Type: text/xml; charset=\"utf-8\"\r\n\
             SOAPAction: \"{service}#{action}\"\r\n\
             Content-Length: {len}\r\nConnection: close\r\n\r\n{body}",
            path = self.path,
            host = self.host,
            service = self.service,
            len = body.len()
        );
        let (status, response) = http(self.host, &request).await.map_err(UpnpError::Io)?;
        let text = String::from_utf8_lossy(&response).into_owned();
        if status == 200 {
            return Ok(text);
        }
        match tag_text(&text, "errorCode").and_then(|c| c.trim().parse().ok()) {
            Some(code) => Err(UpnpError::Fault(
                code,
                tag_text(&text, "errorDescription").unwrap_or_default(),
            )),
            None => Err(UpnpError::Io(format!("{action} answered {status}"))),
        }
    }

    /// 開けてもらう。受けてもらえた期限を返す (0 は期限なし)。
    pub async fn add(&self, local: Ipv4Addr, port: u16) -> Result<u32, String> {
        let args = |lease: u32| {
            vec![
                ("NewRemoteHost", String::new()),
                ("NewExternalPort", port.to_string()),
                ("NewProtocol", "TCP".to_string()),
                ("NewInternalPort", port.to_string()),
                ("NewInternalClient", local.to_string()),
                ("NewEnabled", "1".to_string()),
                ("NewPortMappingDescription", DESCRIPTION.to_string()),
                ("NewLeaseDuration", lease.to_string()),
            ]
        };
        match self.call("AddPortMapping", &args(UPNP_LEASE)).await {
            Ok(_) => Ok(UPNP_LEASE),
            // 期限付きに応じない機器がある。永続で頼み直し、終了時に閉じる。
            Err(UpnpError::Fault(ONLY_PERMANENT_LEASES, _)) => self
                .call("AddPortMapping", &args(0))
                .await
                .map(|_| 0)
                .map_err(|e| e.describe(port)),
            Err(e) => Err(e.describe(port)),
        }
    }

    /// ルーターの外側の住所を聞く。
    pub async fn external_ip(&self) -> Result<Ipv4Addr, String> {
        let text = self
            .call("GetExternalIPAddress", &[])
            .await
            .map_err(|e| e.describe(0))?;
        let ip = tag_text(&text, "NewExternalIPAddress")
            .ok_or("the router did not say its outside address")?;
        let ip: Ipv4Addr = ip
            .trim()
            .parse()
            .map_err(|_| format!("the router's outside address {ip:?} cannot be read"))?;
        if ip.is_unspecified() {
            return Err("the router is not connected to the internet".into());
        }
        Ok(ip)
    }

    /// 閉じてもらう。
    pub async fn delete(&self, port: u16) -> Result<(), String> {
        self.call(
            "DeletePortMapping",
            &[
                ("NewRemoteHost", String::new()),
                ("NewExternalPort", port.to_string()),
                ("NewProtocol", "TCP".to_string()),
            ],
        )
        .await
        .map(|_| ())
        .map_err(|e| e.describe(port))
    }
}

#[derive(Debug)]
enum UpnpError {
    Io(String),
    Fault(u32, String),
}

impl UpnpError {
    fn describe(&self, port: u16) -> String {
        match self {
            UpnpError::Io(e) => e.clone(),
            // 同じ番号を LAN の別の機器が使っている。
            UpnpError::Fault(718, _) => {
                format!("port {port} is already forwarded to another device")
            }
            UpnpError::Fault(code, text) if text.is_empty() => {
                format!("the router refused ({code})")
            }
            UpnpError::Fault(code, text) => format!("the router refused ({code} {text})"),
        }
    }
}

/// SSDP の答えから `LOCATION` を取り出す。
pub fn ssdp_location(response: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(response).ok()?;
    let mut lines = text.split("\r\n");
    let status = lines.next()?;
    if !status.starts_with("HTTP/1.") || !status.contains(" 200") {
        return None;
    }
    for line in lines {
        let (name, value) = line.split_once(':')?;
        if name.trim().eq_ignore_ascii_case("location") {
            return Some(value.trim().to_string());
        }
    }
    None
}

/// 機器の説明から、頼めるサービスとその口のパスを探す。
pub fn find_wan_service(description: &str) -> Option<(String, String)> {
    let mut found: Vec<(usize, String, String)> = Vec::new();
    let mut rest = description;
    while let Some(start) = rest.find("<service>") {
        let after = &rest[start + "<service>".len()..];
        let Some(end) = after.find("</service>") else {
            break;
        };
        let block = &after[..end];
        if let (Some(kind), Some(control)) = (
            tag_text(block, "serviceType"),
            tag_text(block, "controlURL"),
        ) {
            let kind = kind.trim().to_string();
            if let Some(rank) = WAN_SERVICES.iter().position(|s| *s == kind) {
                found.push((rank, kind, control.trim().to_string()));
            }
        }
        rest = &after[end..];
    }
    found.sort_by_key(|(rank, _, _)| *rank);
    found
        .into_iter()
        .next()
        .map(|(_, kind, control)| (kind, control))
}

/// `<name>…</name>` の中身。名前空間の接頭辞 (`<s:name>`) も受ける。
fn tag_text(text: &str, name: &str) -> Option<String> {
    let mut rest = text;
    while let Some(open) = rest.find('<') {
        let after = &rest[open + 1..];
        let close = after.find('>')?;
        let tag = &after[..close];
        // 属性 (`xmlns:u="…"`) を落としてから、接頭辞を落とす。
        let tag_name = tag.split_whitespace().next().unwrap_or("");
        let bare = tag_name.rsplit(':').next().unwrap_or(tag_name);
        if !tag.starts_with('/') && !tag.ends_with('/') && bare == name {
            let body = &after[close + 1..];
            let end = body.find("</")?;
            return Some(xml_unescape(&body[..end]));
        }
        rest = &after[close + 1..];
    }
    None
}

fn xml_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn xml_unescape(text: &str) -> String {
    text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

/// `http://host:port/path` を分ける。**名前ではなく IP の住所だけ受ける。**
/// ルーターは IP で名乗る。名前を引きに行くと、LAN の外へ問い合わせが漏れる。
pub fn parse_url(url: &str) -> Result<(SocketAddr, String), String> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| format!("{url} is not an http address"))?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let host: SocketAddr = if authority.contains(':') {
        authority.parse()
    } else {
        format!("{authority}:80").parse()
    }
    .map_err(|_| format!("{url} does not name an IP address"))?;
    Ok((host, path.to_string()))
}

/// 口のパスを、機器の説明の置き場を起点に解く。
fn resolve(
    base: &str,
    fallback: SocketAddr,
    control: &str,
) -> Result<(SocketAddr, String), String> {
    if control.starts_with("http://") {
        return parse_url(control);
    }
    let host = parse_url(base).map(|(h, _)| h).unwrap_or(fallback);
    let path = if control.starts_with('/') {
        control.to_string()
    } else {
        format!("/{control}")
    };
    Ok((host, path))
}

/// HTTP を 1 往復する。状態と本文を返す。
async fn http(host: SocketAddr, request: &str) -> Result<(u16, Vec<u8>), String> {
    timeout(HTTP_TIMEOUT, async {
        let mut stream = TcpStream::connect(host)
            .await
            .map_err(|e| format!("cannot connect to {host}: {e}"))?;
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| format!("cannot write to {host}: {e}"))?;
        let mut raw = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            let n = stream
                .read(&mut buf)
                .await
                .map_err(|e| format!("cannot read from {host}: {e}"))?;
            if n == 0 {
                break;
            }
            raw.extend_from_slice(&buf[..n]);
            if raw.len() > HTTP_MAX {
                return Err(format!("{host} sent too much"));
            }
        }
        parse_http(&raw)
    })
    .await
    .map_err(|_| format!("{host} did not answer in time"))?
}

/// HTTP の応答を状態と本文に分ける。chunked も解く。
pub fn parse_http(raw: &[u8]) -> Result<(u16, Vec<u8>), String> {
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or("the answer has no header")?;
    let head = std::str::from_utf8(&raw[..split]).map_err(|_| "the header is not text")?;
    let body = &raw[split + 4..];
    let mut lines = head.split("\r\n");
    let status: u16 = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .ok_or("the status line cannot be read")?;
    let mut chunked = false;
    let mut length = None;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim();
            if name.eq_ignore_ascii_case("transfer-encoding")
                && value.to_ascii_lowercase().contains("chunked")
            {
                chunked = true;
            } else if name.eq_ignore_ascii_case("content-length") {
                length = value.trim().parse::<usize>().ok();
            }
        }
    }
    let body = if chunked {
        dechunk(body)?
    } else if let Some(n) = length {
        body.get(..n)
            .ok_or("the body is shorter than it said")?
            .to_vec()
    } else {
        body.to_vec()
    };
    Ok((status, body))
}

fn dechunk(mut body: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    loop {
        let line_end = body
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or("a chunk has no size line")?;
        let size_text =
            std::str::from_utf8(&body[..line_end]).map_err(|_| "a chunk size is not text")?;
        let size_text = size_text.split(';').next().unwrap_or("").trim();
        let size =
            usize::from_str_radix(size_text, 16).map_err(|_| "a chunk size cannot be read")?;
        body = &body[line_end + 2..];
        if size == 0 {
            return Ok(out);
        }
        let chunk = body.get(..size).ok_or("a chunk is cut short")?;
        out.extend_from_slice(chunk);
        body = body.get(size + 2..).ok_or("a chunk is cut short")?;
    }
}

// ======== NAT-PMP (RFC 6886) ========

/// ルーターの外側の住所を聞く。
pub async fn natpmp_external_ip(gateway: SocketAddr) -> Result<Ipv4Addr, String> {
    let answer = natpmp_ask(gateway, &[0, 0], 12).await?;
    if answer[1] != 128 {
        return Err("the router answered something else".into());
    }
    natpmp_result(&answer)?;
    Ok(Ipv4Addr::new(answer[8], answer[9], answer[10], answer[11]))
}

/// TCP の割り当てを頼む。`lease` が 0 なら閉じてもらう。
/// 開いた外側のポートと、受けてもらえた期限を返す。
pub async fn natpmp_map(
    gateway: SocketAddr,
    internal: u16,
    external: u16,
    lease: u32,
) -> Result<(u16, u32), String> {
    let mut request = vec![0, 2, 0, 0];
    request.extend_from_slice(&internal.to_be_bytes());
    request.extend_from_slice(&external.to_be_bytes());
    request.extend_from_slice(&lease.to_be_bytes());
    let answer = natpmp_ask(gateway, &request, 16).await?;
    if answer[1] != 130 {
        return Err("the router answered something else".into());
    }
    natpmp_result(&answer)?;
    let mapped = u16::from_be_bytes([answer[10], answer[11]]);
    let granted = u32::from_be_bytes([answer[12], answer[13], answer[14], answer[15]]);
    Ok((mapped, granted))
}

fn natpmp_result(answer: &[u8]) -> Result<(), String> {
    match u16::from_be_bytes([answer[2], answer[3]]) {
        0 => Ok(()),
        2 => Err("the router has NAT-PMP turned off".into()),
        3 => Err("the router is not connected to the internet".into()),
        code => Err(format!("the router refused ({code})")),
    }
}

/// 1 つ頼み、答えを待つ。RFC どおり、待ち時間を倍にしながら数回送る。
async fn natpmp_ask(gateway: SocketAddr, request: &[u8], len: usize) -> Result<Vec<u8>, String> {
    let bind: SocketAddr = match gateway {
        SocketAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        SocketAddr::V6(_) => return Err("NAT-PMP is IPv4 only".into()),
    };
    let socket = UdpSocket::bind(bind)
        .await
        .map_err(|e| format!("cannot open a UDP socket: {e}"))?;
    socket
        .connect(gateway)
        .await
        .map_err(|e| format!("cannot reach {gateway}: {e}"))?;
    let mut wait = Duration::from_millis(250);
    let mut buf = [0u8; 64];
    for _ in 0..4 {
        // 届かない (ICMP で断られる) なら、何度送っても同じ。
        if socket.send(request).await.is_err() {
            break;
        }
        if let Ok(Ok(n)) = timeout(wait, socket.recv(&mut buf)).await {
            if n >= len && buf[0] == 0 {
                return Ok(buf[..n].to_vec());
            }
        }
        wait *= 2;
    }
    Err("no router answered".into())
}

// ======== 自分の住所 ========

/// 外へ出るときに使う自分の IPv4 の住所。
///
/// UDP のソケットを外の住所へ「繋ぐ」と、経路表から出口が決まる。
/// **パケットは何も送らない。** 宛先は文書用の住所 (RFC 5737) にしてある。
async fn local_ipv4() -> Option<Ipv4Addr> {
    let socket = UdpSocket::bind("0.0.0.0:0").await.ok()?;
    socket.connect("198.51.100.1:9").await.ok()?;
    match socket.local_addr().ok()?.ip() {
        IpAddr::V4(v4) if !v4.is_unspecified() && !v4.is_loopback() => Some(v4),
        _ => None,
    }
}

fn is_cgnat(ip: Ipv4Addr) -> bool {
    ip.octets()[0] == 100 && (ip.octets()[1] & 0xc0) == 64
}

/// ルーターの住所。
///
/// Linux は経路表から読む。ほかは「自分の住所の末尾を 1 にしたもの」と
/// みなす。家庭のルーターはほぼこれである。**外れても害は無い。** 答えが
/// 返らず、NAT-PMP を諦めるだけである。
fn default_gateway(local: Ipv4Addr) -> Ipv4Addr {
    if let Ok(table) = std::fs::read_to_string("/proc/net/route") {
        if let Some(gateway) = gateway_from_route_table(&table) {
            return gateway;
        }
    }
    let [a, b, c, _] = local.octets();
    Ipv4Addr::new(a, b, c, 1)
}

/// `/proc/net/route` から既定の経路の行き先を読む。
pub fn gateway_from_route_table(table: &str) -> Option<Ipv4Addr> {
    for line in table.lines().skip(1) {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() > 2 && fields[1] == "00000000" {
            let raw = u32::from_str_radix(fields[2], 16).ok()?;
            if raw != 0 {
                // 経路表はホストのバイト順 (x86 と ARM ではリトルエンディアン)。
                return Some(Ipv4Addr::from(raw.to_le_bytes()));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_location_is_read_from_an_ssdp_answer() {
        let answer = b"HTTP/1.1 200 OK\r\nCACHE-CONTROL: max-age=120\r\n\
                       ST: urn:schemas-upnp-org:device:InternetGatewayDevice:1\r\n\
                       Location: http://192.168.1.1:5000/rootDesc.xml\r\n\r\n";
        assert_eq!(
            ssdp_location(answer).as_deref(),
            Some("http://192.168.1.1:5000/rootDesc.xml")
        );
        assert_eq!(
            ssdp_location(b"NOTIFY * HTTP/1.1\r\nLOCATION: x\r\n\r\n"),
            None
        );
    }

    #[test]
    fn the_best_wan_service_is_chosen() {
        let description = r#"<?xml version="1.0"?>
<root><device><serviceList>
<service><serviceType>urn:schemas-upnp-org:service:Layer3Forwarding:1</serviceType><controlURL>/ctl/L3F</controlURL></service>
</serviceList><deviceList><device><deviceList><device><serviceList>
<service>
  <serviceType>urn:schemas-upnp-org:service:WANPPPConnection:1</serviceType>
  <controlURL>/ctl/PPP</controlURL>
</service>
<service>
  <serviceType>urn:schemas-upnp-org:service:WANIPConnection:1</serviceType>
  <controlURL>/ctl/IPConn</controlURL>
</service>
</serviceList></device></deviceList></device></deviceList></device></root>"#;
        assert_eq!(
            find_wan_service(description),
            Some((
                "urn:schemas-upnp-org:service:WANIPConnection:1".to_string(),
                "/ctl/IPConn".to_string()
            ))
        );
        assert_eq!(find_wan_service("<root></root>"), None);
    }

    #[test]
    fn tags_are_found_with_or_without_a_prefix() {
        let soap = "<s:Envelope><s:Body><u:GetExternalIPAddressResponse>\
                    <NewExternalIPAddress>203.0.113.7</NewExternalIPAddress>\
                    </u:GetExternalIPAddressResponse></s:Body></s:Envelope>";
        assert_eq!(
            tag_text(soap, "NewExternalIPAddress").as_deref(),
            Some("203.0.113.7")
        );
        let fault = "<s:Fault><detail><UPnPError><errorCode>725</errorCode>\
                     <errorDescription>OnlyPermanentLeasesSupported</errorDescription></UPnPError></detail></s:Fault>";
        assert_eq!(tag_text(fault, "errorCode").as_deref(), Some("725"));
        assert_eq!(tag_text("<a:Body>x</a:Body>", "Body").as_deref(), Some("x"));
        let response = "<u:AddPortMappingResponse xmlns:u=\"urn:x\"><NewRemoteHost/>\
                        <Value>1</Value></u:AddPortMappingResponse>";
        assert!(tag_text(response, "AddPortMappingResponse").is_some());
        assert_eq!(tag_text(response, "NewRemoteHost"), None);
        assert_eq!(tag_text(response, "Value").as_deref(), Some("1"));
    }

    #[test]
    fn urls_must_name_an_ip_address() {
        assert_eq!(
            parse_url("http://192.168.0.1:49152/desc.xml").unwrap(),
            (
                "192.168.0.1:49152".parse().unwrap(),
                "/desc.xml".to_string()
            )
        );
        assert_eq!(
            parse_url("http://10.0.0.1").unwrap(),
            ("10.0.0.1:80".parse().unwrap(), "/".to_string())
        );
        assert!(parse_url("http://router.local/desc.xml").is_err());
        assert!(parse_url("https://192.168.0.1/desc.xml").is_err());
    }

    #[test]
    fn control_paths_are_resolved_against_the_description() {
        let fallback: SocketAddr = "192.168.0.1:5000".parse().unwrap();
        assert_eq!(
            resolve(
                "http://192.168.0.1:5000/rootDesc.xml",
                fallback,
                "/ctl/IPConn"
            )
            .unwrap(),
            (fallback, "/ctl/IPConn".to_string())
        );
        assert_eq!(
            resolve(
                "http://192.168.0.1:5000/rootDesc.xml",
                fallback,
                "ctl/IPConn"
            )
            .unwrap(),
            (fallback, "/ctl/IPConn".to_string())
        );
        assert_eq!(
            resolve("", fallback, "http://192.168.0.254:80/c").unwrap(),
            ("192.168.0.254:80".parse().unwrap(), "/c".to_string())
        );
    }

    #[test]
    fn http_answers_are_split_and_dechunked() {
        let plain = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello-extra";
        assert_eq!(parse_http(plain).unwrap(), (200, b"hello".to_vec()));
        let chunked = b"HTTP/1.1 500 Internal Server Error\r\nTransfer-Encoding: chunked\r\n\r\n\
                        4\r\nhell\r\n1;ext\r\no\r\n0\r\n\r\n";
        assert_eq!(parse_http(chunked).unwrap(), (500, b"hello".to_vec()));
        let until_close = b"HTTP/1.0 200 OK\r\n\r\nbody";
        assert_eq!(parse_http(until_close).unwrap(), (200, b"body".to_vec()));
        assert!(parse_http(b"garbage").is_err());
    }

    #[test]
    fn the_default_route_is_read_from_the_route_table() {
        let table = "Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\n\
                     eth0\t00000000\t0101A8C0\t0003\t0\t0\t100\t00000000\n\
                     eth0\t0001A8C0\t00000000\t0001\t0\t0\t100\t00FFFFFF\n";
        assert_eq!(
            gateway_from_route_table(table),
            Some(Ipv4Addr::new(192, 168, 1, 1))
        );
        assert_eq!(
            gateway_from_route_table("Iface\tDestination\tGateway\n"),
            None
        );
    }

    #[test]
    fn nothing_is_tried_where_it_cannot_help() {
        assert!(should_try(Network::Regtest, None).is_err());
        assert!(should_try(Network::Mainnet, Some("127.0.0.1:9444".parse().unwrap())).is_err());
        assert!(should_try(Network::Mainnet, Some("[::]:9444".parse().unwrap())).is_err());
        assert!(should_try(Network::Mainnet, None).is_ok());
        assert!(should_try(Network::Mainnet, Some("0.0.0.0:9444".parse().unwrap())).is_ok());
    }

    #[test]
    fn a_permanent_mapping_is_rechecked_and_a_leased_one_renewed_at_half() {
        let mapping = |lease| Mapping {
            method: Method::NatPmp("192.168.1.1:5351".parse().unwrap()),
            external: SocketAddrV4::new(Ipv4Addr::new(203, 0, 113, 7), 9444),
            internal: 9444,
            lease,
        };
        assert_eq!(mapping(0).renew_after(), PERMANENT_RECHECK);
        assert_eq!(mapping(3600).renew_after(), Duration::from_secs(1800));
        assert_eq!(mapping(10).renew_after(), Duration::from_secs(60));
    }
}
