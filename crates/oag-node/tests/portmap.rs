//! ルーターに頼む往復を、偽のルーターを相手に通す。
//!
//! 本物のルーターは試験の場に無い。UPnP は HTTP、NAT-PMP は UDP なので、
//! 手元に同じ口を立てれば、頼み方と答えの読み方をそのまま確かめられる。
//! SSDP の探索 (マルチキャスト) だけはここでは通さない。

use oag_node::portmap::{natpmp_external_ip, natpmp_map, start_with, Method, Upnp};
use oag_node::service::{NodeHandle, NodeService};
use oag_primitives::Network;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};

const SERVICE: &str = "urn:schemas-upnp-org:service:WANIPConnection:1";

fn description() -> String {
    format!(
        "<?xml version=\"1.0\"?><root xmlns=\"urn:schemas-upnp-org:device-1-0\"><device>\
         <deviceType>urn:schemas-upnp-org:device:InternetGatewayDevice:1</deviceType>\
         <deviceList><device><deviceList><device><serviceList><service>\
         <serviceType>{SERVICE}</serviceType><serviceId>urn:upnp-org:serviceId:WANIPConn1</serviceId>\
         <controlURL>/ctl/IPConn</controlURL><eventSubURL>/evt/IPConn</eventSubURL>\
         <SCPDURL>/WANIPCn.xml</SCPDURL></service></serviceList></device></deviceList>\
         </device></deviceList></device></root>"
    )
}

fn fault(code: u32, text: &str) -> String {
    format!(
        "<?xml version=\"1.0\"?><s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\">\
         <s:Body><s:Fault><faultcode>s:Client</faultcode><faultstring>UPnPError</faultstring>\
         <detail><UPnPError xmlns=\"urn:schemas-upnp-org:control-1-0\"><errorCode>{code}</errorCode>\
         <errorDescription>{text}</errorDescription></UPnPError></detail></s:Fault></s:Body></s:Envelope>"
    )
}

fn ok(action: &str, inner: &str) -> String {
    format!(
        "<?xml version=\"1.0\"?><s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\">\
         <s:Body><u:{action}Response xmlns:u=\"{SERVICE}\">{inner}</u:{action}Response></s:Body></s:Envelope>"
    )
}

/// 偽のルーター。期限付きの割り当てには応じない (725) 機器を真似る。
/// 受け取った手続きの名前と本文を覚えておく。
async fn fake_igd(permanent_only: bool) -> (SocketAddr, Arc<Mutex<Vec<(String, String)>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let log = log.clone();
            tokio::spawn(async move {
                let mut raw = Vec::new();
                let mut buf = [0u8; 4096];
                // 頭と、Content-Length の分の本文まで読む。
                let (head, body) = loop {
                    let n = stream.read(&mut buf).await.unwrap();
                    raw.extend_from_slice(&buf[..n]);
                    if let Some(i) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&raw[..i]).into_owned();
                        let len = head
                            .lines()
                            .find_map(|l| {
                                let (k, v) = l.split_once(':')?;
                                k.eq_ignore_ascii_case("content-length")
                                    .then(|| v.trim().parse::<usize>().ok())?
                            })
                            .unwrap_or(0);
                        if raw.len() >= i + 4 + len {
                            break (
                                head,
                                String::from_utf8_lossy(&raw[i + 4..i + 4 + len]).into_owned(),
                            );
                        }
                    }
                    if n == 0 {
                        return;
                    }
                };
                let (status, reply) = if head.starts_with("GET /rootDesc.xml ") {
                    (200, description())
                } else if head.starts_with("POST /ctl/IPConn ") {
                    let action = head
                        .lines()
                        .find_map(|l| l.strip_prefix("SOAPAction: "))
                        .and_then(|v| v.trim_matches('"').split('#').nth(1))
                        .unwrap_or("")
                        .to_string();
                    log.lock().unwrap().push((action.clone(), body.clone()));
                    match action.as_str() {
                        "AddPortMapping"
                            if permanent_only
                                && !body.contains("<NewLeaseDuration>0</NewLeaseDuration>") =>
                        {
                            (500, fault(725, "OnlyPermanentLeasesSupported"))
                        }
                        "AddPortMapping" => (200, ok("AddPortMapping", "")),
                        "GetExternalIPAddress" => (
                            200,
                            ok(
                                "GetExternalIPAddress",
                                "<NewExternalIPAddress>203.0.113.7</NewExternalIPAddress>",
                            ),
                        ),
                        "DeletePortMapping" => (200, ok("DeletePortMapping", "")),
                        _ => (500, fault(401, "Invalid Action")),
                    }
                } else {
                    (404, String::new())
                };
                // chunked で返す機器が多い。読む側がそれを解けることも確かめる。
                let response = format!(
                    "HTTP/1.1 {status} X\r\nContent-Type: text/xml\r\nTransfer-Encoding: chunked\r\n\
                     Connection: close\r\n\r\n{:x}\r\n{reply}\r\n0\r\n\r\n",
                    reply.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    });
    (addr, seen)
}

#[tokio::test]
async fn upnp_finds_the_control_point_and_falls_back_to_a_permanent_lease() {
    let (addr, seen) = fake_igd(true).await;
    let upnp = Upnp::from_location(&format!("http://{addr}/rootDesc.xml"))
        .await
        .unwrap();
    assert_eq!(upnp.host, addr);
    assert_eq!(upnp.path, "/ctl/IPConn");
    assert_eq!(upnp.service, SERVICE);

    let local = Ipv4Addr::new(192, 168, 1, 23);
    let mapping = Method::Upnp(upnp.clone()).map(local, 9444).await.unwrap();
    assert_eq!(
        mapping.external,
        SocketAddrV4::new(Ipv4Addr::new(203, 0, 113, 7), 9444)
    );
    assert_eq!(mapping.internal, 9444);
    // 1 度目は期限付きで断られ、2 度目に永続で通った。
    assert_eq!(mapping.lease, 0);

    mapping.remove().await;

    let calls = seen.lock().unwrap().clone();
    let actions: Vec<&str> = calls.iter().map(|(a, _)| a.as_str()).collect();
    assert_eq!(
        actions,
        [
            "AddPortMapping",
            "AddPortMapping",
            "GetExternalIPAddress",
            "DeletePortMapping"
        ]
    );
    let first = &calls[0].1;
    assert!(first.contains("<NewInternalClient>192.168.1.23</NewInternalClient>"));
    assert!(first.contains("<NewExternalPort>9444</NewExternalPort>"));
    assert!(first.contains("<NewProtocol>TCP</NewProtocol>"));
    assert!(first.contains("<NewLeaseDuration>3600</NewLeaseDuration>"));
    assert!(calls[3]
        .1
        .contains("<NewExternalPort>9444</NewExternalPort>"));
}

#[tokio::test]
async fn upnp_keeps_the_lease_when_the_router_accepts_it() {
    let (addr, _) = fake_igd(false).await;
    let upnp = Upnp::from_location(&format!("http://{addr}/rootDesc.xml"))
        .await
        .unwrap();
    let mapping = Method::Upnp(upnp)
        .map(Ipv4Addr::new(192, 168, 1, 23), 9444)
        .await
        .unwrap();
    assert_eq!(mapping.lease, 3600);
}

#[tokio::test]
async fn a_router_that_claims_a_public_control_address_is_not_believed() {
    // LAN の中にいるはずのルーターが、外の住所を口として名乗る。
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 4096];
        let _ = stream.read(&mut buf).await;
        let body = description().replace("/ctl/IPConn", "http://1.1.1.1:80/ctl");
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(response.as_bytes()).await;
    });
    let err = Upnp::from_location(&format!("http://{addr}/rootDesc.xml"))
        .await
        .unwrap_err();
    assert!(err.contains("not on the local network"), "{err}");
}

/// 偽の NAT-PMP のルーター。外側の住所は 198.51.100.4、割り当ては
/// 頼まれた番号 + 1 で返す (ルーターが別の番号を選ぶことがある)。
async fn fake_natpmp(result: u16) -> SocketAddr {
    fake_natpmp_at([198, 51, 100, 4], result).await.0
}

/// 外側の住所を決めて立てる。頼まれた割り当ての期限を順に覚える
/// (0 は閉じてくれという頼み)。
async fn fake_natpmp_at(outside: [u8; 4], result: u16) -> (SocketAddr, Arc<Mutex<Vec<u32>>>) {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    let leases = Arc::new(Mutex::new(Vec::new()));
    let log = leases.clone();
    tokio::spawn(async move {
        let mut buf = [0u8; 64];
        loop {
            let (n, from) = socket.recv_from(&mut buf).await.unwrap();
            let mut reply = vec![0, 128 + buf[1]];
            reply.extend_from_slice(&result.to_be_bytes());
            reply.extend_from_slice(&1234u32.to_be_bytes());
            match buf[1] {
                0 => reply.extend_from_slice(&outside),
                2 if n >= 12 => {
                    let internal = u16::from_be_bytes([buf[4], buf[5]]);
                    let lifetime = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
                    log.lock().unwrap().push(lifetime);
                    reply.extend_from_slice(&internal.to_be_bytes());
                    let mapped = if lifetime == 0 { 0 } else { internal + 1 };
                    reply.extend_from_slice(&mapped.to_be_bytes());
                    reply.extend_from_slice(&lifetime.to_be_bytes());
                }
                _ => continue,
            }
            socket.send_to(&reply, from).await.unwrap();
        }
    });
    (addr, leases)
}

/// 試験ごとに独立した一時ディレクトリ。
struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(tag: &str) -> TempDir {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("oag-portmap-{tag}-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// 名乗る住所がその値になるまで待つ。
async fn wait_for_own(handle: &NodeHandle, wanted: Vec<SocketAddr>) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let own = handle.own_addresses().await.unwrap();
        if own == wanted {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "announcing {own:?}, wanted {wanted:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn a_mapped_port_is_announced_and_closed_on_the_way_out() {
    let dir = TempDir::new("announce");
    let service = NodeService::start(Network::Regtest, &dir.0).unwrap();
    let handle = service.handle();
    let (gateway, leases) = fake_natpmp_at([198, 51, 100, 4], 0).await;

    let portmap = start_with(
        handle.clone(),
        Network::Regtest,
        Ipv4Addr::new(192, 168, 1, 23),
        9444,
        Method::NatPmp(gateway),
    );
    // ルーターが選んだ番号で名乗る。
    wait_for_own(&handle, vec!["198.51.100.4:9445".parse().unwrap()]).await;

    portmap.stop().await;
    // 開けてもらい、終わるときに閉じてもらった。
    assert_eq!(*leases.lock().unwrap(), [7200, 0]);
}

#[tokio::test]
async fn an_outside_address_behind_the_provider_nat_is_not_announced() {
    let dir = TempDir::new("cgnat");
    let service = NodeService::start(Network::Regtest, &dir.0).unwrap();
    let handle = service.handle();
    // ルーターの外側がまた私設の住所 (CGNAT) である。
    let (gateway, leases) = fake_natpmp_at([100, 64, 1, 2], 0).await;

    let portmap = start_with(
        handle.clone(),
        Network::Mainnet,
        Ipv4Addr::new(192, 168, 1, 23),
        9444,
        Method::NatPmp(gateway),
    );
    // 開けてもらったが、届かないので閉じてもらい、名乗らない。
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while leases.lock().unwrap().len() < 2 {
        assert!(
            std::time::Instant::now() < deadline,
            "the mapping was not closed"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(*leases.lock().unwrap(), [7200, 0]);
    assert!(handle.own_addresses().await.unwrap().is_empty());
    portmap.stop().await;
}

#[tokio::test]
async fn natpmp_maps_and_reports_the_port_the_router_chose() {
    let gateway = fake_natpmp(0).await;
    assert_eq!(
        natpmp_external_ip(gateway).await.unwrap(),
        Ipv4Addr::new(198, 51, 100, 4)
    );
    assert_eq!(
        natpmp_map(gateway, 9444, 9444, 7200).await.unwrap(),
        (9445, 7200)
    );

    let mapping = Method::NatPmp(gateway)
        .map(Ipv4Addr::new(192, 168, 1, 23), 9444)
        .await
        .unwrap();
    // 名乗るのはルーターが選んだ番号である。
    assert_eq!(
        mapping.external,
        SocketAddrV4::new(Ipv4Addr::new(198, 51, 100, 4), 9445)
    );
    assert_eq!(mapping.internal, 9444);
    assert_eq!(mapping.lease, 7200);
    mapping.remove().await;
}

#[tokio::test]
async fn natpmp_says_why_the_router_refused() {
    let gateway = fake_natpmp(2).await;
    let err = natpmp_external_ip(gateway).await.unwrap_err();
    assert!(err.contains("turned off"), "{err}");
}

#[tokio::test]
async fn natpmp_gives_up_when_nobody_answers() {
    // 誰も待っていないポート。
    let silent = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = silent.local_addr().unwrap();
    drop(silent);
    let started = std::time::Instant::now();
    assert!(natpmp_external_ip(addr).await.is_err());
    // 250 + 500 + 1000 + 2000 ms。終了や起動を長く待たせない。
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
}
