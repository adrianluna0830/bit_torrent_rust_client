use rand::RngExt;
use serde::Deserialize;
use serde_bytes::ByteBuf;
use std::collections::HashSet;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use tokio::net::UdpSocket;
use tokio::time::{Duration, timeout};
use url::Url;

const CHARACTERS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
const TRACKER_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) struct TrackerRequest {
    pub(crate) info_hash: [u8; 20],
    pub(crate) peer_id: [u8; 20],
    port: u16,
    uploaded: u64,
    downloaded: u64,
    length: u64,
    compact: u8,
    event: Option<AnnounceEvent>,
    tracker_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TrackerResponse {
    interval: Option<u64>,
    pub(crate) peers: Option<ByteBuf>,

    #[serde(rename = "failure reason")]
    failure_reason: Option<String>,

    #[serde(rename = "tracker id")]
    tracker_id: Option<String>,
}

#[derive(Debug)]
enum AnnounceEvent {
    Started,
    Completed,
    Stopped,
}

impl AnnounceEvent {
    fn as_str(&self) -> &'static str {
        match self {
            AnnounceEvent::Started => "started",
            AnnounceEvent::Completed => "completed",
            AnnounceEvent::Stopped => "stopped",
        }
    }
}

pub(crate) async fn announce_to_tracker(url: &str) -> Result<Vec<u8>, String> {
    let response = reqwest::get(url).await.map_err(|error| error.to_string())?;

    let status = response.status();

    let bytes = response.bytes().await.map_err(|error| error.to_string())?;

    if !status.is_success() {
        return Err(format!("Tracker returned HTTP {status}"));
    }

    Ok(bytes.to_vec())
}

pub(crate) fn build_url(announce: &str, tracker_request: &TrackerRequest) -> String {
    let info_hash = percent_encode(&tracker_request.info_hash);

    let peer_id = percent_encode(&tracker_request.peer_id);

    let separator = if announce.ends_with('?') || announce.ends_with('&') {
        ""
    } else if announce.contains('?') {
        "&"
    } else {
        "?"
    };

    let mut url = format!(
        "{}{}info_hash={}&peer_id={}&port={}&uploaded={}&downloaded={}&left={}&compact={}",
        announce, separator, info_hash, peer_id, tracker_request.port, tracker_request.uploaded, tracker_request.downloaded, tracker_request.length, tracker_request.compact,
    );

    if let Some(event) = &tracker_request.event {
        url.push_str("&event=");
        url.push_str(event.as_str());
    }

    if let Some(tracker_id) = &tracker_request.tracker_id {
        url.push_str("&trackerid=");
        url.push_str(&percent_encode(tracker_id.as_bytes()));
    }

    url
}

fn percent_encode(bytes: &[u8]) -> String {
    let mut resultado = String::new();

    for byte in bytes {
        resultado.push_str(&format!("%{:02X}", byte));
    }

    resultado
}

pub(crate) fn generate_id() -> [u8; 20] {
    let mut rng = rand::rng();

    std::array::from_fn(|_| {
        let position = rng.random_range(0..CHARACTERS.len());
        CHARACTERS[position]
    })
}

pub(crate) fn generate_random_u32() -> u32 {
    let mut rng = rand::rng();
    rng.random::<u32>()
}

pub(crate) fn build_initial_tracker_request(info_hash: [u8; 20], peer_id: [u8; 20], port: u16, length: u64) -> TrackerRequest {
    TrackerRequest { info_hash, peer_id, port, uploaded: 0, downloaded: 0, length, compact: 1, event: Some(AnnounceEvent::Started), tracker_id: None }
}

pub(crate) fn parse_compact_peers(bytes: &[u8]) -> Result<HashSet<SocketAddrV4>, String> {
    if bytes.len() % 6 != 0 {
        return Err("Compact peer list has an invalid length".to_string());
    }

    let mut socket_addresses = HashSet::new();

    for chunk in bytes.chunks_exact(6) {
        let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
        let port = u16::from_be_bytes([chunk[4], chunk[5]]);
        let dir = SocketAddrV4::new(ip, port);
        socket_addresses.insert(dir);
    }
    Ok(socket_addresses)
}

async fn receive_udp_response(socket: &UdpSocket, tracker_addr: SocketAddrV4, transaction_id: u32, buffer: &mut [u8]) -> Result<usize, String> {
    loop {
        let (size, sender) = socket.recv_from(buffer).await.map_err(|err| err.to_string())?;

        let response_from_expected_tracker = sender == SocketAddr::V4(tracker_addr);
        let response_has_header = size >= 8;
        let should_ignore_response = !response_from_expected_tracker || !response_has_header;
        if should_ignore_response {
            continue;
        }

        let received_id = u32::from_be_bytes(buffer[4..8].try_into().unwrap());

        if received_id == transaction_id {
            return Ok(size);
        }
    }
}

async fn get_peers_from_udp(announce: &str, info_hash: [u8; 20], peer_id: [u8; 20], port: u16, total_length: u64) -> Result<HashSet<SocketAddrV4>, String> {
    let url = Url::parse(announce).map_err(|err| err.to_string())?;

    let host = url.host_str().ok_or_else(|| "Tracker URL has no host".to_string())?;
    let tracker_port = url.port().ok_or_else(|| "Tracker URL has no port".to_string())?;
    let addresses = tokio::net::lookup_host((host, tracker_port)).await.map_err(|err| err.to_string())?;
    let mut ipv4_address = None;
    for address in addresses {
        if let SocketAddr::V4(address) = address {
            ipv4_address = Some(address);
            break;
        }
    }
    let tracker_addr = match ipv4_address {
        Some(address) => address,
        None => return Err("Tracker has no IPv4 address".to_string()),
    };
    let socket = UdpSocket::bind("0.0.0.0:0").await.map_err(|err| err.to_string())?;
    let connect_transaction_id = generate_random_u32();
    let mut connect_packet = [0u8; 16];
    connect_packet[0..8].copy_from_slice(&0x41727101980u64.to_be_bytes());
    connect_packet[8..12].copy_from_slice(&0u32.to_be_bytes());
    connect_packet[12..16].copy_from_slice(&connect_transaction_id.to_be_bytes());
    let mut connect_response = [0u8; 2048];
    socket.send_to(&connect_packet, tracker_addr).await.map_err(|err| err.to_string())?;

    let connect_size = match timeout(Duration::from_secs(10), receive_udp_response(&socket, tracker_addr, connect_transaction_id, &mut connect_response)).await {
        Ok(Ok(size)) => size,
        Ok(Err(err)) => return Err(err),
        Err(_) => return Err("UDP tracker connection timed out after 10 seconds".to_string()),
    };
    let action = u32::from_be_bytes(connect_response[0..4].try_into().unwrap());

    if action == 3 {
        return Err("UDP tracker rejected the connection".to_string());
    }

    if action != 0 || connect_size < 16 {
        return Err("Invalid UDP connection response".into());
    }

    let connection_id = u64::from_be_bytes(connect_response[8..16].try_into().unwrap());
    let announce_transaction_id = generate_random_u32();
    let key = generate_random_u32();
    let mut announce_packet = Vec::with_capacity(98);
    announce_packet.extend_from_slice(&connection_id.to_be_bytes());
    announce_packet.extend_from_slice(&1u32.to_be_bytes());
    announce_packet.extend_from_slice(&announce_transaction_id.to_be_bytes());
    announce_packet.extend_from_slice(&info_hash);
    announce_packet.extend_from_slice(&peer_id);
    announce_packet.extend_from_slice(&0u64.to_be_bytes());
    announce_packet.extend_from_slice(&total_length.to_be_bytes());
    announce_packet.extend_from_slice(&0u64.to_be_bytes());
    announce_packet.extend_from_slice(&2u32.to_be_bytes());
    announce_packet.extend_from_slice(&0u32.to_be_bytes());
    announce_packet.extend_from_slice(&key.to_be_bytes());
    announce_packet.extend_from_slice(&(-1i32).to_be_bytes());
    announce_packet.extend_from_slice(&port.to_be_bytes());
    let mut announce_response = vec![0u8; 65_535];
    socket.send_to(&announce_packet, tracker_addr).await.map_err(|err| err.to_string())?;

    let announce_size = match timeout(Duration::from_secs(10), receive_udp_response(&socket, tracker_addr, announce_transaction_id, &mut announce_response)).await {
        Ok(Ok(size)) => size,
        Ok(Err(err)) => return Err(err),
        Err(_) => return Err("UDP tracker announce timed out after 10 seconds".to_string()),
    };
    let action = u32::from_be_bytes(announce_response[0..4].try_into().unwrap());

    if action == 3 {
        return Err("UDP tracker rejected the announce".to_string());
    }

    if action != 1 || announce_size < 20 {
        return Err("Invalid UDP announce response".into());
    }

    parse_compact_peers(&announce_response[20..announce_size])
}

pub(crate) async fn announce_and_get_peers(length: u64, info_hash: [u8; 20], peer_id: [u8; 20], port: u16, announces: &[Vec<String>]) -> Result<HashSet<SocketAddrV4>, String> {
    let mut peers = HashSet::new();
    let mut had_valid_response = false;
    let tracker_request = build_initial_tracker_request(info_hash, peer_id, port, length);

    for tier in announces {
        for announce in tier {
            if announce.starts_with("udp://") {
                let parsed_peers = match get_peers_from_udp(announce, info_hash, peer_id, port, length).await {
                    Ok(parsed_peers) => parsed_peers,
                    Err(_) => {
                        continue;
                    }
                };

                had_valid_response = true;
                peers.extend(parsed_peers);
                continue;
            }

            let uses_http = announce.starts_with("http://");
            let uses_https = announce.starts_with("https://");
            let tracker_protocol_supported = uses_http || uses_https;
            if !tracker_protocol_supported {
                continue;
            }
            let url = build_url(announce, &tracker_request);
            let response_bytes = match timeout(TRACKER_TIMEOUT, announce_to_tracker(&url)).await {
                Ok(Ok(response)) => response,
                Ok(Err(_)) => {
                    continue;
                }
                Err(_) => {
                    continue;
                }
            };
            let Ok(tracker_response) = serde_bencode::from_bytes::<TrackerResponse>(&response_bytes) else {
                continue;
            };
            let Some(peers_bytes) = tracker_response.peers else {
                continue;
            };
            let Ok(parsed_peers) = parse_compact_peers(peers_bytes.as_ref()) else {
                continue;
            };

            had_valid_response = true;
            peers.extend(parsed_peers);
        }
    }

    if had_valid_response { Ok(peers) } else { Err("No tracker returned a valid response".into()) }
}
