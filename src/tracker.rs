use rand::RngExt;
use serde::Deserialize;
use serde_bytes::ByteBuf;
use sha1::{Digest, Sha1};
use std::net::{Ipv4Addr, SocketAddrV4};

const CHARACTERS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

pub(crate) struct TrackerRequest {
    pub(crate) info_hash: [u8; 20],
    pub(crate) peer_id: [u8; 20],
    port: u16,
    uploaded: u64,
    downloaded: u64,
    left: u64,
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
        return Err(format!("el tracker respondio con http {status}"));
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
        announce, separator, info_hash, peer_id, tracker_request.port, tracker_request.uploaded, tracker_request.downloaded, tracker_request.left, tracker_request.compact,
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

pub(crate) fn build_initial_tracker_request(info_bytes: &[u8], port: u16, length: u64) -> TrackerRequest {
    let mut hasher = Sha1::new();
    hasher.update(info_bytes);

    let info_hash: [u8; 20] = hasher.finalize().into();

    let mut rng = rand::rng();

    let peer_id: [u8; 20] = std::array::from_fn(|_| {
        let position = rng.random_range(0..CHARACTERS.len());
        CHARACTERS[position]
    });

    TrackerRequest { info_hash, peer_id, port, uploaded: 0, downloaded: 0, left: length, compact: 1, event: Some(AnnounceEvent::Started), tracker_id: None }
}

pub(crate) fn parse_compact_peers(bytes: &[u8]) -> Result<Vec<SocketAddrV4>, String> {
    if bytes.len() % 6 != 0 {
        return Err("compact peers no es multiplo de 6".to_string());
    }

    let mut socket_addresses: Vec<SocketAddrV4> = Vec::new();

    for chunk in bytes.chunks_exact(6) {
        let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
        let port = u16::from_be_bytes([chunk[4], chunk[5]]);
        let dir = SocketAddrV4::new(ip, port);
        socket_addresses.push(dir);
    }
    Ok(socket_addresses)
}
