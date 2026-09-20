use std::fs;

use rand::RngExt;
use reqwest::Response;
use serde_bytes::ByteBuf;
use sha1::{Digest, Sha1};
use tokio::net::TcpListener;
mod torrent;
mod torrent_info;
use crate::torrent::Torrent;
use crate::torrent_info::{get_info_bytes, get_info_length};
use serde::Deserialize;

const CHARACTERS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

struct TrackerRequest {
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    port: u16,
    uploaded: u64,
    downloaded: u64,
    left: u64,
    compact: u8,
    event: Option<AnnounceEvent>,
    tracker_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TrackerResponse {
    interval: Option<u64>,
    peers: Option<ByteBuf>,

    #[serde(rename = "failure reason")]
    failure_reason: Option<String>,

    #[serde(rename = "tracker id")]
    tracker_id: Option<String>,
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

#[derive(Debug)]
enum AnnounceEvent {
    Started,
    Completed,
    Stopped,
}

#[tokio::main]
async fn main() {
    let bytes = fs::read("ubuntu-24.04.4-desktop-arm64.iso.torrent").expect("no se pudo leer el archivo .torrent");

    let torrent: Torrent = serde_bencode::from_bytes(&bytes).expect("no se pudo decodificar el torrent");

    let announce = torrent.announce.as_deref().expect("el torrent no contiene announce");

    if !announce.starts_with("https://") && !announce.starts_with("http://") {
        panic!("el tracker no usa http o https");
    }

    let info_bytes = get_info_bytes(&bytes).expect("no se pudieron extraer los bytes de info");

    let total_length = get_info_length(&torrent.info);

    let listener = TcpListener::bind("0.0.0.0:0").await.expect("no se pudo abrir un puerto tcp");

    let address = listener.local_addr().expect("no se pudo obtener la direccion local");

    let tracker_request = build_initial_tracker_request(info_bytes, address.port(), total_length);

    let url = build_url(announce, &tracker_request);

    println!("url del tracker: {url}");

    let response_bytes = announce_to_tracker(&url).await.expect("fallo la solicitud al tracker");

    let tracker_response: TrackerResponse = serde_bencode::from_bytes(&response_bytes).expect("respuesta bencode invalida");

    println!("{tracker_response:#?}");
}

async fn announce_to_tracker(url: &str) -> Result<Vec<u8>, String> {
    let response = reqwest::get(url).await.map_err(|error| error.to_string())?;

    let status = response.status();

    let bytes = response.bytes().await.map_err(|error| error.to_string())?;

    if !status.is_success() {
        return Err(format!("el tracker respondio con http {status}"));
    }

    Ok(bytes.to_vec())
}

fn build_url(announce: &str, tracker_request: &TrackerRequest) -> String {
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
fn build_initial_tracker_request(info_bytes: &[u8], port: u16, length: u64) -> TrackerRequest {
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
