use crate::peer::{HANDSHAKE_TIMEOUT, build_handshake, validate_peer_handshake};
use crate::tracker::generate_id;
use serde::Deserialize;
use serde_bytes::ByteBuf;
use std::collections::HashSet;
use std::net::SocketAddrV4;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use url::Url;

#[derive(Debug, Deserialize)]
pub(crate) struct Torrent {
    pub(crate) announce: Option<String>,
    pub(crate) info: Info,
    #[serde(rename = "announce-list", default)]
    pub(crate) announce_list: Vec<Vec<String>>,

    #[serde(default)]
    pub(crate) nodes: Vec<(String, u16)>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum Info {
    SingleFile(SingleFile),
    MultiFile(MultiFile),
}

#[derive(Debug, Deserialize)]
pub(crate) struct SingleFile {
    pub(crate) length: u64,
    pub(crate) name: String,

    #[serde(rename = "piece length")]
    pub(crate) piece_length: u64,
    #[serde(default)]
    pub(crate) private: u8,
    pub(crate) pieces: ByteBuf,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MultiFile {
    pub(crate) files: Vec<TorrentFile>,
    name: String,

    #[serde(rename = "piece length")]
    pub(crate) piece_length: u64,

    pub(crate) pieces: ByteBuf,
    #[serde(default)]
    pub(crate) private: u8,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TorrentFile {
    pub(crate) length: u64,
    path: Vec<String>,
}

pub(crate) fn torrent_from_bytes(bytes: &[u8]) -> Result<Torrent, String> {
    serde_bencode::from_bytes(bytes).map_err(|error| error.to_string())
}
pub(crate) struct Magnet {
    pub(crate) info_hash: [u8; 20],
    pub(crate) trackers: Vec<String>,
    display_name: Option<String>,
}

pub(crate) fn parse_magnet(input: &str) -> Result<Magnet, String> {
    let parsed_url = match Url::parse(input.trim()) {
        Ok(parsed_url) => parsed_url,
        Err(_) => return Err("Invalid URL".to_string()),
    };

    if parsed_url.scheme() != "magnet" {
        return Err("The URL must start with magnet:".to_string());
    }

    let mut magnet = Magnet { info_hash: [0; 20], trackers: Vec::new(), display_name: None };
    let mut has_info_hash = false;

    for (key, value) in parsed_url.query_pairs() {
        let is_info_hash_parameter = key == "xt" && value.starts_with("urn:btih:");
        if is_info_hash_parameter {
            let hash_text = &value[9..];

            let hash_has_expected_length = hash_text.len() == 40;
            let hash_is_ascii = hash_text.is_ascii();
            let hash_format_is_valid = hash_has_expected_length && hash_is_ascii;
            if !hash_format_is_valid {
                return Err("Expected a 40-character hexadecimal hash".to_string());
            }

            for byte_index in 0..20 {
                let start = byte_index * 2;
                let hex_pair = &hash_text[start..start + 2];

                magnet.info_hash[byte_index] = match u8::from_str_radix(hex_pair, 16) {
                    Ok(byte) => byte,
                    Err(_) => {
                        return Err("The hash contains invalid hex characters".to_string());
                    }
                };
            }

            has_info_hash = true;
        } else if key == "tr" {
            magnet.trackers.push(value.to_string());
        } else if key == "dn" {
            magnet.display_name = Some(value.to_string());
        }
    }

    if !has_info_hash {
        return Err("Missing xt=urn:btih: hash".to_string());
    }

    Ok(magnet)
}

pub(crate) async fn get_torrent_from_magnet(magnet: &Magnet, peers: &HashSet<SocketAddrV4>) -> Result<Torrent, String> {
    for peer in peers {
        let connection_attempt = TcpStream::connect(peer);
        let connection_result = timeout(Duration::from_secs(2), connection_attempt).await;
        let connection = match connection_result {
            Ok(connection) => connection,
            Err(_) => return Err("la conexión superó los 2 segundos".to_string()),
        };
        let mut stream = connection.map_err(|error| error.to_string())?;
        let peer_id = generate_id();

        let handshake_bytes = build_handshake(magnet.info_hash, peer_id);

        if let Err(err) = stream.write_all(&handshake_bytes).await {
            return Err("err".to_string());
        }

        let mut buf = [0u8; 68];

        match timeout(HANDSHAKE_TIMEOUT, stream.read_exact(&mut buf)).await {
            Ok(Ok(_)) => {}
            Ok(Err(err)) => {
                return Err("err".to_string());
            }
            Err(_) => {
                return Err("err".to_string());
            }
        }

        if buf[25] & 0x10 == 0 {
            return Err("err".to_string());
        }

        if let Err(err) = validate_peer_handshake(&buf, &magnet.info_hash) {
            return Err("err".to_string());
        }

        break;
    }

    unimplemented!()
}
