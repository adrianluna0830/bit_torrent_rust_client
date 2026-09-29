use std::collections::HashSet;
use std::net::SocketAddrV4;

use serde::Deserialize;
use serde_bytes::ByteBuf;

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

pub(crate) async fn get_torrent_from_magnet(_url: &str, _peers: &HashSet<SocketAddrV4>) -> Result<Torrent, String> {
    unimplemented!()
}
