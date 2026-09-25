use serde::Deserialize;
use serde_bytes::ByteBuf;

#[derive(Debug, Deserialize)]
pub(crate) struct Torrent {
    pub(crate) announce: Option<String>,
    pub(crate) info: Info,
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

    pub(crate) pieces: ByteBuf,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MultiFile {
    pub(crate) files: Vec<TorrentFile>,
    name: String,

    #[serde(rename = "piece length")]
    pub(crate) piece_length: u64,

    pub(crate) pieces: ByteBuf,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TorrentFile {
    pub(crate) length: u64,
    path: Vec<String>,
}
