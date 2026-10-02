use std::net::SocketAddrV4;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, Instant, timeout};

pub(crate) const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) const BLOCK_SIZE: u32 = 16_384;

pub(crate) async fn get_peer_stream(peer: SocketAddrV4, info_hash: [u8; 20], peer_id: [u8; 20]) -> Result<TcpStream, String> {
    log::debug!("Peer {peer}: TCP connection started");
    let connection_attempt = TcpStream::connect(peer);
    let connection_result = timeout(Duration::from_secs(15), connection_attempt).await;
    let connection = match connection_result {
        Ok(connection) => connection,
        Err(_) => return Err("Connection timed out after 15 seconds".to_string()),
    };
    let mut stream = connection.map_err(|err| format!("Failed to connect to peer: {err}"))?;

    log::debug!("Peer {peer}: TCP connection established");
    let handshake_bytes = build_handshake(info_hash, peer_id);
    stream.write_all(&handshake_bytes).await.map_err(|err| format!("Failed to send handshake: {err}"))?;

    log::debug!("Peer {peer}: sent handshake");
    let mut buf = [0u8; 68];
    let handshake_result = timeout(HANDSHAKE_TIMEOUT, stream.read_exact(&mut buf)).await;
    let read_result = match handshake_result {
        Ok(result) => result,
        Err(_) => return Err("Handshake timed out".to_string()),
    };
    read_result.map_err(|err| format!("Failed to read handshake: {err}"))?;

    log::debug!("Peer {peer}: received handshake");
    validate_peer_handshake(&buf, &info_hash).map_err(|err| format!("Invalid handshake: {err}"))?;

    log::debug!("Peer {peer}: handshake validated");
    Ok(stream)
}

pub(crate) fn build_handshake(info_hash: [u8; 20], peer_id: [u8; 20]) -> [u8; 68] {
    let mut handshake = [0u8; 68];

    handshake[0] = 19;
    handshake[1..20].copy_from_slice(b"BitTorrent protocol");
    handshake[28..48].copy_from_slice(&info_hash);
    handshake[48..68].copy_from_slice(&peer_id);

    handshake
}

pub(crate) fn validate_peer_handshake(handshake: &[u8; 68], expected_info_hash: &[u8; 20]) -> Result<(), String> {
    if handshake[0] != 19 {
        return Err(format!("Invalid protocol name length: {}", handshake[0]));
    }

    if &handshake[1..20] != b"BitTorrent protocol" {
        return Err("Peer is not using the BitTorrent protocol".to_string());
    }

    if &handshake[28..48] != expected_info_hash {
        return Err("Received info hash does not match".to_string());
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BlockState {
    NotRequested,
    Pending,
    Received,
}

#[derive(Debug)]
pub(crate) struct Block {
    pub(crate) begin: u32,
    pub(crate) length: u32,
    pub(crate) state: BlockState,
    pub(crate) requested_at: Option<Instant>,
}

pub(crate) struct PieceAssignment {
    pub(crate) piece: usize,
    pub(crate) blocks_to_download: Vec<Block>,
    pub(crate) piece_hash: [u8; 20],
    pub(crate) last_progress_at: Instant,
}

impl PieceAssignment {
    pub(crate) fn all_blocks_received(&self) -> bool {
        self.blocks_to_download.iter().all(|block| block.state == BlockState::Received)
    }
}
