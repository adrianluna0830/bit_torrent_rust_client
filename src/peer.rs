use crate::peer_message::PeerMessage;
use sha1::{Digest, Sha1};
use std::io::SeekFrom;
use std::{io, net::SocketAddrV4};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, Instant, timeout, timeout_at};

const BLOCK_SIZE: u32 = 16_384;
const MAX_REQUESTS_PER_ITERATION: usize = 8;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub(crate) enum PeerLoopResult {
    NoPeersAvailable,
    Completed,
    ConnectionError(io::Error),
    HandshakeTimeout,
    PeerDisconnected(io::Error),
    PeerError(String),
    IoError(io::Error),
    SaveError(io::Error),
    RequestTimeout { piece_index: usize, begin: u32 },
}

impl std::fmt::Display for PeerLoopResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoPeersAvailable => write!(formatter, "no habia pares disponibles"),
            Self::Completed => write!(formatter, "descarga completada"),
            Self::ConnectionError(err) => write!(formatter, "error de conexion: {err}"),
            Self::HandshakeTimeout => write!(formatter, "se agoto el tiempo del saludo inicial"),
            Self::PeerDisconnected(err) => write!(formatter, "el par se desconecto: {err}"),
            Self::PeerError(err) => write!(formatter, "error del par: {err}"),
            Self::IoError(err) => write!(formatter, "error de entrada o salida: {err}"),
            Self::SaveError(err) => write!(formatter, "error al guardar: {err}"),
            Self::RequestTimeout { piece_index, begin } => {
                write!(formatter, "se agoto el tiempo de la pieza {piece_index}, inicio {begin}")
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockState {
    NotRequested,
    Pending,
    Received,
}

#[derive(Debug)]
struct Block {
    begin: u32,
    length: u32,
    state: BlockState,
    requested_at: Option<Instant>,
}

#[derive(Debug)]
pub(crate) struct Piece {
    blocks: Vec<Block>,
    expected_hash: [u8; 20],
    verified: bool,
}

impl Piece {
    fn all_blocks_received(&self) -> bool {
        self.blocks.iter().all(|block| block.state == BlockState::Received)
    }

    fn clear_blocks(&mut self) {
        for block in &mut self.blocks {
            block.state = BlockState::NotRequested;
            block.requested_at = None;
        }
    }
}

pub(crate) fn create_pieces(total_size: u64, piece_length: u32, piece_hashes: &[u8]) -> Result<Vec<Piece>, String> {
    if piece_length == 0 {
        return Err("la longitud de pieza no puede ser cero".to_string());
    }

    let mut hash_chunks = piece_hashes.chunks_exact(20);

    if !hash_chunks.remainder().is_empty() {
        return Err("la lista de hashes de piezas no es múltiplo de 20 bytes".to_string());
    }

    let mut pieces = Vec::new();
    let mut piece_start = 0u64;

    while piece_start < total_size {
        let Some(hash_bytes) = hash_chunks.next() else {
            return Err("faltan hashes para las piezas del torrent".to_string());
        };

        let expected_hash: [u8; 20] = hash_bytes.try_into().unwrap();
        let piece_size = (total_size - piece_start).min(u64::from(piece_length)) as u32;
        let mut blocks = Vec::new();
        let mut begin = 0u32;

        while begin < piece_size {
            let length = (piece_size - begin).min(BLOCK_SIZE);

            blocks.push(Block { begin, length, state: BlockState::NotRequested, requested_at: None });

            begin += length;
        }

        pieces.push(Piece { blocks, expected_hash, verified: false });

        piece_start += u64::from(piece_size);
    }

    if hash_chunks.next().is_some() {
        return Err("el torrent contiene más hashes que piezas".to_string());
    }

    Ok(pieces)
}

pub(crate) fn reset_pending_requests(pieces: &mut [Piece]) {
    for piece in pieces {
        for block in &mut piece.blocks {
            if block.state == BlockState::Pending {
                block.state = BlockState::NotRequested;
                block.requested_at = None;
            }
        }
    }
}

pub(crate) async fn peer_loop(stream: &mut TcpStream, pieces: &mut Vec<Piece>, output_path: &str, piece_length: u32) -> PeerLoopResult {
    let mut peer_choked = true;
    let mut peer_pieces = vec![false; pieces.len()];
    let mut has_used_interested = false;
    loop {
        let pending_request = earliest_pending_request(pieces);

        let message = if let Some((piece_index, begin, deadline)) = pending_request {
            match timeout_at(deadline, read_peer_message(stream)).await {
                Ok(Ok(message)) => message,
                Ok(Err(err)) => {
                    return peer_message_error_to_loop_result(err);
                }
                Err(_) => {
                    return PeerLoopResult::RequestTimeout { piece_index, begin };
                }
            }
        } else {
            match read_peer_message(stream).await {
                Ok(message) => message,
                Err(err) => {
                    return peer_message_error_to_loop_result(err);
                }
            }
        };

        match message {
            PeerMessage::KeepAlive => {}
            PeerMessage::Choke => {
                peer_choked = true;
            }
            PeerMessage::Unchoke => {
                peer_choked = false;
            }
            PeerMessage::Interested => {}
            PeerMessage::NotInterested => {}
            PeerMessage::Have(piece) => {
                let Ok(piece_index) = usize::try_from(piece) else {
                    continue;
                };

                let Some(peer_has_piece) = peer_pieces.get_mut(piece_index) else {
                    continue;
                };

                *peer_has_piece = true;
            }
            PeerMessage::Bitfield(items) => {
                for (byte_index, byte) in items.iter().enumerate() {
                    for bit_index in 0..8 {
                        let piece_index = byte_index * 8 + bit_index;

                        if piece_index >= peer_pieces.len() {
                            break;
                        }

                        let mask = 1 << (7 - bit_index);
                        peer_pieces[piece_index] = byte & mask != 0;
                    }
                }
            }
            PeerMessage::Request { index, begin, length } => {}
            PeerMessage::Piece { index, begin, block } => {
                //                 let Ok(piece_index) = usize::try_from(index) else {
                //                     continue;
                //                 };
                //
                //                 let Some(piece) = pieces.get_mut(piece_index) else {
                //                     continue;
                //                 };
                //
                //                 let Some(expected_block) = piece.blocks.iter_mut().find(|expected_block| expected_block.begin == begin) else {
                //                     continue;
                //                 };
                //
                //                 if let Err(err) = expected_block.add_data(&block) {
                //                     continue;
                //                 }

                //                 if piece.all_blocks_received() {
                //                     let actual_hash = piece.calculate_hash();
                //
                //                     if actual_hash == piece.expected_hash {
                //                         let piece_data = piece.data();
                //                         piece.verified = true;
                //
                //                         if let Err(err) = save_piece(output_path, piece_index, piece_length, &piece_data).await {
                //                             piece.verified = false;
                //                             piece.clear_blocks();
                //                             return PeerLoopResult::SaveError(err);
                //                         }
                //
                //                         piece.clear_blocks();
                //
                //                         let verified_pieces = pieces.iter().filter(|piece| piece.verified).count();
                //                         let progress = verified_pieces as f64 / pieces.len() as f64 * 100.0;
                //                     } else {
                //                         piece.verified = false;
                //                         piece.clear_blocks();
                //
                //                         return PeerLoopResult::PeerError(format!("el par envio una pieza corrupta: {piece_index}"));
                //                     }
                //                 }
            }
            PeerMessage::Cancel { index, begin, length } => {}
        }
        if !has_used_interested {
            for (index, &peer_has_piece) in peer_pieces.iter().enumerate() {
                if !peer_has_piece || pieces[index].verified {
                    continue;
                }

                let interested_bytes = PeerMessage::Interested.to_bytes();

                if let Err(err) = stream.write_all(&interested_bytes).await {
                    return PeerLoopResult::IoError(err);
                }

                has_used_interested = true;
                break;
            }
        }

        if has_used_interested && !peer_choked {
            let mut requests_sent = 0;

            'request_loop: for (piece_index, (&peer_has_piece, piece)) in peer_pieces.iter().zip(pieces.iter_mut()).enumerate() {
                if !peer_has_piece || piece.verified {
                    continue;
                }

                let Ok(index) = u32::try_from(piece_index) else {
                    break;
                };

                for block in &mut piece.blocks {
                    if block.state != BlockState::NotRequested {
                        continue;
                    }

                    if requests_sent >= MAX_REQUESTS_PER_ITERATION {
                        break 'request_loop;
                    }

                    let request_bytes = PeerMessage::Request { index, begin: block.begin, length: block.length }.to_bytes();

                    if let Err(err) = stream.write_all(&request_bytes).await {
                        return PeerLoopResult::IoError(err);
                    }

                    block.state = BlockState::Pending;
                    block.requested_at = Some(Instant::now());
                    requests_sent += 1;
                }
            }
        }

        let mut all_pieces_verified = true;

        for piece in pieces.iter() {
            if !piece.verified {
                all_pieces_verified = false;
                break;
            }
        }

        if all_pieces_verified {
            return PeerLoopResult::Completed;
        }
    }
}

fn earliest_pending_request(pieces: &[Piece]) -> Option<(usize, u32, Instant)> {
    let mut earliest: Option<(usize, u32, Instant)> = None;

    for (piece_index, piece) in pieces.iter().enumerate() {
        for block in &piece.blocks {
            if block.state != BlockState::Pending {
                continue;
            }

            let Some(requested_at) = block.requested_at else {
                continue;
            };

            let deadline = requested_at + REQUEST_TIMEOUT;

            match earliest {
                Some((_, _, earliest_deadline)) if earliest_deadline <= deadline => {}
                _ => earliest = Some((piece_index, block.begin, deadline)),
            }
        }
    }

    earliest
}

#[derive(Debug)]
enum ReadPeerMessageError {
    Io(io::Error),
    InvalidMessage(String),
}

impl std::fmt::Display for ReadPeerMessageError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(formatter, "{err}"),
            Self::InvalidMessage(err) => write!(formatter, "{err}"),
        }
    }
}

fn peer_message_error_to_loop_result(error: ReadPeerMessageError) -> PeerLoopResult {
    match error {
        ReadPeerMessageError::Io(err)
            if matches!(err.kind(), io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted | io::ErrorKind::BrokenPipe | io::ErrorKind::NotConnected) =>
        {
            PeerLoopResult::PeerDisconnected(err)
        }
        ReadPeerMessageError::Io(err) => PeerLoopResult::IoError(err),
        ReadPeerMessageError::InvalidMessage(err) => PeerLoopResult::PeerError(err),
    }
}

async fn read_peer_message(stream: &mut TcpStream) -> Result<PeerMessage, ReadPeerMessageError> {
    let mut message_length_buffer = [0u8; 4];

    stream.read_exact(&mut message_length_buffer).await.map_err(ReadPeerMessageError::Io)?;

    let length = u32::from_be_bytes(message_length_buffer) as usize;
    let mut peer_message_bytes = Vec::with_capacity(4 + length);
    peer_message_bytes.extend_from_slice(&message_length_buffer);

    if length != 0 {
        let mut message_content_buffer = vec![0u8; length];

        stream.read_exact(&mut message_content_buffer).await.map_err(ReadPeerMessageError::Io)?;

        peer_message_bytes.extend_from_slice(&message_content_buffer);
    }

    PeerMessage::from_bytes(&peer_message_bytes).map_err(ReadPeerMessageError::InvalidMessage)
}

pub(crate) async fn connect_to_peer(peer: SocketAddrV4) -> io::Result<TcpStream> {
    timeout(Duration::from_secs(2), TcpStream::connect(peer)).await.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "la conexión superó los 2 segundos"))?
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
        return Err(format!("longitud del protocolo invalida: {}", handshake[0]));
    }

    if &handshake[1..20] != b"BitTorrent protocol" {
        return Err("el par no utiliza el protocolo bittorrent".to_string());
    }

    if &handshake[28..48] != expected_info_hash {
        return Err("el hash de informacion recibido no coincide".to_string());
    }

    Ok(())
}

async fn save_piece(path: &str, piece_index: usize, piece_length: u32, data: &[u8]) -> io::Result<()> {
    let offset = piece_index as u64 * piece_length as u64;

    let mut file = OpenOptions::new().write(true).open(path).await?;

    file.seek(SeekFrom::Start(offset)).await?;
    file.write_all(data).await?;
    file.flush().await?;

    Ok(())
}
