use crate::peer::{Block, BlockState, PieceAssignment};
use crate::peer_message::{PeerMessage, try_read_peer_message};
use crate::peer_orchestrator::{AvailablePiecesEvent, PeerFailure, PeerOrchestratorCommand, PeerWorkerEvent};
use sha1::{Digest, Sha1};
use std::collections::HashSet;
use std::io::{self, SeekFrom};
use std::sync::mpsc::{self, TryRecvError};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, Instant, sleep};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const PIECE_PROGRESS_TIMEOUT: Duration = Duration::from_secs(180);
const MAX_PENDING_REQUESTS: usize = 8;

fn has_expired_request(blocks: &[Block]) -> bool {
    for block in blocks {
        if block.state != BlockState::Pending {
            continue;
        }

        let Some(requested_at) = block.requested_at else {
            continue;
        };

        if requested_at.elapsed() >= REQUEST_TIMEOUT {
            return true;
        }
    }

    false
}

pub(crate) async fn peer_worker(
    stream: &mut TcpStream,
    command_rx: mpsc::Receiver<PeerOrchestratorCommand>,
    event_tx: mpsc::Sender<PeerWorkerEvent>,
    peer_id: usize,
    piece_length: u64,
    block_size: u32,
    total_pieces: usize,
    mut file: File,
    initial_verified_downloaded_pieces: HashSet<usize>,
) {
    let mut verified_downloaded_pieces = initial_verified_downloaded_pieces;
    let mut read_buffer = Vec::new();
    let mut notify_event: Option<usize> = None;
    let mut has_sended_bitfield = false;
    let mut piece_assignment: Option<PieceAssignment> = None;
    let mut has_sended_interested = false;
    let mut previous_piece: Option<usize> = None;
    let mut peer_choked = true;

    loop {
        if !has_sended_bitfield {
            let mut bitfield = vec![0u8; total_pieces.div_ceil(8)];
            for &piece in &verified_downloaded_pieces {
                bitfield[piece / 8] |= 1 << (7 - piece % 8);
            }

            let message = PeerMessage::Bitfield(bitfield).to_bytes();
            if let Err(err) = stream.write_all(&message).await {
                let failure = PeerFailure { peer_id, reason: format!("Failed to send Bitfield: {err}") };
                let _ = event_tx.send(PeerWorkerEvent::PeerFailedEvent(failure));
                return;
            }
            has_sended_bitfield = true;
        }

        match command_rx.try_recv() {
            Ok(command) => match command {
                PeerOrchestratorCommand::NewPieceCommand(piece) => {
                    verified_downloaded_pieces.insert(piece);
                    notify_event = Some(piece);
                }
                PeerOrchestratorCommand::PieceAssignmentCommand(new_piece_assignment) => {
                    if piece_assignment.is_some() {
                        panic!("Peer {peer_id} already has an assigned piece");
                    }
                    piece_assignment = Some(new_piece_assignment);
                }
            },
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => {
                return;
            }
        }

        if let Some(piece) = notify_event {
            let message = PeerMessage::Have(piece as u32).to_bytes();
            if let Err(err) = stream.write_all(&message).await {
                let failure = PeerFailure { peer_id, reason: format!("Failed to send Have: {err}") };
                let _ = event_tx.send(PeerWorkerEvent::PeerFailedEvent(failure));
                return;
            }
            notify_event = None;
        }

        let last_piece_progress = piece_assignment.as_ref().map(|assignment| assignment.last_progress_at);
        if let Some(last_progress) = last_piece_progress
            && last_progress.elapsed() >= PIECE_PROGRESS_TIMEOUT
        {
            let failure = PeerFailure { peer_id, reason: "No progress on the assigned piece".to_string() };
            let _ = event_tx.send(PeerWorkerEvent::PeerFailedEvent(failure));
            return;
        }

        if let Some(assignment) = &piece_assignment
            && has_expired_request(&assignment.blocks_to_download)
        {
            let failure = PeerFailure { peer_id, reason: "Block request timed out".to_string() };
            let _ = event_tx.send(PeerWorkerEvent::PeerFailedEvent(failure));
            return;
        }

        let message_received = match try_read_peer_message(stream, &mut read_buffer) {
            Ok(message) => message,
            Err(reason) => {
                let failure = PeerFailure { peer_id, reason };
                let _ = event_tx.send(PeerWorkerEvent::PeerFailedEvent(failure));
                return;
            }
        };
        let no_message_received = message_received.is_none();

        match message_received {
            None | Some(PeerMessage::KeepAlive) => {}
            Some(PeerMessage::Choke) => {
                peer_choked = true;
            }
            Some(PeerMessage::Unchoke) => {
                peer_choked = false;
            }
            Some(PeerMessage::Interested) => {
                let message = PeerMessage::Unchoke.to_bytes();

                if let Err(err) = stream.write_all(&message).await {
                    let failure = PeerFailure { peer_id, reason: format!("Failed to send Unchoke: {err}") };

                    let _ = event_tx.send(PeerWorkerEvent::PeerFailedEvent(failure));

                    return;
                }
            }
            Some(PeerMessage::NotInterested) => {}
            Some(PeerMessage::Have(piece)) => {
                let pieces = HashSet::from([piece as usize]);
                if event_tx.send(PeerWorkerEvent::AvailablePiecesEvent(AvailablePiecesEvent { pieces, peer_id })).is_err() {
                    return;
                }
            }
            Some(PeerMessage::Bitfield(bytes)) => {
                let mut pieces = HashSet::<usize>::new();
                for (byte_index, byte) in bytes.iter().enumerate() {
                    for bit_index in 0..8 {
                        let mask = 1 << (7 - bit_index);
                        if byte & mask != 0 {
                            pieces.insert(byte_index * 8 + bit_index);
                        }
                    }
                }
                if event_tx.send(PeerWorkerEvent::AvailablePiecesEvent(AvailablePiecesEvent { pieces, peer_id })).is_err() {
                    return;
                }
            }
            Some(PeerMessage::Request { index, begin, length }) => {
                if !verified_downloaded_pieces.contains(&(index as usize)) {
                    continue;
                }

                let block_is_empty = length == 0;
                let block_is_too_large = length > block_size;
                let block_end = u64::from(begin) + u64::from(length);
                let block_exceeds_piece = block_end > piece_length;
                let request_is_invalid = block_is_empty || block_is_too_large || block_exceeds_piece;
                if request_is_invalid {
                    continue;
                }

                let result: io::Result<()> = async {
                    let offset = u64::from(index) * piece_length + u64::from(begin);

                    let file_metadata = file.metadata().await?;
                    let requested_end = offset + u64::from(length);
                    let request_exceeds_file = requested_end > file_metadata.len();
                    if request_exceeds_file {
                        return Ok(());
                    }

                    let mut block = vec![0u8; length as usize];
                    file.seek(SeekFrom::Start(offset)).await?;
                    file.read_exact(&mut block).await?;

                    let response = PeerMessage::Piece { index, begin, block };
                    stream.write_all(&response.to_bytes()).await?;
                    Ok(())
                }
                .await;

                if let Err(err) = result {
                    let failure = PeerFailure { peer_id, reason: format!("Failed to serve block request: {err}") };

                    let _ = event_tx.send(PeerWorkerEvent::PeerFailedEvent(failure));

                    return;
                }
            }
            Some(PeerMessage::Piece { index, begin, block }) => {
                if let Some(assignmeent) = &mut piece_assignment {
                    if index != assignmeent.piece as u32 {
                        panic!("Peer {peer_id} sent piece {index} instead of its assigned piece");
                    }
                    let Some(expected_block) = assignmeent.blocks_to_download.iter_mut().find(|expected| expected.begin == begin) else {
                        continue;
                    };
                    let block_already_received = expected_block.state == BlockState::Received;
                    let block_size_matches = block.len() == expected_block.length as usize;
                    let should_ignore_block = block_already_received || !block_size_matches;
                    if should_ignore_block {
                        continue;
                    }
                    let offset = assignmeent.piece as u64 * piece_length;
                    file.seek(SeekFrom::Start(offset + u64::from(begin))).await.unwrap();
                    file.write_all(block.as_slice()).await.unwrap();
                    file.flush().await.unwrap();
                    expected_block.state = BlockState::Received;
                    expected_block.requested_at = None;
                    assignmeent.last_progress_at = Instant::now();

                    if assignmeent.all_blocks_received() {
                        let mut piece_size = 0;
                        for block in &assignmeent.blocks_to_download {
                            let block_end = block.begin as usize + block.length as usize;
                            if block_end > piece_size {
                                piece_size = block_end;
                            }
                        }
                        let mut data = vec![0u8; piece_size];
                        file.seek(SeekFrom::Start(offset)).await.unwrap();
                        file.read_exact(&mut data).await.unwrap();
                        let hash: [u8; 20] = Sha1::digest(&data).into();

                        if hash != assignmeent.piece_hash {
                            let reason = format!("Piece {} hash does not match the expected hash", assignmeent.piece);
                            let _ = event_tx.send(PeerWorkerEvent::PeerFailedEvent(PeerFailure { peer_id, reason }));
                            return;
                        }

                        let piece = assignmeent.piece;
                        if event_tx.send(PeerWorkerEvent::PieceSuccessfullyCompletedEvent(piece)).is_err() {
                            return;
                        }
                        verified_downloaded_pieces.insert(piece);
                        piece_assignment = None;
                    }
                }
            }
            Some(PeerMessage::Cancel { .. }) => {}
        }

        let current_piece = piece_assignment.as_ref().map(|assignment| assignment.piece);
        let has_assigned_piece = current_piece.is_some();
        let assigned_piece_changed = previous_piece != current_piece;
        let should_send_interested = !has_sended_interested && has_assigned_piece && assigned_piece_changed;
        if should_send_interested {
            let message = PeerMessage::Interested.to_bytes();
            if let Err(err) = stream.write_all(&message).await {
                let failure = PeerFailure { peer_id, reason: format!("Failed to send Interested: {err}") };
                let _ = event_tx.send(PeerWorkerEvent::PeerFailedEvent(failure));
                return;
            }
            has_sended_interested = true;
        }
        previous_piece = current_piece;

        let can_send_requests = has_sended_interested && !peer_choked;
        if can_send_requests {
            if let Some(assignment) = &mut piece_assignment {
                let index = assignment.piece as u32;
                let mut pending_requests = assignment.blocks_to_download.iter().filter(|block| block.state == BlockState::Pending).count();

                for block in &mut assignment.blocks_to_download {
                    if pending_requests >= MAX_PENDING_REQUESTS {
                        break;
                    }
                    if block.state != BlockState::NotRequested {
                        continue;
                    }

                    let request = PeerMessage::Request { index, begin: block.begin, length: block.length }.to_bytes();
                    if let Err(err) = stream.write_all(&request).await {
                        let failure = PeerFailure { peer_id, reason: format!("Failed to send block request: {err}") };
                        let _ = event_tx.send(PeerWorkerEvent::PeerFailedEvent(failure));
                        return;
                    }

                    block.state = BlockState::Pending;
                    block.requested_at = Some(Instant::now());
                    pending_requests += 1;
                }
            }
        }

        if no_message_received {
            sleep(Duration::from_millis(10)).await;
        } else {
            tokio::task::yield_now().await;
        }
    }
}
