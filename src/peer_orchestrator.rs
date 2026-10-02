use crate::peer::{BLOCK_SIZE, Block, BlockState, PieceAssignment, get_peer_stream};
use crate::peer_worker::peer_worker;
use crate::torrent::SingleFile;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::OpenOptions;
use std::net::SocketAddrV4;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use tokio::net::TcpStream;
use tokio::task::JoinSet;
use tokio::time::Instant;

pub(crate) const PEERS_PER_TORRENT: usize = 8;

pub(crate) struct PeerTask {
    pub(crate) command_tx: mpsc::Sender<PeerOrchestratorCommand>,
    pub(crate) handle: tokio::task::JoinHandle<()>,
    pub(crate) piece_index_downloading: Option<usize>,
    pub(crate) available_pieces: HashSet<usize>,
}

pub(crate) struct AvailablePiecesEvent {
    pub(crate) pieces: HashSet<usize>,
    pub(crate) peer_id: usize,
}

pub(crate) struct PeerFailure {
    pub(crate) peer_id: usize,
    pub(crate) reason: String,
}

pub(crate) enum PeerOrchestratorCommand {
    NewPieceCommand(usize),
    PieceAssignmentCommand(PieceAssignment),
}

pub(crate) enum PeerWorkerEvent {
    AvailablePiecesEvent(AvailablePiecesEvent),
    PieceSuccessfullyCompletedEvent(usize),
    PeerFailedEvent(PeerFailure),
}

pub(crate) fn create_piece_assignment(piece: usize, file: &SingleFile) -> PieceAssignment {
    let piece_size = file.piece_length.min(file.length - piece as u64 * file.piece_length) as u32;
    let mut blocks_to_download = Vec::new();

    for begin in (0..piece_size).step_by(BLOCK_SIZE as usize) {
        blocks_to_download.push(Block { begin, length: BLOCK_SIZE.min(piece_size - begin), state: BlockState::NotRequested, requested_at: None });
    }

    let mut piece_hash = [0u8; 20];
    piece_hash.copy_from_slice(&file.pieces[piece * 20..(piece + 1) * 20]);

    PieceAssignment { piece, blocks_to_download, piece_hash, last_progress_at: Instant::now() }
}

pub(crate) fn try_start_pending_peer_connections(
    peer_tasks: &HashMap<usize, PeerTask>,
    new_peer_tasks: &mut JoinSet<Result<(TcpStream, usize), String>>,
    peer_queue: &mut VecDeque<SocketAddrV4>,
    next_peer_id: &mut usize,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
) {
    let total_peer_tasks = peer_tasks.len() + new_peer_tasks.len();
    if total_peer_tasks < PEERS_PER_TORRENT {
        let pending_amount = PEERS_PER_TORRENT - total_peer_tasks;
        for _ in 0..pending_amount {
            let Some(new_peer) = peer_queue.pop_front() else {
                break;
            };
            let worker_peer_id = *next_peer_id;
            *next_peer_id += 1;
            new_peer_tasks.spawn(async move {
                let stream = get_peer_stream(new_peer, info_hash, peer_id).await.map_err(|err| format!("Peer {worker_peer_id} ({new_peer}): {err}"))?;
                Ok((stream, worker_peer_id))
            });
        }
    }
}

pub(crate) fn start_workers_for_new_connected_peers(
    new_peer_tasks: &mut JoinSet<Result<(TcpStream, usize), String>>,
    peer_tasks: &mut HashMap<usize, PeerTask>,
    event_tx: &mpsc::Sender<PeerWorkerEvent>,
    path: &Path,
    piece_length: u64,
    piece_count: usize,
    verified_downloaded_pieces: &HashSet<usize>,
) -> Result<(), String> {
    while let Some(result) = new_peer_tasks.try_join_next() {
        let (mut stream, worker_peer_id) = match result {
            Ok(Ok(peer)) => peer,
            Ok(Err(_)) => {
                continue;
            }
            Err(_) => {
                continue;
            }
        };
        let (command_tx, command_rx) = mpsc::channel::<PeerOrchestratorCommand>();
        let event_tx_del_peer = event_tx.clone();
        let mut worker_file_options = OpenOptions::new();
        worker_file_options.read(true);
        worker_file_options.write(true);
        let worker_file = worker_file_options.open(path).map_err(|err| err.to_string())?;
        let worker_file = tokio::fs::File::from_std(worker_file);
        let verified_downloaded_pieces = verified_downloaded_pieces.clone();
        let handle = tokio::spawn(async move {
            peer_worker(&mut stream, command_rx, event_tx_del_peer, worker_peer_id, piece_length, BLOCK_SIZE, piece_count, worker_file, verified_downloaded_pieces).await;
        });
        peer_tasks.insert(worker_peer_id, PeerTask { command_tx, handle, piece_index_downloading: None, available_pieces: HashSet::new() });
    }

    Ok(())
}

pub(crate) async fn process_peer_worker_events(
    event_rx: &mpsc::Receiver<PeerWorkerEvent>,
    peer_tasks: &mut HashMap<usize, PeerTask>,
    completed_pieces: &mut [bool],
    verified_downloaded_pieces: &mut HashSet<usize>,
    path: &mut PathBuf,
    hidden: &Path,
    completed_visible_path: &Path,
    peers_to_remove: &mut Vec<usize>,
) -> Result<(), String> {
    let piece_count = completed_pieces.len();
    while let Ok(event) = event_rx.try_recv() {
        match event {
            PeerWorkerEvent::AvailablePiecesEvent(available) => {
                let Some(task) = peer_tasks.get_mut(&available.peer_id) else {
                    continue;
                };
                for piece_index in available.pieces {
                    let piece_index_is_valid = piece_index < piece_count;
                    if piece_index_is_valid {
                        task.available_pieces.insert(piece_index);
                    }
                }
            }
            PeerWorkerEvent::PieceSuccessfullyCompletedEvent(piece_index) => {
                completed_pieces[piece_index] = true;
                verified_downloaded_pieces.insert(piece_index);

                let all_pieces_completed = verified_downloaded_pieces.len() == piece_count;
                let file_is_hidden = path.as_path() == hidden;
                let should_make_file_visible = all_pieces_completed && file_is_hidden;
                if should_make_file_visible {
                    tokio::fs::rename(hidden, completed_visible_path).await.map_err(|err| err.to_string())?;
                    *path = completed_visible_path.to_path_buf();
                }

                for (&peer_id, task) in peer_tasks.iter_mut() {
                    if task.piece_index_downloading == Some(piece_index) {
                        task.piece_index_downloading = None;
                    }
                    if task.command_tx.send(PeerOrchestratorCommand::NewPieceCommand(piece_index)).is_err() {
                        peers_to_remove.push(peer_id);
                    }
                }
            }
            PeerWorkerEvent::PeerFailedEvent(failure) => {
                remove_peer(failure.peer_id, peer_tasks);
            }
        }
    }

    Ok(())
}

pub(crate) fn assign_pieces_to_idle_workers(peer_tasks: &mut HashMap<usize, PeerTask>, completed_pieces: &[bool], single_file: &SingleFile, peers_to_remove: &mut Vec<usize>) {
    let mut downloading_pieces_indexes: HashSet<usize> = HashSet::new();
    for task in peer_tasks.values() {
        if let Some(piece_index) = task.piece_index_downloading {
            downloading_pieces_indexes.insert(piece_index);
        }
    }

    for (&peer_id, task) in peer_tasks {
        if task.piece_index_downloading.is_some() {
            continue;
        }
        let mut piece_to_assign = None;
        for piece_index in 0..completed_pieces.len() {
            let piece_is_completed = completed_pieces[piece_index];
            let piece_is_assigned = downloading_pieces_indexes.contains(&piece_index);
            let peer_has_piece = task.available_pieces.contains(&piece_index);
            let can_assign_piece = !piece_is_completed && !piece_is_assigned && peer_has_piece;

            if can_assign_piece {
                piece_to_assign = Some(piece_index);
                break;
            }
        }

        let Some(piece_index) = piece_to_assign else {
            continue;
        };
        let assignment = create_piece_assignment(piece_index, single_file);
        if task.command_tx.send(PeerOrchestratorCommand::PieceAssignmentCommand(assignment)).is_err() {
            peers_to_remove.push(peer_id);
            continue;
        }

        task.piece_index_downloading = Some(piece_index);
        downloading_pieces_indexes.insert(piece_index);
    }
}

pub(crate) fn remove_peer(peer_id: usize, peer_tasks: &mut HashMap<usize, PeerTask>) {
    if let Some(task) = peer_tasks.remove(&peer_id) {
        task.handle.abort();
    }
}
