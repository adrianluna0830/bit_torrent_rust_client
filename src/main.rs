use crate::cli::{read_download_path, read_torrent_path};
use crate::dht::announce_and_get_peers_dht;
use crate::logging::initialize_logging;
use crate::peer_orchestrator::{
    MAX_ACTIVE_PEERS, MAX_CONNECTING_PEERS, PeerTask, PeerWorkerEvent, assign_pieces_to_idle_workers, process_peer_worker_events, remove_peer, start_workers_for_new_connected_peers,
    try_start_pending_peer_connections,
};
use crate::torrent::{Info, torrent_from_bytes};
use crate::torrent_info::get_info_bytes;
use crate::tracker::{announce_and_get_peers, generate_id};
use sha1::{Digest, Sha1};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::net::SocketAddrV4;
use std::sync::mpsc;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio::time::{Duration, Instant, sleep};
mod cli;
mod dht;
mod logging;
mod peer;
mod peer_message;
mod peer_orchestrator;
mod peer_worker;
mod torrent;
mod torrent_info;
mod tracker;

#[tokio::main]
async fn main() -> Result<(), String> {
    initialize_logging();
    log::info!("Starting BitTorrent client");
    let listener = TcpListener::bind("0.0.0.0:0").await.map_err(|err| err.to_string())?;
    let port = listener.local_addr().map_err(|err| err.to_string())?.port();

    log::debug!("Tracker announce port: {port}");
    let torrent_path_string = read_torrent_path();
    let download_path = read_download_path();
    let bytes = fs::read(torrent_path_string.trim()).expect("Failed to read the torrent file");
    let torrent = match torrent_from_bytes(&bytes) {
        Ok(torrent) => torrent,
        Err(err) => {
            log::error!("Failed to parse torrent metadata: {err}");
            return Ok(());
        }
    };

    let info_bytes = get_info_bytes(&bytes).expect("Failed to extract the info dictionary");
    let mut hasher = Sha1::new();
    hasher.update(info_bytes);
    let info_hash: [u8; 20] = hasher.finalize().into();
    let peer_id = generate_id();

    let single_file = match &torrent.info {
        Info::SingleFile(file) => file,
        Info::MultiFile(_) => {
            log::warn!("Multi-file downloads are not implemented");
            return Ok(());
        }
    };

    let mut announce: Option<Vec<Vec<String>>> = None;
    if let Some(torrent_announce) = &torrent.announce {
        announce = Some(vec![vec![torrent_announce.clone()]]);
    }
    if !torrent.announce_list.is_empty() {
        announce = Some(torrent.announce_list.clone());
    }

    let mut discovered_peers = HashSet::new();
    if let Some(announce) = announce {
        match announce_and_get_peers(single_file.length, info_hash, peer_id, port, &announce).await {
            Ok(peers) => discovered_peers = peers,
            Err(err) => log::warn!("Tracker discovery failed: {err}"),
        }
    }
    match announce_and_get_peers_dht(info_hash).await {
        Ok(dht_peers) => discovered_peers.extend(dht_peers),
        Err(err) => log::warn!("DHT discovery failed: {err}"),
    }

    log::info!("Peer discovery finished: {} unique peers", discovered_peers.len());
    let piece_length = single_file.piece_length as u32;

    let hidden = download_path.join(format!(".{}", single_file.name));
    let completed_visible_path = download_path.join(&single_file.name);

    let hidden_file_exists = hidden.try_exists().map_err(|err| err.to_string())?;
    let completed_visible_file_exists = completed_visible_path.try_exists().map_err(|err| err.to_string())?;
    let both_files_exist = hidden_file_exists && completed_visible_file_exists;
    if both_files_exist {
        log::error!("Both incomplete and completed files exist");
        return Err("Both incomplete and completed files exist".to_string());
    }

    let mut file_options = OpenOptions::new();
    file_options.read(true);
    file_options.write(true);

    let mut file;
    if hidden_file_exists {
        log::info!("Opening incomplete file for resume");
        file = file_options.open(&hidden).map_err(|err| err.to_string())?;
    } else if completed_visible_file_exists {
        log::info!("Opening completed file for verification and seeding");
        file = file_options.open(&completed_visible_path).map_err(|err| err.to_string())?;
    } else {
        log::info!("Creating incomplete file of {} bytes", single_file.length);
        file_options.create_new(true);
        file = file_options.open(&hidden).map_err(|err| err.to_string())?;
        file.set_len(single_file.length).map_err(|err| err.to_string())?;
    }

    let file_metadata = file.metadata().map_err(|err| err.to_string())?;
    let file_size_matches_torrent = file_metadata.len() == single_file.length;
    if !file_size_matches_torrent {
        log::error!("File size does not match torrent length");
        return Err("File size does not match the torrent".to_string());
    }
    file.seek(SeekFrom::Start(0)).map_err(|err| err.to_string())?;
    let mut buff = vec![0u8; piece_length as usize];

    let mut pending = single_file.length;
    let piece_count = single_file.length.div_ceil(piece_length as u64) as usize;
    if single_file.pieces.len() != piece_count * 20 {
        log::error!("Hash count does not match the expected piece count");
        return Err("Hash count does not match the expected piece count".to_string());
    }

    log::info!("Verifying {piece_count} local pieces");
    let mut completed_pieces: Vec<bool> = Vec::with_capacity(piece_count);
    let mut index: usize = 0;
    while pending > 0 {
        let amount = (piece_length as u64).min(pending) as usize;
        file.read_exact(&mut buff[..amount]).map_err(|err| err.to_string())?;

        let slice = &buff[..amount];
        let hash: [u8; 20] = Sha1::digest(slice).into();
        let is_equal = hash == single_file.pieces[index * 20..(index + 1) * 20];
        completed_pieces.push(is_equal);
        log::trace!("Local piece {index}: verified={is_equal}");
        pending -= amount as u64;

        index += 1;
    }

    let mut peer_tasks: HashMap<usize, PeerTask> = HashMap::with_capacity(MAX_ACTIVE_PEERS);
    let mut peer_queue: VecDeque<SocketAddrV4> = VecDeque::new();
    for peer in discovered_peers {
        peer_queue.push_back(peer);
    }

    let mut new_peer_tasks: JoinSet<Result<(TcpStream, usize), String>> = JoinSet::new();
    let (event_tx, event_rx) = mpsc::channel::<PeerWorkerEvent>();
    let mut verified_downloaded_pieces: HashSet<usize> = HashSet::new();
    for piece_index in 0..piece_count {
        if completed_pieces[piece_index] {
            verified_downloaded_pieces.insert(piece_index);
        }
    }

    log::info!("Local verification finished: {}/{} pieces complete", verified_downloaded_pieces.len(), piece_count);
    let hidden_file_exists = hidden.try_exists().map_err(|err| err.to_string())?;
    let mut path;
    if hidden_file_exists {
        path = hidden.clone();
    } else {
        path = completed_visible_path.clone();
    }

    let all_pieces_completed = verified_downloaded_pieces.len() == piece_count;
    let file_is_hidden = path == hidden;
    let should_make_file_visible = all_pieces_completed && file_is_hidden;
    if should_make_file_visible {
        tokio::fs::rename(&hidden, &completed_visible_path).await.map_err(|err| err.to_string())?;
        path = completed_visible_path.clone();
        log::info!("All local pieces verified; file is now visible");
    }
    let piece_length = single_file.piece_length;
    let mut next_peer_id = 0;
    let mut last_progress_log = Instant::now();
    log::info!("Starting peer coordination; active peer limit: {MAX_ACTIVE_PEERS}; simultaneous connection attempt limit: {MAX_CONNECTING_PEERS}");

    loop {
        try_start_pending_peer_connections(&peer_tasks, &mut new_peer_tasks, &mut peer_queue, &mut next_peer_id, info_hash, peer_id);

        start_workers_for_new_connected_peers(&mut new_peer_tasks, &mut peer_tasks, &event_tx, &path, piece_length, piece_count, &verified_downloaded_pieces)?;

        let mut peers_to_remove: Vec<usize> = Vec::new();
        for (&peer_id, task) in &peer_tasks {
            let worker_has_stopped = task.handle.is_finished();
            if worker_has_stopped {
                log::debug!("Peer {peer_id}: worker has stopped");
                peers_to_remove.push(peer_id);
            }
        }

        process_peer_worker_events(&event_rx, &mut peer_tasks, &mut completed_pieces, &mut verified_downloaded_pieces, &mut path, &hidden, &completed_visible_path, &mut peers_to_remove).await?;

        for peer_id in peers_to_remove.drain(..) {
            remove_peer(peer_id, &mut peer_tasks);
        }

        assign_pieces_to_idle_workers(&mut peer_tasks, &completed_pieces, single_file, &mut peers_to_remove);

        for peer_id in peers_to_remove {
            remove_peer(peer_id, &mut peer_tasks);
        }

        if last_progress_log.elapsed() >= Duration::from_secs(1) {
            let completed_count = verified_downloaded_pieces.len();
            let progress;
            if piece_count == 0 {
                progress = 100.0;
            } else {
                progress = completed_count as f64 / piece_count as f64 * 100.0;
            }
            log::info!("Progress: {progress:.2}% ({completed_count}/{piece_count} pieces). Active: {}. Pending connections: {}. Queued: {}", peer_tasks.len(), new_peer_tasks.len(), peer_queue.len());
            last_progress_log = Instant::now();
        }

        let no_active_peers = peer_tasks.is_empty();
        let no_connections_in_progress = new_peer_tasks.is_empty();
        let no_queued_peers = peer_queue.is_empty();
        let no_peers_remaining = no_active_peers && no_connections_in_progress && no_queued_peers;
        if no_peers_remaining {
            let all_pieces_completed = verified_downloaded_pieces.len() == piece_count;
            if all_pieces_completed {
                log::info!("All pieces completed; no peers remain connected");
                break;
            }
            log::error!("Download cannot continue: no peers remain for missing pieces");
            return Err("No peers remain to complete the download".to_string());
        }

        sleep(Duration::from_millis(10)).await;
    }

    log::info!("BitTorrent client stopped");
    Ok(())
}
