mod dht;
mod peer;
mod peer_message;
mod torrent;
mod torrent_info;
mod tracker;

use crate::dht::announce_and_get_peers_dht;
use crate::peer::{PeerLoopResult, build_handshake, connect_to_peer, create_pieces, peer_loop, reset_pending_requests, validate_peer_handshake};
use crate::torrent::{Info, SingleFile, Torrent, get_torrent_from_magnet, torrent_from_bytes};
use crate::torrent_info::get_info_bytes;
use crate::tracker::{announce_and_get_peers, generate_id};
use sha1::{Digest, Sha1};
use std::collections::HashSet;
use std::net::SocketAddrV4;
use std::path::Path;
use std::{fs, io};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::{Duration, timeout};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut number: u8;
    loop {
        let mut input = String::new();
        io::stdin().read_line(&mut input).expect("Error al leer el input");
        match input.trim().parse::<u8>() {
            Ok(n) => {
                number = n;
                if number != 1 && number != 0 {
                    continue;
                }
                break;
            }
            Err(_) => {}
        }
    }
    let info_hash: [u8; 20];
    let peer_id: [u8; 20];
    let torrent: Torrent;
    let download_path: String;
    let mut peers: HashSet<SocketAddrV4>;
    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let port = listener.local_addr()?.port();

    if number == 1 {
        let mut input = String::new();

        io::stdin().read_line(&mut input).expect("Error al leer el input");

        let mut download_path_string = String::new();

        loop {
            io::stdin().read_line(&mut download_path_string).expect("Error al leer el input");

            let download_path = Path::new(download_path_string.trim());

            if download_path.is_dir() {
                break;
            }
        }
        download_path = download_path_string;
        peers = HashSet::new();
        torrent = get_torrent_from_magnet(&input, &peers).await.expect("No se pudo obtener el torrent desde el magnet");
        return Ok(());
    } else {
        let mut torrent_path_string = String::new();

        loop {
            io::stdin().read_line(&mut torrent_path_string).expect("Error al leer el input");

            let torrent_path = Path::new(torrent_path_string.trim());

            if torrent_path.is_file() {
                break;
            }
        }
        let mut download_path_string = String::new();

        loop {
            io::stdin().read_line(&mut download_path_string).expect("Error al leer el input");

            let download_path = Path::new(download_path_string.trim());

            if download_path.is_dir() {
                break;
            }
        }
        let bytes = fs::read(torrent_path_string).expect("no se pudo leer el archivo .torrent");
        torrent = torrent_from_bytes(&bytes).expect("No se pudo leer el torrent");

        let info_bytes = get_info_bytes(&bytes).expect("no se pudieron extraer los bytes de info");

        let mut hasher = Sha1::new();
        hasher.update(info_bytes);

        info_hash = hasher.finalize().into();
        peer_id = generate_id();
        download_path = download_path_string;

        let single_file = match &torrent.info {
            Info::SingleFile(file) => file,
            Info::MultiFile(_) => return Ok(()),
        };

        let mut announce: Option<Vec<Vec<String>>> = None;

        if let Some(torrent_announce) = &torrent.announce {
            announce = Some(vec![vec![torrent_announce.clone()]]);
        }

        if !torrent.announce_list.is_empty() {
            announce = Some(torrent.announce_list.clone());
        }

        peers = HashSet::new();

        if let Some(announce) = announce {
            peers = announce_and_get_peers(single_file.length, info_hash, peer_id, port, &announce).await.expect("");
        }

        let dht_peers = announce_and_get_peers_dht(info_hash).await.expect("no se pudieron obtener peers mediante DHT");

        println!("peers obtenidos mediante trackers: {}", peers.len());
        println!("peers obtenidos mediante DHT: {}", dht_peers.len());

        peers.extend(dht_peers);
    }

    return Ok(());

    let single_file: &SingleFile = match &torrent.info {
        Info::SingleFile(file) => file,
        Info::MultiFile(_) => return Ok(()),
    };

    let piece_length = match u32::try_from(single_file.piece_length) {
        Ok(piece_length) => piece_length,
        Err(_) => return Ok(()),
    };

    let mut pieces = match create_pieces(single_file.length, piece_length, single_file.pieces.as_ref()) {
        Ok(pieces) => pieces,
        Err(_) => return Ok(()),
    };

    match fs::File::create(&single_file.name) {
        Ok(_) => {}
        Err(_) => return Ok(()),
    }

    let mut peer_loop_result = PeerLoopResult::NoPeersAvailable;

    for peer in peers {
        let mut stream = match connect_to_peer(peer).await {
            Ok(stream) => stream,
            Err(err) => {
                peer_loop_result = PeerLoopResult::ConnectionError(err);
                continue;
            }
        };

        let handshake_bytes = build_handshake(info_hash, peer_id);

        if let Err(err) = stream.write_all(&handshake_bytes).await {
            peer_loop_result = PeerLoopResult::IoError(err);
            continue;
        }

        let mut buf = [0u8; 68];

        match timeout(HANDSHAKE_TIMEOUT, stream.read_exact(&mut buf)).await {
            Ok(Ok(_)) => {}
            Ok(Err(err)) => {
                peer_loop_result = PeerLoopResult::PeerDisconnected(err);
                continue;
            }
            Err(_) => {
                peer_loop_result = PeerLoopResult::HandshakeTimeout;
                continue;
            }
        }

        if let Err(err) = validate_peer_handshake(&buf, &info_hash) {
            peer_loop_result = PeerLoopResult::PeerError(err);
            continue;
        }

        let result = peer_loop(&mut stream, &mut pieces, &single_file.name, piece_length).await;
        let completed = matches!(result, PeerLoopResult::Completed);
        peer_loop_result = result;

        if completed {
            break;
        }

        reset_pending_requests(&mut pieces);
        continue;
    }

    Ok(())
}
