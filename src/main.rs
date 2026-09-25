mod peer;
mod peer_message;
mod torrent;
mod torrent_info;
mod tracker;

use crate::peer::{build_handshake, connect_to_peer, create_pieces, peer_loop};
use crate::torrent::{Info, SingleFile, Torrent};
use crate::torrent_info::get_info_bytes;
use crate::tracker::{TrackerResponse, announce_to_tracker, build_initial_tracker_request, build_url, parse_compact_peers};
use std::{fs, io};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let bytes = fs::read("ubuntu-24.04.4-desktop-arm64.iso.torrent").expect("no se pudo leer el archivo .torrent");

    let torrent: Torrent = serde_bencode::from_bytes(&bytes).expect("no se pudo decodificar el torrent");

    let single_file: SingleFile = match torrent.info {
        Info::SingleFile(file) => file,
        Info::MultiFile(_) => {
            eprintln!("los torrents de varios archivos todavia no estan soportados");
            return;
        }
    };

    let announce = torrent.announce.as_deref().expect("el torrent no contiene announce");

    if !announce.starts_with("https://") && !announce.starts_with("http://") {
        panic!("el tracker no usa http o https");
    }

    let info_bytes = get_info_bytes(&bytes).expect("no se pudieron extraer los bytes de info");

    let total_length = single_file.length;

    let listener = TcpListener::bind("0.0.0.0:0").await.expect("no se pudo abrir un puerto tcp");

    let address = listener.local_addr().expect("no se pudo obtener la direccion local");

    let tracker_request = build_initial_tracker_request(info_bytes, address.port(), total_length);

    let url = build_url(announce, &tracker_request);

    println!("url del rastreador: {url}");

    let response_bytes = announce_to_tracker(&url).await.expect("fallo la solicitud al tracker");

    let tracker_response: TrackerResponse = serde_bencode::from_bytes(&response_bytes).expect("respuesta bencode invalida");

    let peers_bytes = tracker_response.peers.expect("no existen peers");

    let peers = parse_compact_peers(peers_bytes.as_ref()).expect("lista compacta de peers invalida");

    let mut stream: Option<TcpStream> = None;

    for peer in peers {
        match connect_to_peer(peer).await {
            Ok(connected_stream) => {
                println!("conectado al par {peer}");
                stream = Some(connected_stream);
                break;
            }
            Err(err) if err.kind() == io::ErrorKind::ConnectionRefused => {
                eprintln!("el par {peer} rechazo la conexion");
            }
            Err(err) if err.kind() == io::ErrorKind::TimedOut => {
                eprintln!("la conexion a {peer} supero los 2 segundos");
            }
            Err(err) => {
                eprintln!("no se pudo conectar a {peer}: {err}");
            }
        }
    }

    let Some(mut stream) = stream else {
        eprintln!("no se pudo conectar con ningun par");
        return;
    };

    let handshake_bytes = build_handshake(tracker_request.info_hash, tracker_request.peer_id);

    match stream.write_all(&handshake_bytes).await {
        Ok(()) => {
            println!("saludo inicial enviado correctamente");
        }
        Err(err) => {
            eprintln!("no se pudo enviar el saludo inicial: {err}");
            return;
        }
    }
    let mut buf = [0u8; 68];
    match stream.read_exact(&mut buf).await {
        Ok(_) => {
            println!("saludo inicial recibido correctamente");
        }
        Err(err) => {
            eprintln!("no se pudo leer el saludo inicial: {err}");
            return;
        }
    }

    if buf[0] != 19 {
        eprintln!("longitud del protocolo invalida: {}", buf[0]);
        return;
    }

    if &buf[1..20] != b"BitTorrent protocol" {
        eprintln!("el par no utiliza el protocolo bittorrent");
        return;
    }

    let received_info_hash = &buf[28..48];

    if received_info_hash != tracker_request.info_hash.as_slice() {
        eprintln!("el hash de informacion recibido no coincide");
        return;
    }

    let peer_id = &buf[48..68];

    println!("saludo inicial del par validado correctamente");
    println!("identificador del par: {peer_id:?}");
    let piece_length = match u32::try_from(single_file.piece_length) {
        Ok(piece_length) => piece_length,
        Err(_) => {
            eprintln!("la longitud de pieza es demasiado grande: {}", single_file.piece_length);
            return;
        }
    };

    let mut pieces = match create_pieces(single_file.length, piece_length, single_file.pieces.as_ref()) {
        Ok(pieces) => pieces,
        Err(err) => {
            eprintln!("no se pudieron crear las piezas: {err}");
            return;
        }
    };

    match fs::File::create(&single_file.name) {
        Ok(_) => {}
        Err(err) => {
            eprintln!("no se pudo crear el archivo de descarga: {err}");
            return;
        }
    }

    peer_loop(&mut stream, &mut pieces, &single_file.name, piece_length).await;
}
