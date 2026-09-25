mod peer;
mod peer_message;
mod torrent;
mod torrent_info;
mod tracker;

use crate::peer::{PeerLoopResult, build_handshake, connect_to_peer, create_pieces, peer_loop, reset_pending_requests, validate_peer_handshake};
use crate::torrent::{Info, SingleFile, Torrent};
use crate::torrent_info::get_info_bytes;
use crate::tracker::{TrackerResponse, announce_to_tracker, build_initial_tracker_request, build_url, parse_compact_peers};
use std::{fs, io};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::{Duration, timeout};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() {
    println!("iniciando cliente bittorrent");
    println!("leyendo archivo torrent");

    let bytes = fs::read("ubuntu-26.04.1-live-server-amd64.iso.torrent").expect("no se pudo leer el archivo .torrent");

    println!("archivo torrent leido: {} bytes", bytes.len());

    let torrent: Torrent = serde_bencode::from_bytes(&bytes).expect("no se pudo decodificar el torrent");

    println!("metadatos del torrent decodificados");

    let single_file: SingleFile = match torrent.info {
        Info::SingleFile(file) => file,
        Info::MultiFile(_) => {
            eprintln!("los torrents de varios archivos todavia no estan soportados");
            return;
        }
    };

    println!("torrent de un archivo seleccionado: {}", single_file.name);
    println!("tamaño total de la descarga: {} bytes", single_file.length);
    println!("longitud declarada de cada pieza: {} bytes", single_file.piece_length);

    let announce = torrent.announce.as_deref().expect("el torrent no contiene announce");

    if !announce.starts_with("https://") && !announce.starts_with("http://") {
        panic!("el tracker no usa http o https");
    }

    let info_bytes = get_info_bytes(&bytes).expect("no se pudieron extraer los bytes de info");

    println!("diccionario info extraido correctamente");

    let total_length = single_file.length;

    let listener = TcpListener::bind("0.0.0.0:0").await.expect("no se pudo abrir un puerto tcp");

    let address = listener.local_addr().expect("no se pudo obtener la direccion local");

    println!("puerto local asignado: {}", address.port());

    let tracker_request = build_initial_tracker_request(info_bytes, address.port(), total_length);

    println!("solicitud para el rastreador construida");

    let url = build_url(announce, &tracker_request);

    println!("url del rastreador: {url}");
    println!("enviando solicitud al rastreador");

    let response_bytes = announce_to_tracker(&url).await.expect("fallo la solicitud al tracker");

    println!("respuesta del rastreador recibida: {} bytes", response_bytes.len());

    let tracker_response: TrackerResponse = serde_bencode::from_bytes(&response_bytes).expect("respuesta bencode invalida");

    println!("respuesta del rastreador decodificada");

    let peers_bytes = tracker_response.peers.expect("no existen peers");

    let peers = parse_compact_peers(peers_bytes.as_ref()).expect("lista compacta de peers invalida");

    println!("cantidad de pares: {}", peers.len());

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

    println!("piezas preparadas para descargar: {}", pieces.len());

    match fs::File::create(&single_file.name) {
        Ok(_) => {
            println!("archivo de salida creado: {}", single_file.name);
        }
        Err(err) => {
            eprintln!("no se pudo crear el archivo de descarga: {err}");
            return;
        }
    }

    let mut peer_loop_result = PeerLoopResult::NoPeersAvailable;

    for peer in peers {
        println!("intentando conectar con el par {peer}");

        let mut stream = match connect_to_peer(peer).await {
            Ok(stream) => {
                println!("conectado al par {peer}");
                stream
            }
            Err(err) => {
                if err.kind() == io::ErrorKind::ConnectionRefused {
                    eprintln!("el par {peer} rechazo la conexion");
                } else if err.kind() == io::ErrorKind::TimedOut {
                    eprintln!("la conexion a {peer} supero los 2 segundos");
                } else {
                    eprintln!("no se pudo conectar a {peer}: {err}");
                }

                peer_loop_result = PeerLoopResult::ConnectionError(err);
                continue;
            }
        };

        let handshake_bytes = build_handshake(tracker_request.info_hash, tracker_request.peer_id);

        println!("enviando saludo inicial al par {peer}");

        if let Err(err) = stream.write_all(&handshake_bytes).await {
            eprintln!("no se pudo enviar el saludo inicial: {err}");
            peer_loop_result = PeerLoopResult::IoError(err);
            continue;
        }

        println!("saludo inicial enviado correctamente");

        let mut buf = [0u8; 68];

        println!("esperando saludo inicial del par {peer}");

        match timeout(HANDSHAKE_TIMEOUT, stream.read_exact(&mut buf)).await {
            Ok(Ok(_)) => {
                println!("saludo inicial recibido correctamente");
            }
            Ok(Err(err)) => {
                eprintln!("no se pudo leer el saludo inicial: {err}");
                peer_loop_result = PeerLoopResult::PeerDisconnected(err);
                continue;
            }
            Err(_) => {
                eprintln!("se agotaron los 10 segundos para recibir el saludo inicial");
                peer_loop_result = PeerLoopResult::HandshakeTimeout;
                continue;
            }
        }

        if let Err(err) = validate_peer_handshake(&buf, &tracker_request.info_hash) {
            eprintln!("{err}");
            peer_loop_result = PeerLoopResult::PeerError(err);
            continue;
        }

        println!("saludo inicial del par validado correctamente");
        println!("iniciando intercambio de mensajes con el par {peer}");

        let result = peer_loop(&mut stream, &mut pieces, &single_file.name, piece_length).await;
        let completed = matches!(result, PeerLoopResult::Completed);
        peer_loop_result = result;

        if completed {
            println!("descarga completada con el par {peer}");
            break;
        }

        eprintln!("el intento con el par {peer} termino: {peer_loop_result}");
        println!("preparando solicitudes pendientes para el siguiente par");
        reset_pending_requests(&mut pieces);
        continue;
    }

    println!("resultado final del ciclo de pares: {peer_loop_result}");
}
