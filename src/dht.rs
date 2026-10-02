use crate::tracker::{generate_id, generate_random_u32, parse_compact_peers};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use std::collections::{HashMap, HashSet};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use tokio::net::UdpSocket;
use tokio::time::{Duration, timeout};

const HOST: &str = "dht.libtorrent.org";
const HOST_PORT: u16 = 25401;
const K: usize = 8;
const MAX_QUERIED_NODES: usize = 100;
const DHT_RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) async fn announce_and_get_peers_dht(info_hash: [u8; 20]) -> Result<HashSet<SocketAddrV4>, String> {
    log::info!("DHT discovery started; resolving {HOST}");
    let addresses = tokio::net::lookup_host((HOST, HOST_PORT)).await.map_err(|err| err.to_string())?;
    let mut ipv4_address = None;
    for address in addresses {
        if let SocketAddr::V4(address) = address {
            ipv4_address = Some(address);
            break;
        }
    }
    let initial_address = match ipv4_address {
        Some(address) => address,
        None => return Err("DHT bootstrap node has no IPv4 address".to_string()),
    };

    let node_id = generate_id();
    let mut queried_nodes = HashSet::new();
    let mut known_nodes = HashMap::new();
    let mut found_peers = HashSet::new();

    queried_nodes.insert(initial_address);
    let (initial_peers, initial_nodes) = query_node(initial_address, node_id, info_hash).await?;
    log::debug!("DHT bootstrap {initial_address}: {} peers, {} nodes", initial_peers.len(), initial_nodes.len());
    found_peers.extend(initial_peers);

    for node in initial_nodes {
        if !queried_nodes.contains(&node.address) {
            known_nodes.entry(node.address).or_insert(node);
        }
    }

    loop {
        if queried_nodes.len() >= MAX_QUERIED_NODES {
            break;
        }

        let Some(node) = next_node(&known_nodes, &queried_nodes, &info_hash) else {
            break;
        };

        queried_nodes.insert(node.address);

        match query_node(node.address, node_id, info_hash).await {
            Ok((peers, new_nodes)) => {
                log::debug!("DHT node {}: {} peers, {} nodes", node.address, peers.len(), new_nodes.len());
                found_peers.extend(peers);

                for new_node in new_nodes {
                    if !queried_nodes.contains(&new_node.address) {
                        known_nodes.entry(new_node.address).or_insert(new_node);
                    }
                }
            }
            Err(err) => {
                log::warn!("DHT node {} failed: {err}", node.address);
                known_nodes.remove(&node.address);
            }
        }
    }

    log::info!("DHT discovery finished: {} unique peers from {} nodes", found_peers.len(), queried_nodes.len());
    Ok(found_peers)
}

fn xor_distance(node_id: &[u8; 20], info_hash: &[u8; 20]) -> [u8; 20] {
    let mut distance = [0u8; 20];

    for index in 0..20 {
        distance[index] = node_id[index] ^ info_hash[index];
    }

    distance
}

fn next_node(known_nodes: &HashMap<SocketAddrV4, DhtNode>, queried_nodes: &HashSet<SocketAddrV4>, info_hash: &[u8; 20]) -> Option<DhtNode> {
    let mut sorted_nodes = Vec::new();

    for node in known_nodes.values() {
        sorted_nodes.push(*node);
    }

    sorted_nodes.sort_by_key(|node| {
        let distance = xor_distance(&node.id, info_hash);
        (distance, node.address)
    });

    let mut checked_nodes = 0;

    for node in sorted_nodes {
        if checked_nodes == K {
            break;
        }

        checked_nodes += 1;

        if !queried_nodes.contains(&node.address) {
            return Some(node);
        }
    }

    None
}

async fn query_node(address: SocketAddrV4, node_id: [u8; 20], info_hash: [u8; 20]) -> Result<(Vec<SocketAddrV4>, Vec<DhtNode>), String> {
    let socket = UdpSocket::bind("0.0.0.0:0").await.map_err(|err| err.to_string())?;
    let query = GetPeersQuery::new(node_id, info_hash);
    let bytes = serde_bencode::to_bytes(&query).map_err(|err| err.to_string())?;
    socket.send_to(&bytes, address).await.map_err(|err| err.to_string())?;
    log::debug!("DHT node {address}: sent get_peers query");

    let mut buffer = vec![0u8; 4096];
    let response_result = timeout(DHT_RESPONSE_TIMEOUT, socket.recv_from(&mut buffer)).await;
    let read_result = match response_result {
        Ok(result) => result,
        Err(_) => return Err("DHT response timed out after 10 seconds".to_string()),
    };
    let (received_bytes, sender) = read_result.map_err(|err| err.to_string())?;

    if sender != SocketAddr::V4(address) {
        return Err("DHT response came from a different node".to_string());
    }

    log::debug!("DHT node {address}: received response, {received_bytes} bytes");
    let reply: GetPeersResponse = serde_bencode::from_bytes(&buffer[..received_bytes]).map_err(|error| error.to_string())?;

    if query.transaction_id.as_slice() != reply.transaction_id.as_ref() {
        return Err("DHT response transaction ID does not match".to_string());
    }

    if reply.message_type != "r" {
        log::warn!("DHT node {address}: rejected query or returned an unexpected response type");
        return match reply.error {
            Some((code, _)) => Err(format!("DHT node reported error {code}")),
            None => Err("Unexpected DHT response type".to_string()),
        };
    }

    let Some(response) = reply.response else {
        return Err("DHT response is missing the response dictionary".to_string());
    };

    let mut found_peers = Vec::new();
    let mut new_nodes = Vec::new();

    if let Some(values) = response.values {
        for peer_bytes in values {
            found_peers.extend(parse_compact_peers(peer_bytes.as_ref())?);
        }
    }

    if let Some(nodes) = response.nodes {
        new_nodes = parse_compact_nodes(nodes.as_ref())?;
    }

    Ok((found_peers, new_nodes))
}

fn parse_compact_nodes(bytes: &[u8]) -> Result<Vec<DhtNode>, String> {
    if bytes.len() % 26 != 0 {
        return Err("DHT node list has an invalid length".to_string());
    }

    let mut nodes = Vec::new();

    for node_bytes in bytes.chunks_exact(26) {
        let id = node_bytes[0..20].try_into().unwrap();
        let ip = Ipv4Addr::new(node_bytes[20], node_bytes[21], node_bytes[22], node_bytes[23]);
        let port = u16::from_be_bytes([node_bytes[24], node_bytes[25]]);

        nodes.push(DhtNode { id, address: SocketAddrV4::new(ip, port) });
    }

    log::trace!("Decoded {} DHT nodes", nodes.len());
    Ok(nodes)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DhtNode {
    id: [u8; 20],
    address: SocketAddrV4,
}

#[derive(Serialize)]
struct GetPeersQuery {
    #[serde(rename = "t", with = "serde_bytes")]
    transaction_id: [u8; 2],

    #[serde(rename = "y")]
    message_type: &'static str,

    #[serde(rename = "q")]
    method: &'static str,

    #[serde(rename = "a")]
    arguments: GetPeersArguments,
}

#[derive(Serialize)]
struct GetPeersArguments {
    #[serde(rename = "id", with = "serde_bytes")]
    node_id: [u8; 20],

    #[serde(with = "serde_bytes")]
    info_hash: [u8; 20],
}

impl GetPeersQuery {
    fn new(node_id: [u8; 20], info_hash: [u8; 20]) -> Self {
        let random_bytes = generate_random_u32().to_be_bytes();

        Self { transaction_id: [random_bytes[0], random_bytes[1]], message_type: "q", method: "get_peers", arguments: GetPeersArguments { node_id, info_hash } }
    }
}

#[derive(Debug, Deserialize)]
struct GetPeersResponse {
    #[serde(rename = "t")]
    transaction_id: ByteBuf,

    #[serde(rename = "y")]
    message_type: String,

    #[serde(rename = "r")]
    response: Option<GetPeersResponseData>,

    #[serde(rename = "e")]
    error: Option<(i64, String)>,
}

#[derive(Debug, Deserialize)]
struct GetPeersResponseData {
    #[serde(rename = "id")]
    node_id: ByteBuf,

    token: Option<ByteBuf>,
    nodes: Option<ByteBuf>,
    values: Option<Vec<ByteBuf>>,
}
