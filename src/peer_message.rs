use std::fmt;
use std::io;
use tokio::net::TcpStream;

#[derive(Debug)]
pub(crate) enum PeerMessage {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(Vec<u8>),
    Request { index: u32, begin: u32, length: u32 },
    Piece { index: u32, begin: u32, block: Vec<u8> },
    Cancel { index: u32, begin: u32, length: u32 },
}

impl fmt::Display for PeerMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KeepAlive => write!(formatter, "KeepAlive"),
            Self::Choke => write!(formatter, "Choke"),
            Self::Unchoke => write!(formatter, "Unchoke"),
            Self::Interested => write!(formatter, "Interested"),
            Self::NotInterested => write!(formatter, "NotInterested"),
            Self::Have(index) => write!(formatter, "Have: piece {index}"),
            Self::Bitfield(bytes) => write!(formatter, "Bitfield: {} bytes", bytes.len()),
            Self::Request { index, begin, length } => write!(formatter, "Request: piece {index}, offset {begin}, length {length}"),
            Self::Piece { index, begin, block } => write!(formatter, "Piece: piece {index}, offset {begin}, length {}", block.len()),
            Self::Cancel { index, begin, length } => write!(formatter, "Cancel: piece {index}, offset {begin}, length {length}"),
        }
    }
}

impl PeerMessage {
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        if let Self::KeepAlive = self {
            return 0u32.to_be_bytes().to_vec();
        }

        let mut content = Vec::new();

        let message_id = match self {
            Self::KeepAlive => unreachable!(),
            Self::Choke => 0,
            Self::Unchoke => 1,
            Self::Interested => 2,
            Self::NotInterested => 3,
            Self::Have(index) => {
                content.extend_from_slice(&index.to_be_bytes());
                4
            }
            Self::Bitfield(bitfield) => {
                content.extend_from_slice(bitfield);
                5
            }
            Self::Request { index, begin, length } => {
                content.extend_from_slice(&index.to_be_bytes());
                content.extend_from_slice(&begin.to_be_bytes());
                content.extend_from_slice(&length.to_be_bytes());
                6
            }
            Self::Piece { index, begin, block } => {
                content.extend_from_slice(&index.to_be_bytes());
                content.extend_from_slice(&begin.to_be_bytes());
                content.extend_from_slice(block);
                7
            }
            Self::Cancel { index, begin, length } => {
                content.extend_from_slice(&index.to_be_bytes());
                content.extend_from_slice(&begin.to_be_bytes());
                content.extend_from_slice(&length.to_be_bytes());
                8
            }
        };

        let message_length = (1 + content.len()) as u32;
        let mut bytes = Vec::with_capacity(4 + message_length as usize);

        bytes.extend_from_slice(&message_length.to_be_bytes());
        bytes.push(message_id);
        bytes.extend_from_slice(&content);

        bytes
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 4 {
            return Err("Message is missing its length prefix".to_string());
        }

        let message_length = u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let expected_length = 4 + message_length;

        if bytes.len() != expected_length {
            return Err(format!("Invalid message length: expected {expected_length}, received {}", bytes.len()));
        }

        if message_length == 0 {
            return Ok(Self::KeepAlive);
        }

        let message_id = bytes[4];
        let content = &bytes[5..];

        match message_id {
            0 => {
                Self::require_content_length(content, 0, "Choke")?;
                Ok(Self::Choke)
            }
            1 => {
                Self::require_content_length(content, 0, "Unchoke")?;
                Ok(Self::Unchoke)
            }
            2 => {
                Self::require_content_length(content, 0, "Interested")?;
                Ok(Self::Interested)
            }
            3 => {
                Self::require_content_length(content, 0, "NotInterested")?;
                Ok(Self::NotInterested)
            }
            4 => {
                Self::require_content_length(content, 4, "Have")?;
                Ok(Self::Have(Self::read_u32(content, 0)))
            }
            5 => Ok(Self::Bitfield(content.to_vec())),
            6 => {
                Self::require_content_length(content, 12, "Request")?;
                Ok(Self::Request { index: Self::read_u32(content, 0), begin: Self::read_u32(content, 4), length: Self::read_u32(content, 8) })
            }
            7 => {
                if content.len() < 8 {
                    return Err("Piece message is missing its block header".to_string());
                }

                Ok(Self::Piece { index: Self::read_u32(content, 0), begin: Self::read_u32(content, 4), block: content[8..].to_vec() })
            }
            8 => {
                Self::require_content_length(content, 12, "Cancel")?;
                Ok(Self::Cancel { index: Self::read_u32(content, 0), begin: Self::read_u32(content, 4), length: Self::read_u32(content, 8) })
            }
            id => Err(format!("Unknown message ID: {id}")),
        }
    }

    fn require_content_length(content: &[u8], expected: usize, message_name: &str) -> Result<(), String> {
        if content.len() != expected {
            return Err(format!("Invalid {message_name} payload length: expected {expected}, received {}", content.len()));
        }

        Ok(())
    }

    fn read_u32(bytes: &[u8], start: usize) -> u32 {
        let end = start + 4;
        let number_bytes: [u8; 4] = bytes[start..end].try_into().unwrap();
        u32::from_be_bytes(number_bytes)
    }
}

pub(crate) fn try_read_peer_message(stream: &TcpStream, buffer: &mut Vec<u8>) -> Result<Option<PeerMessage>, String> {
    let mut chunk = [0u8; 4096];

    loop {
        if buffer.len() >= 4 {
            let length = u32::from_be_bytes(buffer[..4].try_into().unwrap()) as usize;
            let total = 4 + length;

            if buffer.len() >= total {
                let message = PeerMessage::from_bytes(&buffer[..total])?;
                buffer.drain(..total);
                return Ok(Some(message));
            }
        }

        match stream.try_read(&mut chunk) {
            Ok(0) => return Err("Peer closed the connection".to_string()),
            Ok(size) => buffer.extend_from_slice(&chunk[..size]),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(None),
            Err(err) => return Err(err.to_string()),
        }
    }
}
