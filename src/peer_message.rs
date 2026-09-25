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

        let message_length = u32::try_from(1 + content.len()).expect("el mensaje del peer es demasiado grande");
        let mut bytes = Vec::with_capacity(4 + message_length as usize);

        bytes.extend_from_slice(&message_length.to_be_bytes());
        bytes.push(message_id);
        bytes.extend_from_slice(&content);

        bytes
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 4 {
            return Err("el mensaje no contiene los 4 bytes de longitud".to_string());
        }

        let message_length = u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let expected_length = 4 + message_length;

        if bytes.len() != expected_length {
            return Err(format!("longitud incorrecta: se esperaban {expected_length} bytes, pero llegaron {}", bytes.len()));
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
                    return Err("el contenido de Piece debe tener al menos 8 bytes".to_string());
                }

                Ok(Self::Piece { index: Self::read_u32(content, 0), begin: Self::read_u32(content, 4), block: content[8..].to_vec() })
            }
            8 => {
                Self::require_content_length(content, 12, "Cancel")?;
                Ok(Self::Cancel { index: Self::read_u32(content, 0), begin: Self::read_u32(content, 4), length: Self::read_u32(content, 8) })
            }
            id => Err(format!("identificador de mensaje desconocido: {id}")),
        }
    }

    fn require_content_length(content: &[u8], expected: usize, message_name: &str) -> Result<(), String> {
        if content.len() != expected {
            return Err(format!("el contenido de {message_name} debe tener {expected} bytes, pero tiene {}", content.len()));
        }

        Ok(())
    }

    fn read_u32(bytes: &[u8], start: usize) -> u32 {
        u32::from_be_bytes(bytes[start..start + 4].try_into().unwrap())
    }
}
