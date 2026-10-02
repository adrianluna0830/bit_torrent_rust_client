use crate::torrent::Info;

#[derive(Debug, PartialEq, Eq)]
enum BencodeFieldType {
    Integer,
    List,
    ByteString,
    Dictionary,
}

pub(crate) fn get_info_length(info: &Info) -> u64 {
    match info {
        Info::SingleFile(file) => file.length,
        Info::MultiFile(info) => {
            let mut total_length = 0;
            for file in &info.files {
                total_length += file.length;
            }
            total_length
        }
    }
}

pub(crate) fn get_info_bytes(bytes: &[u8]) -> Result<&[u8], String> {
    if bytes.is_empty() {
        return Err("Torrent file is empty".to_string());
    }

    if bytes[0] != b'd' {
        return Err("Torrent does not start with a bencoded dictionary".to_string());
    }

    let mut position: usize = 1;
    let mut info_range: Option<(usize, usize)> = None;

    loop {
        if position >= bytes.len() {
            return Err("Root dictionary is missing its terminator".to_string());
        }

        if bytes[position] == b'e' {
            let position_after_dictionary = position + 1;

            if position_after_dictionary != bytes.len() {
                return Err("Unexpected data after the root dictionary".to_string());
            }

            return match info_range {
                Some((start, end)) => Ok(&bytes[start..end]),
                None => Err("Root dictionary is missing the info key".to_string()),
            };
        }

        let (key, value_start) = read_bencode_byte_string(bytes, position)?;

        let value_end = skip_bencode_field(value_start, bytes)?;

        if key == b"info" {
            if info_range.is_some() {
                return Err("Torrent contains duplicate info keys".to_string());
            }

            if bytes.get(value_start) != Some(&b'd') {
                return Err("Info value is not a dictionary".to_string());
            }

            info_range = Some((value_start, value_end));
        }

        position = value_end;
    }
}

fn get_bencode_field(byte: u8) -> Result<BencodeFieldType, String> {
    match byte {
        b'i' => Ok(BencodeFieldType::Integer),
        b'l' => Ok(BencodeFieldType::List),
        b'd' => Ok(BencodeFieldType::Dictionary),
        b'0'..=b'9' => Ok(BencodeFieldType::ByteString),
        _ => Err("Unknown bencode token".to_string()),
    }
}

fn skip_bencode_field(position: usize, bytes: &[u8]) -> Result<usize, String> {
    let byte = match bytes.get(position) {
        Some(&byte) => byte,
        None => return Err("se intento leer fuera del archivo".to_string()),
    };

    match get_bencode_field(byte)? {
        BencodeFieldType::Integer => skip_bencode_integer(position, bytes),
        BencodeFieldType::List => skip_bencode_list(position, bytes),
        BencodeFieldType::ByteString => skip_bencode_byte_string(position, bytes),
        BencodeFieldType::Dictionary => skip_bencode_dictionary(position, bytes),
    }
}

fn read_bencode_byte_string(bytes: &[u8], position: usize) -> Result<(&[u8], usize), String> {
    let first_byte = match bytes.get(position) {
        Some(&byte) => byte,
        None => return Err("se esperaba una cadena, pero termino el archivo".to_string()),
    };

    if !first_byte.is_ascii_digit() {
        return Err(format!("Expected a string length at position {position}"));
    }

    let mut cursor = position;
    let mut length: usize = 0;

    while cursor < bytes.len() && bytes[cursor] != b':' {
        let byte = bytes[cursor];

        if !byte.is_ascii_digit() {
            return Err(format!("Invalid string length at position {cursor}"));
        }

        let digit = (byte - b'0') as usize;

        length = length * 10 + digit;

        cursor += 1;
    }

    if cursor >= bytes.len() {
        return Err("String length is missing its separator".to_string());
    }

    let content_start = cursor + 1;
    let content_end = content_start + length;

    if content_end > bytes.len() {
        return Err("String length exceeds the available data".to_string());
    }

    Ok((&bytes[content_start..content_end], content_end))
}

fn skip_bencode_byte_string(position: usize, bytes: &[u8]) -> Result<usize, String> {
    let (_, next_position) = read_bencode_byte_string(bytes, position)?;
    Ok(next_position)
}

fn skip_bencode_integer(position: usize, bytes: &[u8]) -> Result<usize, String> {
    let mut cursor = position + 1;

    if cursor >= bytes.len() {
        return Err("Integer is incomplete".to_string());
    }

    if bytes[cursor] == b'-' {
        cursor += 1;
    }

    let digits_start = cursor;

    while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
        cursor += 1;
    }

    if cursor == digits_start {
        return Err(format!("Integer at position {position} contains no digits"));
    }

    let integer_has_terminator = cursor < bytes.len() && bytes[cursor] == b'e';
    if !integer_has_terminator {
        return Err(format!("Integer at position {position} is missing its terminator"));
    }

    Ok(cursor + 1)
}

fn skip_bencode_list(position: usize, bytes: &[u8]) -> Result<usize, String> {
    let mut cursor = position + 1;

    loop {
        if cursor >= bytes.len() {
            return Err("List is missing its terminator".to_string());
        }

        if bytes[cursor] == b'e' {
            return Ok(cursor + 1);
        }

        cursor = skip_bencode_field(cursor, bytes)?;
    }
}

fn skip_bencode_dictionary(position: usize, bytes: &[u8]) -> Result<usize, String> {
    let mut cursor = position + 1;

    loop {
        if cursor >= bytes.len() {
            return Err("Dictionary is missing its terminator".to_string());
        }

        if bytes[cursor] == b'e' {
            return Ok(cursor + 1);
        }

        let (_, position_after_key) = read_bencode_byte_string(bytes, cursor)?;

        cursor = skip_bencode_field(position_after_key, bytes)?;
    }
}
