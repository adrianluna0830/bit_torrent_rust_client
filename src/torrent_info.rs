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
        Info::MultiFile(info) => info.files.iter().map(|file| file.length).sum(),
    }
}

pub(crate) fn get_info_bytes(bytes: &[u8]) -> Result<&[u8], String> {
    if bytes.is_empty() {
        return Err("el archivo esta vacio".to_string());
    }

    if bytes[0] != b'd' {
        return Err("el torrent no comienza con un diccionario bencode".to_string());
    }

    let mut position: usize = 1;
    let mut info_range: Option<(usize, usize)> = None;

    loop {
        if position >= bytes.len() {
            return Err("el diccionario principal no tiene una 'e' final".to_string());
        }

        // La 'e' termina el diccionario principal.
        if bytes[position] == b'e' {
            let position_after_dictionary = position + 1;

            if position_after_dictionary != bytes.len() {
                return Err("hay bytes adicionales despues del diccionario principal".to_string());
            }

            return match info_range {
                Some((start, end)) => Ok(&bytes[start..end]),
                None => Err("el diccionario principal no contiene la clave info".to_string()),
            };
        }

        // Las claves de un diccionario bencode siempre son byte strings.
        let (key, value_start) = read_bencode_byte_string(bytes, position)?;

        // Saltamos el valor completo para descubrir dónde termina.
        let value_end = skip_bencode_field(value_start, bytes)?;

        if key == b"info" {
            if info_range.is_some() {
                return Err("el torrent contiene mas de una clave info".to_string());
            }

            if bytes.get(value_start) != Some(&b'd') {
                return Err("el valor de info no es un diccionario".to_string());
            }

            info_range = Some((value_start, value_end));
        }

        // El próximo campo comienza después del valor actual.
        position = value_end;
    }
}

fn get_bencode_field(byte: u8) -> Result<BencodeFieldType, String> {
    match byte {
        b'i' => Ok(BencodeFieldType::Integer),
        b'l' => Ok(BencodeFieldType::List),
        b'd' => Ok(BencodeFieldType::Dictionary),
        b'0'..=b'9' => Ok(BencodeFieldType::ByteString),
        _ => Err(format!("byte bencode desconocido: {byte}")),
    }
}

fn skip_bencode_field(position: usize, bytes: &[u8]) -> Result<usize, String> {
    let byte = bytes.get(position).copied().ok_or_else(|| "se intento leer fuera del archivo".to_string())?;

    match get_bencode_field(byte)? {
        BencodeFieldType::Integer => skip_bencode_integer(position, bytes),
        BencodeFieldType::List => skip_bencode_list(position, bytes),
        BencodeFieldType::ByteString => skip_bencode_byte_string(position, bytes),
        BencodeFieldType::Dictionary => skip_bencode_dictionary(position, bytes),
    }
}

fn read_bencode_byte_string(bytes: &[u8], position: usize) -> Result<(&[u8], usize), String> {
    let first_byte = bytes.get(position).copied().ok_or_else(|| "se esperaba una cadena, pero termino el archivo".to_string())?;

    if !first_byte.is_ascii_digit() {
        return Err(format!("se esperaba la longitud de una cadena en la posicion {position}"));
    }

    let mut cursor = position;
    let mut length: usize = 0;

    while cursor < bytes.len() && bytes[cursor] != b':' {
        let byte = bytes[cursor];

        if !byte.is_ascii_digit() {
            return Err(format!("longitud de cadena invalida en la posicion {cursor}"));
        }

        let digit = (byte - b'0') as usize;

        length = length.checked_mul(10).and_then(|value| value.checked_add(digit)).ok_or_else(|| "la longitud de la cadena es demasiado grande".to_string())?;

        cursor += 1;
    }

    if cursor >= bytes.len() {
        return Err("no se encontro ':' despues de la longitud".to_string());
    }

    let content_start = cursor + 1;
    let content_end = content_start.checked_add(length).ok_or_else(|| "la posicion final de la cadena es demasiado grande".to_string())?;

    if content_end > bytes.len() {
        return Err("la cadena declara mas bytes de los disponibles".to_string());
    }

    Ok((&bytes[content_start..content_end], content_end))
}

fn skip_bencode_byte_string(position: usize, bytes: &[u8]) -> Result<usize, String> {
    let (_, next_position) = read_bencode_byte_string(bytes, position)?;
    Ok(next_position)
}

fn skip_bencode_integer(position: usize, bytes: &[u8]) -> Result<usize, String> {
    if bytes.get(position) != Some(&b'i') {
        return Err(format!("se esperaba un entero en la posicion {position}"));
    }

    let mut cursor = position + 1;

    if cursor >= bytes.len() {
        return Err("el entero esta incompleto".to_string());
    }

    // Bencode permite enteros negativos.
    if bytes[cursor] == b'-' {
        cursor += 1;
    }

    let digits_start = cursor;

    while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
        cursor += 1;
    }

    if cursor == digits_start {
        return Err(format!("el entero de la posicion {position} no contiene digitos"));
    }

    if cursor >= bytes.len() || bytes[cursor] != b'e' {
        return Err(format!("el entero de la posicion {position} no termina con 'e'"));
    }

    // Regresamos la posición posterior a la 'e'.
    Ok(cursor + 1)
}

fn skip_bencode_list(position: usize, bytes: &[u8]) -> Result<usize, String> {
    if bytes.get(position) != Some(&b'l') {
        return Err(format!("se esperaba una lista en la posicion {position}"));
    }

    let mut cursor = position + 1;

    loop {
        if cursor >= bytes.len() {
            return Err("la lista no tiene una 'e' final".to_string());
        }

        if bytes[cursor] == b'e' {
            return Ok(cursor + 1);
        }

        cursor = skip_bencode_field(cursor, bytes)?;
    }
}

fn skip_bencode_dictionary(position: usize, bytes: &[u8]) -> Result<usize, String> {
    if bytes.get(position) != Some(&b'd') {
        return Err(format!("se esperaba un diccionario en la posicion {position}"));
    }

    let mut cursor = position + 1;

    loop {
        if cursor >= bytes.len() {
            return Err("el diccionario no tiene una 'e' final".to_string());
        }

        if bytes[cursor] == b'e' {
            return Ok(cursor + 1);
        }

        // Cada clave de un diccionario debe ser una cadena.
        let (_, position_after_key) = read_bencode_byte_string(bytes, cursor)?;

        // Después de la clave viene su valor.
        cursor = skip_bencode_field(position_after_key, bytes)?;
    }
}
