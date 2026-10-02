use env_logger::{Builder, Env};
use std::io::Write;

pub(crate) fn initialize_logging() {
    let environment = Env::default().default_filter_or("warn,bit_torrent_rust_client=info");
    let mut logger = Builder::from_env(environment);
    logger.format(|buffer, record| {
        let original_message = record.args().to_string();
        let mut message = String::new();
        for character in original_message.chars().take(300) {
            if character.is_control() {
                message.push(' ');
            } else {
                message.push(character);
            }
        }
        writeln!(buffer, "[{} {} {}] {}", buffer.timestamp_millis(), record.level(), record.target(), message)
    });
    logger.init();
}
