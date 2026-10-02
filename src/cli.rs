use std::io;
use std::path::{Path, PathBuf};

pub(crate) fn read_torrent_path() -> String {
    println!("Enter the torrent file path:");
    loop {
        let mut input = String::new();
        io::stdin().read_line(&mut input).expect("Failed to read input");

        if Path::new(input.trim()).is_file() {
            log::debug!("Input validated");
            return input.trim().to_string();
        }
        println!("File not found. Enter an existing torrent file path:");
    }
}

pub(crate) fn read_download_path() -> PathBuf {
    println!("Enter the download directory:");
    loop {
        let mut input = String::new();
        io::stdin().read_line(&mut input).expect("Failed to read input");

        let path = PathBuf::from(input.trim());
        if path.is_dir() {
            log::debug!("Download directory validated");
            return path;
        }
        println!("Directory not found. Enter an existing download directory:");
    }
}
