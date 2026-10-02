use crate::torrent::parse_magnet;
use std::io;
use std::path::{Path, PathBuf};

pub(crate) fn read_input_choice() -> u8 {
    println!("Select input: 0 = torrent file, 1 = magnet link.");
    loop {
        let mut input = String::new();
        io::stdin().read_line(&mut input).expect("Failed to read input");
        let input_text = input.trim();
        let parsed_choice = input_text.parse::<u8>();
        match parsed_choice {
            Ok(number) => {
                let choice_is_valid = number == 0 || number == 1;
                if !choice_is_valid {
                    println!("Invalid choice. Enter 0 or 1.");
                    continue;
                }

                return number;
            }
            Err(_) => println!("Invalid choice. Enter 0 or 1."),
        }
    }
}

pub(crate) fn read_magnet_url() -> String {
    println!("Enter the magnet link:");
    loop {
        let mut input = String::new();
        io::stdin().read_line(&mut input).expect("Failed to read input");

        match parse_magnet(&input) {
            Ok(_) => {
                return input.trim().to_string();
            }
            Err(_) => println!("Invalid magnet link. Try again."),
        }
    }
}

pub(crate) fn read_torrent_path() -> String {
    println!("Enter the torrent file path:");
    loop {
        let mut input = String::new();
        io::stdin().read_line(&mut input).expect("Failed to read input");

        if Path::new(input.trim()).is_file() {
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
            return path;
        }
        println!("Directory not found. Enter an existing download directory:");
    }
}
