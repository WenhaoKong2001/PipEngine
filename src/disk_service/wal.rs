use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::io::{self, BufReader, Read};

pub struct WALEntry {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    timestamp: u128,
    deleted: bool,
}

pub struct WALIterator {
    reader: BufReader<File>,
}

impl WALIterator {
    pub fn new(path: PathBuf) -> io::Result<WALIterator> {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        Ok(WALIterator {
            reader
        })
    }
}

impl Iterator for WALIterator {
    type Item = WALEntry;

    fn next(&mut self) -> Option<WALEntry> {
        let mut key_len_buf = [0; 8];
        if self.reader.read_exact(&mut key_len_buf).is_err() {
            return None;
        }
        let key_len = usize::from_le_bytes(key_len_buf);

        let mut tombstone = [0; 1];
        if self.reader.read_exact(&mut tombstone).is_err() {
            return None;
        }
        let deleted = tombstone[0] != 0;
        let mut key = vec![0; key_len];
        let mut value = None;
        if deleted {
            //let mut key = vec![0; key_len];
            if self.reader.read_exact(&mut key).is_err() {
                return None;
            }
        } else {
            let mut value_size_buf = [0; 8];
            if self.reader.read_exact(&mut value_size_buf).is_err() {
                return None;
            }
            let value_len = usize::from_le_bytes(value_size_buf);

            //let mut key = vec![0; key_len];
            if self.reader.read_exact(&mut key).is_err() {
                return None;
            }

            let mut value_buf = vec![0; value_len];
            if self.reader.read_exact(&mut value_buf).is_err() {
                return None;
            }
            value = Some(value_buf);
        }
        let mut timestamp_buf = [0; 16];
        if self.reader.read_exact(&mut timestamp_buf).is_err() {
            return None;
        }
        let timestamp = u128::from_le_bytes(timestamp_buf);
        Some(WALEntry {
            key,
            value,
            timestamp,
            deleted,
        })
    }
}