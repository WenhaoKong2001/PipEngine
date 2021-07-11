use std::fs::{self, File, OpenOptions};
use std::path::PathBuf;
use std::io::{self, BufReader, Read, BufWriter, Write};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::mem_table::MemTable;

pub struct WALEntry {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    timestamp: u128,
    deleted: bool,
}

pub struct WAL {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl WAL {
    fn new(dir: PathBuf) -> io::Result<WAL> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();
        let path = dir.join(timestamp.to_string() + ".wal");
        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(&path)?;
        let writer = BufWriter::new(file);
        Ok(WAL {
            path,
            writer,
        })
    }

    fn from_path(path: &PathBuf) -> io::Result<WAL> {
        let file = OpenOptions::new().append(true).create(true).open(path)?;
        let writer = BufWriter::new(file);
        Ok(WAL {
            path: path.to_owned(),
            writer,
        })
    }

    fn recover(dir: PathBuf) -> io::Result<(WAL, MemTable)> {
        let mut wal_path = PathBuf::new();
        let dir_entry = fs::read_dir(&dir)?;
        for entry in dir_entry {
            let path = entry.unwrap().path();
            if path.extension().unwrap() == "wal" {
                wal_path = path;
                break;
            }
        }

        let mut new_mem_table = MemTable::new();
        let mut new_wal = WAL::new(dir).unwrap();
        if let Ok(wal) = WAL::from_path(&wal_path) {
            for wal_entry in wal.into_iter() {
                if wal_entry.deleted {
                    new_mem_table.delete(wal_entry.key.as_slice(), wal_entry.timestamp);
                    new_wal.delete(wal_entry.key.as_slice(), wal_entry.timestamp);
                } else {
                    new_mem_table.put(wal_entry.key.as_slice(), wal_entry.value.as_ref().unwrap()
                        .as_slice(), wal_entry.timestamp);
                    new_wal.put(wal_entry.key.as_slice(), wal_entry.value.as_ref().unwrap()
                        .as_slice(), wal_entry.timestamp);
                }
            }
        }
        new_wal.writer.flush().unwrap();
        fs::remove_dir(wal_path).unwrap();

        Ok((new_wal,new_mem_table))
    }

    pub fn put(&mut self, key: &[u8], value: &[u8], timestamp: u128) -> io::Result<()> {
        self.writer.write_all(&key.len().to_le_bytes())?;
        self.writer.write_all(&(false as u8).to_le_bytes())?;
        self.writer.write_all(&value.len().to_le_bytes())?;
        self.writer.write_all(key)?;
        self.writer.write_all(value)?;
        self.writer.write_all(&timestamp.to_le_bytes())?;
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8], timestamp: u128) -> io::Result<()> {
        self.writer.write_all(&key.len().to_le_bytes())?;
        self.writer.write_all(&(true as u8).to_le_bytes())?;
        self.writer.write_all(key)?;
        self.writer.write_all(&timestamp.to_le_bytes())?;
        Ok(())
    }
}

impl IntoIterator for WAL {
    type Item = WALEntry;
    type IntoIter = WALIterator;

    fn into_iter(self) -> WALIterator {
        WALIterator::new(self.path).unwrap()
    }
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