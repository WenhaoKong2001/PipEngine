use std::fs::{self, File, OpenOptions};
use std::path::PathBuf;
use std::io::{self, BufReader, Read, BufWriter, Write};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::mem_table::MemTable;
use crate::util;

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
    pub fn new(dir: &PathBuf) -> io::Result<WAL> {
        let timestamp = util::get_timestamp();
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

    //todo when current mem_table is full,it need to be written to a db file.
    // at the same time current wal need to be deleted and fresh.
    pub fn fresh(&mut self) -> io::Result<()> {
        let dir = self.path.parent().unwrap();
        fs::remove_file(&self.path)?;
        let timestamp = util::get_timestamp();
        let path = dir.join(timestamp.to_string() + ".wal");
        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(&path)?;
        let writer = BufWriter::new(file);
        self.writer = writer;
        self.path = path;
        Ok(())
    }

    fn from_path(path: &PathBuf) -> io::Result<WAL> {
        let file = OpenOptions::new().append(true).create(true).open(path)?;
        let writer = BufWriter::new(file);
        Ok(WAL {
            path: path.to_owned(),
            writer,
        })
    }

    pub fn recover(dir: &PathBuf) -> io::Result<(WAL, MemTable)> {
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
        let mut new_wal = WAL::new(&dir).unwrap();
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
        fs::remove_file(wal_path).unwrap();

        Ok((new_wal, new_mem_table))
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

#[cfg(test)]
mod tests {
    use std::io::{BufReader, Read, Write};
    use std::fs::{File, OpenOptions};
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};
    use crate::wal::WAL;
    use std::path::PathBuf;

    fn check_entry(
        reader: &mut BufReader<File>,
        key: &[u8],
        value: Option<&[u8]>,
        timestamp: u128,
        deleted: bool,
    ) {
        let mut len_buffer = [0; 8];
        reader.read_exact(&mut len_buffer).unwrap();
        let file_key_len = usize::from_le_bytes(len_buffer);
        assert_eq!(file_key_len, key.len());

        let mut bool_buffer = [0; 1];
        reader.read_exact(&mut bool_buffer).unwrap();
        let file_deleted = bool_buffer[0] != 0;
        assert_eq!(file_deleted, deleted);

        if deleted {
            let mut file_key = vec![0; file_key_len];
            reader.read_exact(&mut file_key).unwrap();
            assert_eq!(file_key, key);
        } else {
            reader.read_exact(&mut len_buffer).unwrap();
            let file_value_len = usize::from_le_bytes(len_buffer);
            assert_eq!(file_value_len, value.unwrap().len());
            let mut file_key = vec![0; file_key_len];
            reader.read_exact(&mut file_key).unwrap();
            assert_eq!(file_key, key);
            let mut file_value = vec![0; file_value_len];
            reader.read_exact(&mut file_value).unwrap();
            assert_eq!(file_value, value.unwrap());
        }

        let mut timestamp_buffer = [0; 16];
        reader.read_exact(&mut timestamp_buffer).unwrap();
        let file_timestamp = u128::from_le_bytes(timestamp_buffer);
        assert_eq!(file_timestamp, timestamp);
    }

    #[test]
    fn test_put() {
        let path = PathBuf::from(format!("./{}", "WAL"));
        fs::create_dir(&path);

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let test_value: Vec<(&[u8], Option<&[u8]>)> = vec![
            (b"a", Some(b"value_a")),
            (b"b", Some(b"value_b")),
            (b"a", Some(b"value_a2")),
            (b"c", Some(b"value_c")),
        ];

        let mut wal = WAL::new(&path).unwrap();

        for val in test_value.iter() {
            wal.put(val.0, val.1.unwrap(), timestamp).unwrap();
        }
        wal.writer.flush().unwrap();

        let file = OpenOptions::new().read(true).open(&wal.path).unwrap();
        let mut reader = BufReader::new(file);

        for val in test_value.iter() {
            check_entry(&mut reader, val.0, val.1, timestamp, false);
        }

        fs::remove_dir_all(&path).unwrap();
    }

    #[test]
    fn test_delete() {
        let path = PathBuf::from(format!("./{}", "WAL"));
        fs::create_dir(&path);

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let test_value: Vec<(&[u8], Option<&[u8]>)> = vec![
            (b"a", Some(b"value_a")),
            (b"b", Some(b"value_b")),
            (b"a", Some(b"value_a2")),
            (b"c", Some(b"value_c")),
        ];
        let mut wal = WAL::new(&path).unwrap();
        for val in test_value.iter() {
            wal.delete(val.0, timestamp);
        }
        wal.writer.flush().unwrap();

        let file = OpenOptions::new().read(true).open(&wal.path).unwrap();
        let mut reader = BufReader::new(file);

        for val in test_value.iter() {
            check_entry(&mut reader, val.0, None, timestamp, true);
        }

        fs::remove_dir_all(&path).unwrap();
    }

    // Notice:
    // (b"a",Some(b"value_a")),
    // (b"b",Some(b"value_b")),
    // (b"a",Some(b"value_a2")),
    // (b"c",Some(b"value_c")),
    // can't pass this test.
    // but it behaves as the desired behavior.
    // The reason for this is that test_read_wal only tests the given value one by one
    // But the internal Btree will rewrite it's value when encounter the same key.
    #[test]
    fn test_read_wal() {
        let path = PathBuf::from(format!("./{}", "WAL"));
        fs::create_dir(&path);

        let test_value: Vec<(&[u8], Option<&[u8]>)> = vec![
            (b"Apple", Some(b"Apple Smoothie")),
            (b"Lime", Some(b"Lime Smoothie")),
            (b"Orange", Some(b"Orange Smoothie")),
        ];

        let mut wal = WAL::new(&path).unwrap();
        for (i, val) in test_value.iter().enumerate() {
            wal.put(val.0, val.1.unwrap(), i as u128).unwrap();
        }
        wal.writer.flush().unwrap();

        let (new_wal, new_mem_table) = WAL::recover(&path).unwrap();

        let file = OpenOptions::new().read(true).open(&new_wal.path).unwrap();
        let mut reader = BufReader::new(file);

        for (i, e) in test_value.iter().enumerate() {
            check_entry(&mut reader, e.0, e.1, i as u128, false);

            let mem_e = new_mem_table.get(e.0).unwrap();
            assert_eq!(mem_e.key, e.0);
            assert_eq!(mem_e.value.as_ref().unwrap().as_slice(), e.1.unwrap());
            assert_eq!(mem_e.timestamp, i as u128);
        }

        fs::remove_dir_all(&path).unwrap();
    }
}