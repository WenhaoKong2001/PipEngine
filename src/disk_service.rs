use std::path::PathBuf;
use crate::mem_table::{MemTableEntry, MemTable};
use crate::util;
use std::io;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, BufWriter, Write, Seek, SeekFrom};
use std::time::{SystemTime, UNIX_EPOCH};
use std::convert::TryFrom;

const MAX_KEY_SIZE: usize = 128;

pub struct DiskService {
    dir: PathBuf,
    files: Vec<FileService>,
}
//TODO range get compression
impl DiskService {

    pub fn new(dir: &PathBuf) -> io::Result<DiskService> {
        fs::create_dir(dir);
        Ok(DiskService {
            dir: dir.to_owned(),
            files: vec![],
        })
    }

    // dir:DISK_FILE
    pub fn open(dir: &PathBuf) -> io::Result<DiskService> {
        let mut files = vec![];
        let entries = fs::read_dir(dir).unwrap();
        for entry in entries {
            let path = entry.unwrap().path();
            if path.extension().unwrap() == "dbf" {
                let file = OpenOptions::new().read(true).write(true).open(&path).unwrap();
                let mut reader = BufReader::new(file);
                //let mut writer = BufWriter::new(&file);
                let mut key_size = [0; 8];
                reader.read_exact(&mut key_size).unwrap();
                let min_key_size = usize::from_le_bytes(key_size);
                reader.read_exact(&mut key_size).unwrap();
                let max_key_size = usize::from_le_bytes(key_size);
                let mut min_key = vec![0; min_key_size];
                let mut max_key = vec![0; max_key_size];
                reader.read_exact(&mut min_key).unwrap();
                reader.read_exact(&mut max_key).unwrap();
                files.push(FileService {
                    min_size: min_key_size,
                    max_size: max_key_size,
                    min_key,
                    max_key,
                    file_path: path,
                });
            }
        }

        Ok(DiskService {
            dir: dir.to_owned(),
            files,
        })
    }

    pub fn range(&self, min_key: &[u8], max_key: &[u8]) -> Vec<MemTableEntry> {
        vec![]
    }

    pub fn get(&self, key: &[u8]) -> Option<&MemTableEntry> {
        None
    }
    //min_key_size--max_key-size--min_key--max_key
    //key_size--tombstone--value_size--key--value--timestamp
    pub fn write_mem_table_to_disk(&mut self, mem_table: &MemTable) -> io::Result<()> {
        let timestamp = util::get_timestamp();
        let path = self.dir.join(timestamp.to_string() + ".dbf");
        let mut new_db_file = OpenOptions::new().write(true).create(true).open(&path).unwrap();

        let mut iter = mem_table.iter();
        let (min, _) = iter.next().unwrap();
        let (max, _) = iter.last().unwrap();
        let min_size = min.len();
        let max_size = max.len();
        new_db_file.write_all(&min_size.to_le_bytes());
        new_db_file.write_all(&max_size.to_le_bytes());
        new_db_file.write_all(&min.as_slice())?;
        new_db_file.write_all(&max.as_slice())?;
        let mut iter = mem_table.iter();
        //todo fill min_key and max_key.
        for (key, entry) in iter {
            //if iter.last()
            let key = entry.key.to_owned();
            let deleted = entry.deleted;
            let value = entry.value.to_owned().unwrap();
            let timestamp = entry.timestamp;
            new_db_file.write_all(&key.len().to_le_bytes())?;
            new_db_file.write_all(&(deleted as u8).to_le_bytes())?;
            if deleted {
                new_db_file.write_all(key.as_slice())?;
                new_db_file.write_all(&timestamp.to_le_bytes())?;
            } else {
                new_db_file.write_all((&value.len().to_le_bytes()))?;
                new_db_file.write_all(key.as_slice())?;
                new_db_file.write_all(&value)?;
                new_db_file.write_all(&timestamp.to_le_bytes())?;
            }
        }
        let mut reader = BufReader::new(new_db_file);
        reader.seek(SeekFrom::Start((8 + 8 + min_size + max_size) as u64));
        self.files.push(FileService {
            min_size,
            max_size,
            min_key: min.to_owned(),
            max_key: max.to_owned(),
            file_path: path,
        });
        Ok(())
    }
}

struct FileService {
    min_size: usize,
    max_size: usize,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    file_path: PathBuf,
}

impl FileService {
    fn rebuild_mem_table_from_disk(&mut self) -> MemTable {
        let mut new_mem_table = MemTable::new();
        for entry in self.iter() {
            if entry.deleted {
                new_mem_table.delete(entry.key.as_slice(), entry.timestamp);
            } else {
                new_mem_table.put(entry.key.as_slice()
                                  , entry.value.as_ref().unwrap()
                                  , entry.timestamp);
            }
        }
        new_mem_table
    }

    fn iter(&self) -> DBFIterator {
        DBFIterator::new(self.file_path.to_owned(),
                         (8 + 8 + self.min_size + self.max_size)).unwrap()
    }
}

impl IntoIterator for FileService {
    type Item = MemTableEntry;
    type IntoIter = DBFIterator;

    fn into_iter(self) -> DBFIterator {
        DBFIterator::new(self.file_path, (8 + 8 + self.min_size + self.max_size)).unwrap()
    }
}


pub struct DBFIterator {
    reader: BufReader<File>,
}

impl DBFIterator {
    pub fn new(path: PathBuf, pos: usize) -> io::Result<DBFIterator> {
        let file = OpenOptions::new().read(true).open(path)?;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(pos as u64));
        Ok(DBFIterator {
            reader
        })
    }
}

impl Iterator for DBFIterator {
    type Item = MemTableEntry;

    fn next(&mut self) -> Option<MemTableEntry> {
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
        Some(MemTableEntry {
            key,
            value,
            timestamp,
            deleted,
        })
    }
}
