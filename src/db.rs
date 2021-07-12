use crate::disk_service::disk_service;
use crate::mem_table::{MemTable, MemTableEntry};

use std::path::PathBuf;
use std::fs;
use std::io;
use std::fs::OpenOptions;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::wal::WAL;

pub struct db {
    disk_service: disk_service,
    mem_table: MemTable,
    wal: WAL,
}

impl db {
    pub fn new() -> io::Result<db> {
        let dir_db = PathBuf::from(format!("./{}", "DB"));
        let dir_wal = PathBuf::from(format!("./{}", "WAL"));
        fs::create_dir(&dir_db)?;
        fs::create_dir(&dir_wal)?;

        let disk_service = disk_service::new(&dir_db);
        let mem_table = MemTable::new();
        let wal = WAL::new(&dir_wal).unwrap();
        Ok(db {
            disk_service,
            mem_table,
            wal,
        })
    }

    pub fn put(&mut self, key: &str, value: &str) -> bool {
        let timestamp = self.get_timestamp();
        self.wal.put(key.as_bytes(), value.as_bytes(), timestamp);
        self.mem_table.put(key.as_bytes(), value.as_bytes(), timestamp);
        //todo if mem_table.size > max_size
        true
    }

    pub fn get(&self, key: &str) -> Option<String> {
        //in mem_table
        return if let Some(entry) = self.mem_table.get(key.as_bytes()) {
            self.get_value_from_mem_entry(entry)
        } else {
            //TODO if not in mem_table,search in disk.
            if let Some(entry) = self.disk_service.get(key.as_bytes()) {
                self.get_value_from_mem_entry(entry)
            } else {
                None
            }
        };
    }

    pub fn range(&self, min_key: &str, max_key: &str) -> Vec<String> {
        let mut vec_range = Vec::new();

        let mem_entries = self.mem_table
            .range(min_key.as_bytes(), max_key.as_bytes());
        self.vec_range_push(&mem_entries, &mut vec_range);
        //todo search in disk file.
        let disk_entries = self.disk_service.
            range(min_key.as_bytes(), max_key.as_bytes());
        self.vec_range_push(&disk_entries, &mut vec_range);
        vec_range
    }

    pub fn delete(&mut self, key: &str) {
        let timestamp = self.get_timestamp();
        self.wal.delete(key.as_bytes(),timestamp).unwrap();
        self.mem_table.delete(key.as_bytes(),timestamp);
    }

    pub fn close(&mut self){

    }

    pub fn open(&mut self, path: &str){

    }

    fn get_value_from_mem_entry(&self, entry: &MemTableEntry) -> Option<String> {
        if let Some(value) = &entry.value {
            let value = value.to_owned();
            Some(String::from_utf8(value).unwrap())
        } else {
            None
        }
    }

    fn vec_range_push(&self, entries: &Vec<MemTableEntry>
                      , vec_range: &mut Vec<String>) {
        for entry in entries.iter() {
            if let Some(value) = &entry.value {
                vec_range.push(String::from_utf8(value.clone()).unwrap());
            }
        }
    }

    fn get_timestamp(&self) -> u128 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros()
    }
}

