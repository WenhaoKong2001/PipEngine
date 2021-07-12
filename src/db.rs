use crate::disk_service::DiskService;
use crate::mem_table::{MemTable, MemTableEntry};
use std::io::{Error, ErrorKind};
use std::path::PathBuf;
use std::fs;
use std::io;
use std::fs::OpenOptions;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::wal::WAL;


struct db {
    dir_db: PathBuf,
    disk_service: DiskService,
    mem_table: MemTable,
    wal: WAL,
}

impl db {
    pub fn new() -> io::Result<db> {
        let dir_db = PathBuf::from(format!("./{}", "DB"));
        let dir_file = dir_db.join("FILE");
        let dir_wal = dir_db.join("WAL");

        fs::create_dir(&dir_db)?;
        fs::create_dir(&dir_wal)?;

        let disk_service = DiskService::new(&dir_file)?;
        let mem_table = MemTable::new();
        let wal = WAL::new(&dir_wal).unwrap();
        Ok(db {
            dir_db,
            disk_service,
            mem_table,
            wal,
        })
    }

    pub fn put(&mut self, key: &str, value: &str) {
        let timestamp = self.get_timestamp();
        self.wal.put(key.as_bytes(), value.as_bytes(), timestamp);
        self.mem_table.put(key.as_bytes(), value.as_bytes(), timestamp);
        //todo if mem_table.size > max_size
        if self.mem_table.is_over_weight() {
            //write mem_table to disk.
            //fresh wal.
        }
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

    pub fn delete(&mut self, key: &str) {
        let timestamp = self.get_timestamp();
        self.wal.delete(key.as_bytes(), timestamp).unwrap();
        self.mem_table.delete(key.as_bytes(), timestamp);
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

    pub fn close(self) {}

    pub fn open(path: &PathBuf) -> io::Result<db> {
        let dir_db = PathBuf::from(path);
        let dir_file = dir_db.join("FILE");
        let dir_wal = dir_db.join("WAL");

        if !dir_file.exists() || !dir_wal.exists() {
            return Err(io::Error::new(ErrorKind::NotFound, "Not Found"));
        }

        let disk_service = DiskService::open(&dir_file)?;
        let (wal, mem_table) = WAL::recover(&dir_wal)?;
        Ok(db {
            dir_db,
            disk_service,
            mem_table,
            wal,
        })
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

#[cfg(test)]
mod tests {
    use crate::db::db;
    use std::path::PathBuf;
    use std::fs;

    #[test]
    fn test_put_get_range() {
        let mut handler_db = db::new().unwrap();
        handler_db.put("a", "value-a");
        handler_db.put("b", "value-b");
        handler_db.put("a", "value-a2");
        handler_db.put("c", "value-c");
        handler_db.put("d", "value-d");
        let a = handler_db.get("a").unwrap();
        let b = handler_db.get("b").unwrap();
        let c = handler_db.get("c").unwrap();

        assert_eq!(a, "value-a2".to_string());
        assert_eq!(b, "value-b".to_string());
        assert_eq!(c, "value-c".to_string());

        let test_data_range =
            vec!["value-a2".to_string(), "value-b".to_string(), "value-c".to_string()];
        let vec_range = handler_db.range("a", "c");

        let mut test_iter = test_data_range.into_iter();
        for val in vec_range {
            assert_eq!(val, test_iter.next().unwrap())
            //println!("{}",val);
        }
        fs::remove_dir_all(handler_db.dir_db);
    }

    #[test]
    fn test_close_and_open() {
        let mut handler_db = db::new().unwrap();
        handler_db.put("a", "value-a");
        handler_db.put("b", "value-b");
        handler_db.put("a", "value-a2");
        handler_db.put("c", "value-c");
        handler_db.put("d", "value-d");
        let a = handler_db.get("a").unwrap();
        let b = handler_db.get("b").unwrap();
        let c = handler_db.get("c").unwrap();
        handler_db.close();
        let path = PathBuf::from(format!("./{}", "DB"));
        let new_handler_db = db::open(&path).unwrap();

        let a = new_handler_db.get("a").unwrap();
        let b = new_handler_db.get("b").unwrap();
        let c = new_handler_db.get("c").unwrap();

        assert_eq!(a, "value-a2".to_string());
        assert_eq!(b, "value-b".to_string());
        assert_eq!(c, "value-c".to_string());
        fs::remove_dir_all(new_handler_db.dir_db);
    }
}