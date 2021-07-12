use std::path::PathBuf;
use crate::mem_table::{MemTableEntry, MemTable};
use std::io;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read};

const MAX_KEY_SIZE: usize = 128;

pub struct DiskService {
    dir: PathBuf,
    files: Vec<FileService>,
}

impl DiskService {
    //todo creat level dir.
    pub fn new(dir: &PathBuf) -> io::Result<DiskService> {
        Ok(DiskService {
            dir: dir.to_owned(),
            files: vec![],
        })
    }

    pub fn open(dir: &PathBuf) -> io::Result<DiskService> {
        let mut files = vec![];
        let entries = fs::read_dir(dir).unwrap();
        for entry in entries {
            let path = entry.unwrap().path();
            if path.extension().unwrap() == "dbf" {
                let file = OpenOptions::new().read(true).write(true).open(&path).unwrap();
                let mut reader = BufReader::new(file);
                let mut max_key = [0; MAX_KEY_SIZE];
                let mut min_key = [0; MAX_KEY_SIZE];
                reader.read_exact(&mut min_key).unwrap();
                reader.read_exact(&mut max_key).unwrap();

                files.push(FileService {
                    min_key,
                    max_key,
                    file_path: path,
                    reader,
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
    //key_size--tombstone--value_size--key--value--timestamp
    pub fn write_mem_table_to_disk(&self, mem_table: &MemTable) {

    }
}

struct FileService {
    min_key: [u8; MAX_KEY_SIZE],
    max_key: [u8; MAX_KEY_SIZE],
    file_path: PathBuf,
    reader: BufReader<File>,
}

impl FileService {
    //fn new() -> FileService {}
}