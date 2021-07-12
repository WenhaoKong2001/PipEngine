use std::fs::File;
use std::path::PathBuf;
use crate::mem_table::MemTableEntry;

mod levels;

pub struct disk_service{
    dir:PathBuf
}

impl disk_service{
    //todo creat level dir.
    pub fn new(dir:&PathBuf)->disk_service{
        disk_service{
            dir:dir.to_owned()
        }
    }

    pub fn range(&self, min_key: &[u8], max_key: &[u8]) -> Vec<MemTableEntry>{
        vec![]
    }

    pub fn get(&self, key: &[u8]) -> Option<&MemTableEntry>{
        None
    }
}