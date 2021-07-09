use crate::disk_service;
use crate::mem_table;
use std::path::PathBuf;
use std::io;

pub struct db{
    path:PathBuf,
    disk:disk_service::disk_service,
    mem_table:mem_table::MemTable,
}

/*
    pub fn new(path:&str) -> db;
    pub fn put(&mut self, key: &str, value: &str) -> bool;
    pub fn get(&self, key: &str) -> Option<String>;
    pub fn range(&self, start_key: &str, end_key: &str) -> Vec<String>;
    pub fn delete(&mut self, key: &str);
    pub fn close(&mut self);
    pub fn open(&mut self, path: &str);
 */

