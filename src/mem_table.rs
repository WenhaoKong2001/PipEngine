use std::collections::{BTreeMap};
use std::collections::btree_map::{Range, Iter};
use std::ops::Bound::Included;

const MAX_MEM_TABLE_SIZE: usize = 4096;

/// +--------------+------------------------+-----------------+---------------+
/// | key: Vec<u8> | value: Option<Vec<u8>> | timestamp: u128 | deleted: bool |
/// +--------------+------------------------+-----------------+---------------+
#[derive(Clone)]
pub struct MemTableEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: u128,
    pub deleted: bool,
}


/// +-----------------------------+-------------+
/// | BTreeMap<key,MemTableEntry> | size: usize |
/// +-----------------------------+-------------+
pub struct MemTable {
    btree: BTreeMap<Vec<u8>, MemTableEntry>,
    size: usize,
}

impl MemTable {
    pub fn new() -> MemTable {
        MemTable {
            btree: BTreeMap::new(),
            size: 0,
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8], timestamp: u128) {
        let entry = MemTableEntry {
            key: key.to_owned(),
            value: Some(value.to_owned()),
            timestamp,
            deleted: false,
        };

        if self.btree.contains_key(key) {
            let old_value = self.btree.get_mut(key).unwrap();
            if let Some(v) = old_value.value.as_ref() {
                let old_size = v.len();
                let new_size = value.len();
                if old_size < new_size {
                    let gap = new_size - old_size;
                    self.size += gap;
                } else if old_size > new_size {
                    let gap = old_size - new_size;
                    self.size -= gap;
                }
            }
            *old_value = entry;
        } else {
            self.size += key.len() + value.len() + 16 + 1;
            self.btree.insert(key.to_vec(), entry);
            // if let Some(v) = self.btree.get(&*key.to_vec()){
            //     println!("{}",v.timestamp);
            // }else{
            //     println!("fuck");
            // }
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&MemTableEntry> {
        self.btree.get(key)
    }

    pub fn range(&self, min_key: &[u8], max_key: &[u8]) -> Vec<MemTableEntry> {
        let range = self.btree.range((Included(min_key.to_owned()), Included(max_key.to_owned())));
        let mut range_vec: Vec<MemTableEntry> = Vec::new();
        for (key, entry) in range {
            range_vec.push(entry.clone());
        }
        range_vec
    }

    pub fn delete(&mut self, key: &[u8], timestamp: u128) {
        let entry = MemTableEntry {
            key: key.to_owned(),
            value: None,
            timestamp,
            deleted: true,
        };

        if self.btree.contains_key(key) {
            let old_value = self.btree.get_mut(key).unwrap();
            if let Some(value) = old_value.value.as_ref() {
                self.size -= value.len();
            }
            *old_value = entry;
        } else {
            self.size += key.len() + 16 + 1;
            self.btree.insert(key.to_vec(), entry);
        }
    }

    pub fn is_over_weight(&self) -> bool {
        self.size >= 128
    }

    pub fn clear(&mut self) {
        self.btree.clear();
        self.size = 0;
    }

    pub fn is_empty(& self)->bool{
        self.btree.is_empty()
    }

    pub fn iter(&self) -> Iter<'_, Vec<u8>, MemTableEntry> {
        self.btree.iter()
    }
}


#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    //use super::key;
    use crate::mem_table::MemTable;

    #[test]
    fn test_put_and_get() {
        let mut table = MemTable::new();
        table.put(b"a", b"valueA", 1);
        table.put(b"a", b"valueA-2", 2);
        table.put(b"b", b"valueB", 3);
        table.put(b"c", b"valueC", 4);
        table.put(b"c", b"valueC-2", 5);

        let value_a = table.get(b"a").unwrap();
        let value_b = table.get(b"b").unwrap();
        let value_c = table.get(b"c").unwrap();

        assert_eq!(value_a.value.as_ref().unwrap(), b"valueA-2");
        assert_eq!(value_b.value.as_ref().unwrap(), b"valueB");
        assert_eq!(value_c.value.as_ref().unwrap(), b"valueC-2");
        let a = table.range(b"a", b"c");
        for aa in &a {
            println!("{}", aa.timestamp);
        }
    }
}



