# PipEngine
PipEngine is a simple db engine with LSM-Tree structure.I develop this project for gaining experience
of the Rust Programming Language.

## API design
```rust
    pub fn new(path:&str) -> db;
    pub fn put(&mut self, key: &str, value: &str) -> bool;
    pub fn get(&self, key: &str) -> Option<String>;
    pub fn range(&self, start_key: &str, end_key: &str) -> Vec<String>;
    pub fn delete(&mut self, key: &str);
    pub fn close(&mut self);
    pub fn open(&mut self, path: &str);
```

## Sub-unit design
**⚠ NOTES:**
This is just a draft now.May be changed during  development.


## Reference
- [database-engine](https://github.com/adambcomer/database-engine)
- 数据库系统内幕   ISBN 978-7-111-65516-9
