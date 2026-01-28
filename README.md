# lsm-go implementation 

minimal Log-Structured Merge-tree (LSM) key/value store written in Go.

This repo intentionally focuses on a small, readable subset:

- Memtable (in-memory)
- WAL + recovery
- SSTable flush (sorted on-disk runs)
- Compaction (merge SSTables)
- Bloom Filters
