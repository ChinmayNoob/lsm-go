# lsm-go implementation

minimal Log-Structured Merge-tree (LSM) key/value store written in Go.

**Run:**
```bash
go run ./cmd put -dir demo -mem 1 -maxsst 1 -verbose a 1
go run ./cmd put -dir demo -mem 1 -maxsst 1 -verbose b 2
go run ./cmd put -dir demo -mem 1 -maxsst 1 -verbose c 3

# Get a key that exists - you'll see which SSTables are checked
go run ./cmd get -dir demo -verbose a

# Get a key that doesn't exist - you'll see Bloom filters skip SSTables
go run ./cmd get -dir demo -verbose nonexistent
```

**Testing the implementation** — run these to verify each LSM piece:

| What you're testing | Commands |
|---------------------|----------|
| **Memtable** (in-memory) | `-mem 0` = no flush. `go run ./cmd put -dir mtest -mem 0 k v` then `go run ./cmd get -dir mtest k` → `v`. Data only in memtable. |
| **WAL + recovery** | Use a fresh dir. **Run 1:** `go run ./cmd put -dir recover x 99` then exit. **Run 2:** `go run ./cmd get -dir recover x` → `99`. Recovery replays WAL into memtable. |
| **SSTable flush** | `-mem 1` forces flush every put. `go run ./cmd put -dir flush -mem 1 -verbose a 1` — you'll see `[flush] flushing memtable...` and new `.sst` in `flush/sstables/`. |
| **Compaction** | `-maxsst 1` = compact when >1 SSTable. `go run ./cmd put -dir compact -mem 1 -maxsst 1 -verbose a 1` then `b 2` — second put triggers `[compact] merging...` and a single merged SSTable. |
| **Bloom filters** | Get existing key: `go run ./cmd get -dir demo -verbose a` (SSTables checked). Get missing key: `go run ./cmd get -dir demo -verbose nonexistent` (skipped via Bloom). |
| **Updates** | `go run ./cmd put -dir upd u 1` then `go run ./cmd put -dir upd u 2`. `go run ./cmd get -dir upd u` → `2`. Latest write wins. |
| **Deletes (tombstones)** | `go run ./cmd put -dir del d 1` then `go run ./cmd del -dir del d`. `go run ./cmd get -dir del d` → `(not found)` and exit 1. |
| **Delete + recovery** | **Run 1:** `go run ./cmd put -dir delrec y 1` then `go run ./cmd del -dir delrec y` then exit. **Run 2:** `go run ./cmd get -dir delrec y` → `(not found)`. Tombstones replayed from WAL. |

Use **`-verbose`** with `get` to see memtable vs SSTable lookups and Bloom filter skip / hit messages.

This repo intentionally focuses on a small, readable subset:

- Memtable (in-memory)
- WAL + recovery
- SSTable flush (sorted on-disk runs)
- Compaction (merge SSTables)
- Bloom Filters
