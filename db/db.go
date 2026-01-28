package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/ChinmayNoob/lsm-go/compaction"
	"github.com/ChinmayNoob/lsm-go/memtable"
	"github.com/ChinmayNoob/lsm-go/sstable"
	"github.com/ChinmayNoob/lsm-go/wal"
)

var (
	ErrClosed   = errors.New("db is closed")
	ErrEmptyKey = errors.New("empty key")
)

type DB struct {
	mu     sync.Mutex
	closed bool

	mem *memtable.Memtable
	seq uint64

	opts    Options
	walPath string
	w       *wal.WAL

	memBytes int

	sstDir   string
	nextSST  uint64
	sstables []*sstable.Table // sorted by ID ascending
}

func Open(opts Options) (*DB, error) {
	if opts.Dir == "" {
		opts.Dir = "."
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}

	sstDir := filepath.Join(opts.Dir, "sstables")
	if err := os.MkdirAll(sstDir, 0o755); err != nil {
		return nil, err
	}
	// Cleanup leftover tmp files.
	if err := cleanupTmpFiles(sstDir); err != nil {
		return nil, err
	}

	d := &DB{
		opts:    opts,
		mem:     memtable.New(),
		seq:     1,
		walPath: filepath.Join(opts.Dir, "wal.log"),
		sstDir:  sstDir,
	}

	// Replay WAL into memtable (if present).
	maxSeq, err := wal.Replay(d.walPath, func(r wal.Record) error {
		switch r.Op {
		case wal.OpPut:
			d.mem.Apply(memtable.Record{
				Key:   r.Key,
				Value: r.Value,
				Seq:   r.Seq,
			})
		case wal.OpDelete:
			d.mem.Apply(memtable.Record{
				Key:       r.Key,
				Tombstone: true,
				Seq:       r.Seq,
			})
		default:
			return wal.ErrCorrupt
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	d.seq = maxSeq + 1

	// Load existing SSTables (minimal manifest).
	tables, nextID, err := loadSSTables(d.sstDir)
	if err != nil {
		return nil, err
	}
	d.sstables = tables
	d.nextSST = nextID

	ww, err := wal.Open(d.walPath, opts.SyncOnWrite)
	if err != nil {
		return nil, err
	}
	d.w = ww
	return d, nil
}

func (d *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	if value == nil {
		// Treat nil as empty; keeps semantics simple for beginners.
		value = []byte{}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	seq := d.seq
	d.seq++
	if err := d.w.Append(wal.OpPut, seq, key, value); err != nil {
		return err
	}
	d.mem.Apply(memtable.Record{
		Key:   key,
		Value: value,
		Seq:   seq,
	})
	d.memBytes += approxRecordBytes(key, value)
	if err := d.maybeFlushLocked(); err != nil {
		return err
	}
	return nil
}

func (d *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	seq := d.seq
	d.seq++
	if err := d.w.Append(wal.OpDelete, seq, key, nil); err != nil {
		return err
	}
	d.mem.Apply(memtable.Record{
		Key:       key,
		Tombstone: true,
		Seq:       seq,
	})
	d.memBytes += approxRecordBytes(key, nil)
	if err := d.maybeFlushLocked(); err != nil {
		return err
	}
	return nil
}

// Get returns (value, ok, err).
//
// ok=false means key not found (or deleted by tombstone).
func (d *DB) Get(key []byte) ([]byte, bool, error) {
	if len(key) == 0 {
		return nil, false, ErrEmptyKey
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, false, ErrClosed
	}
	r, ok := d.mem.Get(key)
	if ok {
		if d.opts.Verbose {
			fmt.Fprintf(os.Stderr, "[get] found in memtable\n")
		}
		if r.Tombstone {
			return nil, false, nil
		}
		return r.Value, true, nil
	}
	if d.opts.Verbose {
		fmt.Fprintf(os.Stderr, "[get] not in memtable, checking %d SSTables...\n", len(d.sstables))
	}

	// SSTables: newest to oldest.
	for i := len(d.sstables) - 1; i >= 0; i-- {
		tbl := d.sstables[i]
		if !tbl.MaybeContains(key) {
			if d.opts.Verbose {
				fmt.Fprintf(os.Stderr, "[bloom] SSTable-%06d: skipped (key not present)\n", tbl.ID)
			}
			continue
		}
		if d.opts.Verbose {
			fmt.Fprintf(os.Stderr, "[bloom] SSTable-%06d: maybe present, checking...\n", tbl.ID)
		}
		rec, ok, err := tbl.Get(key)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			if d.opts.Verbose {
				fmt.Fprintf(os.Stderr, "[bloom] SSTable-%06d: false positive (not found after check)\n", tbl.ID)
			}
			continue
		}
		if rec.Tombstone {
			if d.opts.Verbose {
				fmt.Fprintf(os.Stderr, "[bloom] SSTable-%06d: found tombstone\n", tbl.ID)
			}
			return nil, false, nil
		}
		if d.opts.Verbose {
			fmt.Fprintf(os.Stderr, "[bloom] SSTable-%06d: found value\n", tbl.ID)
		}
		return rec.Value, true, nil
	}

	if d.opts.Verbose {
		fmt.Fprintf(os.Stderr, "[get] key not found in any SSTable\n")
	}
	return nil, false, nil
}

func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	if d.w != nil {
		if err := d.w.Close(); err != nil {
			return err
		}
	}
	d.closed = true
	return nil
}

func (d *DB) maybeFlushLocked() error {
	if d.opts.MemtableMaxBytes <= 0 {
		return nil
	}
	if d.memBytes < d.opts.MemtableMaxBytes {
		return nil
	}

	// Rotate WAL safely: keep old WAL until flush succeeds.
	oldWALPath := d.walPath + fmt.Sprintf(".old-%d", d.seq)
	if err := d.w.Close(); err != nil {
		return err
	}
	if err := os.Rename(d.walPath, oldWALPath); err != nil {
		return err
	}
	newW, err := wal.Open(d.walPath, d.opts.SyncOnWrite)
	if err != nil {
		_ = os.Rename(oldWALPath, d.walPath)
		return err
	}

	immutable := d.mem
	keys := immutable.KeysSorted()

	// Swap to new memtable + WAL.
	d.mem = memtable.New()
	d.memBytes = 0
	d.w = newW

	// Flush immutable memtable to SSTable.
	id := d.nextSST
	if id == 0 {
		id = 1
	}
	d.nextSST = id + 1
	sstPath := filepath.Join(d.sstDir, sstable.FormatFilename(id))
	if d.opts.Verbose {
		fmt.Fprintf(os.Stderr, "[flush] flushing memtable (%d keys) to SSTable-%06d\n", len(keys), id)
	}
	if err := sstable.Build(sstPath, keys, immutable, 16); err != nil {
		return err
	}
	tbl, err := sstable.Open(sstPath, id)
	if err != nil {
		return err
	}
	d.sstables = append(d.sstables, tbl)
	sort.Slice(d.sstables, func(i, j int) bool { return d.sstables[i].ID < d.sstables[j].ID })
	if d.opts.Verbose {
		fmt.Fprintf(os.Stderr, "[flush] SSTable-%06d created (with Bloom filter)\n", id)
	}

	// Delete old WAL now that its contents are safely persisted.
	_ = os.Remove(oldWALPath)

	// Optional compaction trigger.
	if d.opts.MaxSSTTables > 0 && len(d.sstables) > d.opts.MaxSSTTables {
		return d.compactLocked()
	}
	return nil
}

func approxRecordBytes(key, value []byte) int {
	return len(key) + len(value) + 32
}

func loadSSTables(dir string) ([]*sstable.Table, uint64, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, 1, err
	}
	type pair struct {
		id   uint64
		path string
	}
	var ps []pair
	var maxID uint64
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, "sstable-") || !strings.HasSuffix(name, ".sst") {
			continue
		}
		idStr := strings.TrimSuffix(strings.TrimPrefix(name, "sstable-"), ".sst")
		id64, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil {
			continue
		}
		if id64 > maxID {
			maxID = id64
		}
		ps = append(ps, pair{id: id64, path: filepath.Join(dir, name)})
	}
	sort.Slice(ps, func(i, j int) bool { return ps[i].id < ps[j].id })
	out := make([]*sstable.Table, 0, len(ps))
	for _, p := range ps {
		t, err := sstable.Open(p.path, p.id)
		if err != nil {
			return nil, 1, err
		}
		out = append(out, t)
	}
	return out, maxID + 1, nil
}

func cleanupTmpFiles(dir string) error {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), ".tmp") {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
	return nil
}

func (d *DB) compactLocked() error {
	if len(d.sstables) <= 1 {
		return nil
	}
	if d.opts.Verbose {
		fmt.Fprintf(os.Stderr, "[compact] merging %d SSTables...\n", len(d.sstables))
	}
	outID := d.nextSST
	if outID == 0 {
		outID = 1
	}
	d.nextSST = outID + 1

	newTbl, err := compaction.Run(d.sstDir, d.sstables, outID)
	if err != nil {
		return err
	}
	if newTbl == nil {
		return nil
	}
	if d.opts.Verbose {
		fmt.Fprintf(os.Stderr, "[compact] created SSTable-%06d (with Bloom filter)\n", outID)
	}
	d.sstables = []*sstable.Table{newTbl}
	return nil
}
