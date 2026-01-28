package compaction

import (
	"bufio"
	"bytes"
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/ChinmayNoob/lsm-go/memtable"
	"github.com/ChinmayNoob/lsm-go/sstable"
)

// Run is a very simple compaction:
// - open all input tables
// - do a k-way merge by key, picking the highest Seq per key
// - write to a new SSTable (tmp + rename)
// - delete old SSTables
//
// Tombstones are preserved.
func Run(sstDir string, inputs []*sstable.Table, outputID uint64) (*sstable.Table, error) {
	if len(inputs) == 0 {
		return nil, nil
	}

	// We'll stream entries by scanning each file sequentially.
	iters := make([]*tableIter, 0, len(inputs))
	for _, t := range inputs {
		it, err := newTableIter(t)
		if err != nil {
			for _, it2 := range iters {
				_ = it2.close()
			}
			return nil, err
		}
		iters = append(iters, it)
	}
	defer func() {
		for _, it := range iters {
			_ = it.close()
		}
	}()

	// Initialize heap.
	h := &mergeHeap{}
	for _, it := range iters {
		if it.next() {
			heap.Push(h, it)
		}
		if it.err != nil {
			return nil, it.err
		}
	}

	// Output file path.
	finalName := sstable.FormatFilename(outputID)
	tmpPath := filepath.Join(sstDir, fmt.Sprintf("%s.tmp", finalName))
	outPath := filepath.Join(sstDir, finalName)

	// We'll build output using the main SSTable builder to keep file format consistent
	// (including Bloom filter, if enabled by the SSTable package).
	mt := memtable.New()
	var keys [][]byte

	var (
		curKey []byte
		best   memtable.Record
		have   bool
	)
	flushBest := func() error {
		if !have {
			return nil
		}
		mt.Apply(best)
		keys = append(keys, cloneBytes(best.Key))
		have = false
		curKey = nil
		return nil
	}

	for h.Len() > 0 {
		it := heap.Pop(h).(*tableIter)
		r := it.cur
		if !have || !bytes.Equal(r.Key, curKey) {
			if err := flushBest(); err != nil {
				return nil, err
			}
			curKey = cloneBytes(r.Key)
			best = r
			have = true
		} else {
			if r.Seq > best.Seq {
				best = r
			}
		}

		if it.next() {
			heap.Push(h, it)
		}
		if it.err != nil {
			return nil, it.err
		}
	}
	if err := flushBest(); err != nil {
		return nil, err
	}

	// keys are produced in sorted order by the merge.
	if err := sstable.Build(tmpPath, keys, mt, 16); err != nil {
		return nil, err
	}
	if err := os.Rename(tmpPath, outPath); err != nil {
		return nil, err
	}

	// Delete inputs.
	for _, t := range inputs {
		_ = os.Remove(t.Path)
	}

	return sstable.Open(outPath, outputID)
}

type tableIter struct {
	t *sstable.Table
	f *os.File
	r *bufio.Reader

	cur memtable.Record
	err error

	indexOffset uint64
}

func newTableIter(t *sstable.Table) (*tableIter, error) {
	f, err := os.Open(t.Path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	// Footer size: 14 bytes.
	if st.Size() < 14 {
		_ = f.Close()
		return nil, sstable.ErrCorrupt
	}
	footer := make([]byte, 14)
	if _, err := f.ReadAt(footer, st.Size()-14); err != nil {
		_ = f.Close()
		return nil, err
	}
	// Determine footer version by reading magic+version at the end.
	gotMagic := binary.LittleEndian.Uint32(footer[8:12])
	gotVer := binary.LittleEndian.Uint16(footer[12:14])
	if gotMagic != 0x4c534d31 {
		_ = f.Close()
		return nil, sstable.ErrCorrupt
	}
	var idxOff uint64
	if gotVer == 1 {
		idxOff = binaryLittleU64(footer[0:8])
	} else {
		// v2 footer is 30 bytes: [idxOff][bloomOff][bloomLen][magic][ver]
		if st.Size() < 30 {
			_ = f.Close()
			return nil, sstable.ErrCorrupt
		}
		v2 := make([]byte, 30)
		if _, err := f.ReadAt(v2, st.Size()-30); err != nil {
			_ = f.Close()
			return nil, err
		}
		idxOff = binary.LittleEndian.Uint64(v2[0:8])
	}

	if _, err := f.Seek(0, 0); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &tableIter{
		t:           t,
		f:           f,
		r:           bufio.NewReaderSize(f, 64*1024),
		indexOffset: idxOff,
	}, nil
}

func (it *tableIter) next() bool {
	if it.err != nil {
		return false
	}
	off, _ := it.f.Seek(0, 1)
	if uint64(off) >= it.indexOffset {
		return false
	}
	rec, ok, err := readEntry(it.r)
	if err != nil {
		it.err = err
		return false
	}
	if !ok {
		return false
	}
	it.cur = rec
	return true
}

func (it *tableIter) close() error {
	if it.f != nil {
		return it.f.Close()
	}
	return nil
}

type mergeHeap []*tableIter

func (h mergeHeap) Len() int           { return len(h) }
func (h mergeHeap) Less(i, j int) bool { return bytes.Compare(h[i].cur.Key, h[j].cur.Key) < 0 }
func (h mergeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)        { *h = append(*h, x.(*tableIter)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func cloneBytes(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func binaryLittleU64(b []byte) uint64 {
	_ = b[7]
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}

func readEntry(r *bufio.Reader) (memtable.Record, bool, error) {
	var klenBuf [4]byte
	if _, err := io.ReadFull(r, klenBuf[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return memtable.Record{}, false, nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return memtable.Record{}, false, sstable.ErrCorrupt
		}
		return memtable.Record{}, false, err
	}
	klen := binary.LittleEndian.Uint32(klenBuf[:])
	if klen == 0 {
		return memtable.Record{}, false, sstable.ErrCorrupt
	}
	k := make([]byte, klen)
	if _, err := io.ReadFull(r, k); err != nil {
		return memtable.Record{}, false, sstable.ErrCorrupt
	}
	tomb, err := r.ReadByte()
	if err != nil {
		return memtable.Record{}, false, sstable.ErrCorrupt
	}
	var vlenBuf [4]byte
	if _, err := io.ReadFull(r, vlenBuf[:]); err != nil {
		return memtable.Record{}, false, sstable.ErrCorrupt
	}
	vlen := binary.LittleEndian.Uint32(vlenBuf[:])
	v := make([]byte, vlen)
	if _, err := io.ReadFull(r, v); err != nil {
		return memtable.Record{}, false, sstable.ErrCorrupt
	}
	var seqBuf [8]byte
	if _, err := io.ReadFull(r, seqBuf[:]); err != nil {
		return memtable.Record{}, false, sstable.ErrCorrupt
	}
	seq := binary.LittleEndian.Uint64(seqBuf[:])
	return memtable.Record{Key: k, Value: v, Tombstone: tomb == 1, Seq: seq}, true, nil
}
