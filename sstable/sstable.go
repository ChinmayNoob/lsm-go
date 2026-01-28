package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/ChinmayNoob/lsm-go/bloom"
	"github.com/ChinmayNoob/lsm-go/memtable"
)

const (
	magic        uint32 = 0x4c534d31
	version      uint16 = 1
	versionBloom uint16 = 2
)

var ErrCorrupt = errors.New("sstable: corrupt")

type indexEntry struct {
	key    []byte
	offset uint64
}

type Table struct {
	Path  string
	ID    uint64
	index []indexEntry

	indexOffset uint64

	bloomOffset uint64
	bloomLen    uint64
	bf          *bloom.Filter
}

// Open opens an existing SSTable and loads its sparse index.
func Open(path string, id uint64) (*Table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() < 8+4+2 {
		return nil, ErrCorrupt
	}

	// Peek v1 footer first to get version.
	v1FooterSize := int64(8 + 4 + 2)
	footer := make([]byte, v1FooterSize)
	if _, err := f.ReadAt(footer, st.Size()-v1FooterSize); err != nil {
		return nil, err
	}
	gotMagic := binary.LittleEndian.Uint32(footer[8:12])
	gotVer := binary.LittleEndian.Uint16(footer[12:14])
	if gotMagic != magic {
		return nil, ErrCorrupt
	}

	var (
		idxOff     uint64
		bloomOff   uint64
		bloomLen   uint64
		footerSize uint64
	)
	switch gotVer {
	case version:
		idxOff = binary.LittleEndian.Uint64(footer[0:8])
		footerSize = uint64(v1FooterSize)
	case versionBloom:
		// Footer v2 layout (30 bytes):
		// [u64 indexOffset][u64 bloomOffset][u64 bloomLen][u32 magic][u16 version]
		v2Size := int64(8 + 8 + 8 + 4 + 2)
		if st.Size() < v2Size {
			return nil, ErrCorrupt
		}
		v2 := make([]byte, v2Size)
		if _, err := f.ReadAt(v2, st.Size()-v2Size); err != nil {
			return nil, err
		}
		idxOff = binary.LittleEndian.Uint64(v2[0:8])
		bloomOff = binary.LittleEndian.Uint64(v2[8:16])
		bloomLen = binary.LittleEndian.Uint64(v2[16:24])
		gotMagic2 := binary.LittleEndian.Uint32(v2[24:28])
		gotVer2 := binary.LittleEndian.Uint16(v2[28:30])
		if gotMagic2 != magic || gotVer2 != versionBloom {
			return nil, ErrCorrupt
		}
		footerSize = uint64(v2Size)
	default:
		return nil, ErrCorrupt
	}

	if idxOff >= uint64(st.Size()) {
		return nil, ErrCorrupt
	}

	// Index format: repeated [u32 keyLen][key][u64 offset] until footer starts.
	indexBytesLen := uint64(st.Size()) - footerSize - idxOff
	if _, err := f.Seek(int64(idxOff), io.SeekStart); err != nil {
		return nil, err
	}
	r := bufio.NewReaderSize(f, 64*1024)
	limited := io.LimitReader(r, int64(indexBytesLen))

	var entries []indexEntry
	for {
		var klenBuf [4]byte
		_, err := io.ReadFull(limited, klenBuf[:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, ErrCorrupt
			}
			return nil, err
		}
		klen := binary.LittleEndian.Uint32(klenBuf[:])
		if klen == 0 {
			return nil, ErrCorrupt
		}
		k := make([]byte, klen)
		if _, err := io.ReadFull(limited, k); err != nil {
			return nil, ErrCorrupt
		}
		var offBuf [8]byte
		if _, err := io.ReadFull(limited, offBuf[:]); err != nil {
			return nil, ErrCorrupt
		}
		off := binary.LittleEndian.Uint64(offBuf[:])
		entries = append(entries, indexEntry{key: k, offset: off})
	}

	t := &Table{
		Path:        path,
		ID:          id,
		index:       entries,
		indexOffset: idxOff,
		bloomOffset: bloomOff,
		bloomLen:    bloomLen,
	}

	if bloomLen > 0 {
		if bloomOff >= uint64(st.Size()) || bloomOff+bloomLen > uint64(st.Size()) {
			return nil, ErrCorrupt
		}
		bb := make([]byte, bloomLen)
		if _, err := f.ReadAt(bb, int64(bloomOff)); err != nil {
			return nil, err
		}
		bf, ok := bloom.Decode(bb)
		if !ok {
			return nil, ErrCorrupt
		}
		t.bf = bf
	}

	return t, nil
}

// Build writes a new SSTable at path from the given memtable.
// keys must be sorted (ascending).
func Build(path string, keys [][]byte, mt *memtable.Memtable, indexEveryN int) error {
	if indexEveryN <= 0 {
		indexEveryN = 16
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	w := bufio.NewWriterSize(f, 64*1024)

	var index []indexEntry
	bf := bloom.NewForKeys(len(keys), 10, 7)
	for i, k := range keys {
		r, ok := mt.Get(k)
		if !ok {
			continue
		}
		off, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		if i%indexEveryN == 0 {
			index = append(index, indexEntry{key: cloneBytes(k), offset: uint64(off)})
		}
		bf.Add(k)
		if err := writeEntry(w, r); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}

	// Bloom section.
	bloomOff, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	bloomBytes := bf.Encode()
	if _, err := w.Write(bloomBytes); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}

	idxOff, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	// Write index.
	for _, e := range index {
		if err := writeIndexEntry(w, e); err != nil {
			return err
		}
	}
	// Footer.
	// Footer v2 layout (30 bytes):
	// [u64 indexOffset][u64 bloomOffset][u64 bloomLen][u32 magic][u16 version]
	var footer [8 + 8 + 8 + 4 + 2]byte
	binary.LittleEndian.PutUint64(footer[0:8], uint64(idxOff))
	binary.LittleEndian.PutUint64(footer[8:16], uint64(bloomOff))
	binary.LittleEndian.PutUint64(footer[16:24], uint64(len(bloomBytes)))
	binary.LittleEndian.PutUint32(footer[24:28], magic)
	binary.LittleEndian.PutUint16(footer[28:30], versionBloom)
	if _, err := w.Write(footer[:]); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return f.Sync()
}

func writeIndexEntry(w *bufio.Writer, e indexEntry) error {
	var klenBuf [4]byte
	binary.LittleEndian.PutUint32(klenBuf[:], uint32(len(e.key)))
	if _, err := w.Write(klenBuf[:]); err != nil {
		return err
	}
	if _, err := w.Write(e.key); err != nil {
		return err
	}
	var offBuf [8]byte
	binary.LittleEndian.PutUint64(offBuf[:], e.offset)
	if _, err := w.Write(offBuf[:]); err != nil {
		return err
	}
	return nil
}

// Entry format:
// [u32 keyLen][key][u8 tomb][u32 valLen][val][u64 seq]
func writeEntry(w *bufio.Writer, r memtable.Record) error {
	var klenBuf [4]byte
	binary.LittleEndian.PutUint32(klenBuf[:], uint32(len(r.Key)))
	if _, err := w.Write(klenBuf[:]); err != nil {
		return err
	}
	if _, err := w.Write(r.Key); err != nil {
		return err
	}
	t := byte(0)
	if r.Tombstone {
		t = 1
	}
	if err := w.WriteByte(t); err != nil {
		return err
	}
	var vlenBuf [4]byte
	binary.LittleEndian.PutUint32(vlenBuf[:], uint32(len(r.Value)))
	if _, err := w.Write(vlenBuf[:]); err != nil {
		return err
	}
	if _, err := w.Write(r.Value); err != nil {
		return err
	}
	var seqBuf [8]byte
	binary.LittleEndian.PutUint64(seqBuf[:], r.Seq)
	if _, err := w.Write(seqBuf[:]); err != nil {
		return err
	}
	return nil
}

// Get looks for key in the table and returns the entry if found.
func (t *Table) Get(key []byte) (memtable.Record, bool, error) {
	f, err := os.Open(t.Path)
	if err != nil {
		return memtable.Record{}, false, err
	}
	defer func() { _ = f.Close() }()

	startOff, err := t.seekStartOffset(key)
	if err != nil {
		return memtable.Record{}, false, err
	}

	if _, err := f.Seek(int64(startOff), io.SeekStart); err != nil {
		return memtable.Record{}, false, err
	}

	// Scan forward until key >= target or we hit the index section.
	r := bufio.NewReaderSize(f, 64*1024)
	for {
		curOff, _ := f.Seek(0, io.SeekCurrent)
		if uint64(curOff) >= t.indexOffset {
			return memtable.Record{}, false, nil
		}
		rec, ok, err := readEntry(r)
		if err != nil {
			return memtable.Record{}, false, err
		}
		if !ok {
			return memtable.Record{}, false, nil
		}
		cmp := bytes.Compare(rec.Key, key)
		if cmp == 0 {
			return rec, true, nil
		}
		if cmp > 0 {
			return memtable.Record{}, false, nil
		}
	}
}

// MaybeContains checks the Bloom filter (if present).
// If the table doesn't have a Bloom filter (older version), it returns true.
func (t *Table) MaybeContains(key []byte) bool {
	if t.bf == nil {
		return true
	}
	return t.bf.MaybeContains(key)
}

func (t *Table) seekStartOffset(key []byte) (uint64, error) {
	if len(t.index) == 0 {
		return 0, nil
	}
	// Find last index entry with entry.key <= key.
	lo, hi := 0, len(t.index)
	for lo < hi {
		mid := (lo + hi) / 2
		if bytes.Compare(t.index[mid].key, key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	i := lo - 1
	if i < 0 {
		return 0, nil
	}
	return t.index[i].offset, nil
}

func readEntry(r *bufio.Reader) (memtable.Record, bool, error) {
	var klenBuf [4]byte
	_, err := io.ReadFull(r, klenBuf[:])
	if err != nil {
		if errors.Is(err, io.EOF) {
			return memtable.Record{}, false, nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return memtable.Record{}, false, ErrCorrupt
		}
		return memtable.Record{}, false, err
	}
	klen := binary.LittleEndian.Uint32(klenBuf[:])
	if klen == 0 {
		return memtable.Record{}, false, ErrCorrupt
	}
	k := make([]byte, klen)
	if _, err := io.ReadFull(r, k); err != nil {
		return memtable.Record{}, false, ErrCorrupt
	}
	tomb, err := r.ReadByte()
	if err != nil {
		return memtable.Record{}, false, ErrCorrupt
	}
	var vlenBuf [4]byte
	if _, err := io.ReadFull(r, vlenBuf[:]); err != nil {
		return memtable.Record{}, false, ErrCorrupt
	}
	vlen := binary.LittleEndian.Uint32(vlenBuf[:])
	v := make([]byte, vlen)
	if _, err := io.ReadFull(r, v); err != nil {
		return memtable.Record{}, false, ErrCorrupt
	}
	var seqBuf [8]byte
	if _, err := io.ReadFull(r, seqBuf[:]); err != nil {
		return memtable.Record{}, false, ErrCorrupt
	}
	seq := binary.LittleEndian.Uint64(seqBuf[:])
	return memtable.Record{
		Key:       k,
		Value:     v,
		Tombstone: tomb == 1,
		Seq:       seq,
	}, true, nil
}

func cloneBytes(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func FormatFilename(id uint64) string {
	return fmt.Sprintf("sstable-%06d.sst", id)
}


