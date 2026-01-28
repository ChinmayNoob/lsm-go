package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
)

type Op uint8

const (
	OpPut    Op = 1
	OpDelete Op = 2
)

var ErrCorrupt = errors.New("corrupt wal")

type WAL struct {
	f           *os.File
	w           *bufio.Writer
	syncOnWrite bool
}

func Open(path string, syncOnWrite bool) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		f:           f,
		w:           bufio.NewWriter(f),
		syncOnWrite: syncOnWrite,
	}, nil
}

func (w *WAL) Close() error {
	if w == nil || w.f == nil {
		return nil
	}

	if err := w.w.Flush(); err != nil {
		_ = w.f.Close()
		return err
	}

	if err := w.f.Close(); err != nil {
		return err
	}
	return nil
}

func (w *WAL) Append(op Op, seq uint64, key, value []byte) error {
	if w == nil || w.f == nil {
		return errors.New("wal is closed")
	}

	keyLen := uint32(len(key))
	valLen := uint32(len(value))
	recLen := 1 + 8 + 4 + 4 + int(keyLen) + int(valLen)

	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(recLen))
	if _, err := w.w.Write(lenBuf[:]); err != nil {
		return err
	}

	var hdr [1 + 8 + 4 + 4]byte
	hdr[0] = byte(op)
	binary.LittleEndian.PutUint64(hdr[1:9], seq)
	binary.LittleEndian.PutUint32(hdr[9:13], keyLen)
	binary.LittleEndian.PutUint32(hdr[13:17], valLen)
	if _, err := w.w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.w.Write(key); err != nil {
		return err
	}
	if _, err := w.w.Write(value); err != nil {
		return err
	}

	if err := w.w.Flush(); err != nil {
		return err
	}
	if w.syncOnWrite {
		return w.f.Sync()
	}
	return nil

}


type Record struct {
	Op    Op
	Seq   uint64
	Key   []byte
	Value []byte
}

func Replay(path string, fn func(Record) error) (maxSeq uint64, err error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	defer func() { _ = f.Close() }()

	r := bufio.NewReaderSize(f, 64*1024)
	for {
		var lenBuf [4]byte
		_, err := io.ReadFull(r, lenBuf[:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				return maxSeq, nil
			}
			// If we got a clean EOF at boundary, ReadFull returns EOF above.
			if errors.Is(err, io.ErrUnexpectedEOF) {
				// Ignore trailing partial length prefix (common after crash).
				return maxSeq, nil
			}
			return maxSeq, err
		}
		recLen := binary.LittleEndian.Uint32(lenBuf[:])
		if recLen == 0 {
			return maxSeq, ErrCorrupt
		}
		rec := make([]byte, recLen)
		if _, err := io.ReadFull(r, rec); err != nil {
			// Ignore trailing partial record (common after crash).
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				return maxSeq, nil
			}
			return maxSeq, err
		}
		rr, err := decodeRecord(rec)
		if err != nil {
			return maxSeq, err
		}
		if rr.Seq > maxSeq {
			maxSeq = rr.Seq
		}
		if err := fn(rr); err != nil {
			return maxSeq, err
		}
	}
}

func decodeRecord(b []byte) (Record, error) {
	// [u8 op][u64 seq][u32 keyLen][u32 valLen][key][val]
	if len(b) < 1+8+4+4 {
		return Record{}, ErrCorrupt
	}
	op := Op(b[0])
	seq := binary.LittleEndian.Uint64(b[1:9])
	keyLen := binary.LittleEndian.Uint32(b[9:13])
	valLen := binary.LittleEndian.Uint32(b[13:17])
	need := 1 + 8 + 4 + 4 + int(keyLen) + int(valLen)
	if len(b) != need {
		return Record{}, ErrCorrupt
	}
	keyStart := 17
	keyEnd := keyStart + int(keyLen)
	valEnd := keyEnd + int(valLen)
	key := make([]byte, keyLen)
	copy(key, b[keyStart:keyEnd])
	val := make([]byte, valLen)
	copy(val, b[keyEnd:valEnd])
	if op != OpPut && op != OpDelete {
		return Record{}, ErrCorrupt
	}
	return Record{Op: op, Seq: seq, Key: key, Value: val}, nil
}