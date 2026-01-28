package bloom

import (
	"encoding/binary"
	"hash/fnv"
)

// guarantees no false negatives but many have false positives
type Filter struct {
	k    uint8
	bits uint32
	buf  []byte
}

func New(bits uint32, k uint8) *Filter {
	if k == 0 {
		k = 7
	}
	if bits < 8 {
		bits = 8
	}
	byteLen := (bits + 7) / 8
	bits = byteLen * 8
	return &Filter{
		k:    k,
		bits: bits,
		buf:  make([]byte, byteLen),
	}
}

func NewForKeys(nkeys int, bitsPerKey uint32, k uint8) *Filter {
	if nkeys < 1 {
		nkeys = 1
	}
	if bitsPerKey == 0 {
		bitsPerKey = 10
	}
	return New(uint32(nkeys)*bitsPerKey, k)
}

func (f *Filter) Add(key []byte) {
	h1, h2 := hash2(key)
	for i := uint8(0); i < f.k; i++ {
		// double hashing: h_i = h1 + i*h2
		h := h1 + uint64(i)*h2
		f.setBit(uint32(h % uint64(f.bits)))
	}
}

func (f *Filter) MaybeContains(key []byte) bool {
	h1, h2 := hash2(key)
	for i := uint8(0); i < f.k; i++ {
		h := h1 + uint64(i)*h2
		if !f.getBit(uint32(h % uint64(f.bits))) {
			return false
		}
	}
	return true
}

func (f *Filter) setBit(bit uint32) {
	byteIdx := bit / 8
	mask := byte(1 << (bit % 8))
	f.buf[byteIdx] |= mask
}
func (f *Filter) getBit(bit uint32) bool {
	byteIdx := bit / 8
	mask := byte(1 << (bit % 8))
	return (f.buf[byteIdx] & mask) != 0
}

func (f *Filter) Encode() []byte {
	out := make([]byte, 1+4+len(f.buf))
	out[0] = f.k
	binary.LittleEndian.PutUint32(out[1:5], f.bits)
	copy(out[5:], f.buf)
	return out
}

func Decode(b []byte) (*Filter, bool) {
	if len(b) < 1+4 {
		return nil, false
	}
	k := b[0]
	bits := binary.LittleEndian.Uint32(b[1:5])
	buf := b[5:]
	if bits == 0 || k == 0 {
		return nil, false
	}
	// Must match buffer length.
	if uint32(len(buf))*8 != bits {
		return nil, false
	}
	f := &Filter{k: k, bits: bits, buf: make([]byte, len(buf))}
	copy(f.buf, buf)
	return f, true
}

// hash2 returns two 64-bit hashes for double hashing.
func hash2(key []byte) (uint64, uint64) {
	// h1: FNV-1a 64
	h := fnv.New64a()
	_, _ = h.Write(key)
	h1 := h.Sum64()

	// h2: FNV-1a 64 of prefixed key (cheap second hash).
	h.Reset()
	_, _ = h.Write([]byte{0x7f})
	_, _ = h.Write(key)
	h2 := h.Sum64()
	if h2 == 0 {
		h2 = 0x9e3779b97f4a7c15
	}
	return h1, h2
}
