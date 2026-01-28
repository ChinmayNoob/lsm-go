package memtable

import "bytes"

type Memtable struct {
	byKey map[string]Record
}

func New() *Memtable {
	return &Memtable{
		byKey: make(map[string]Record),
	}
}

// applies record if it is newer than the current record 
func (m *Memtable) Apply(r Record) {
	k := string(r.Key)
	curr, ok := m.byKey[k]

	if !ok || r.Seq >= curr.Seq {
		//copy bytes so that cant mutate internal state
		m.byKey[k] = Record{
			Key:       cloneBytes(r.Key),
			Value:     cloneBytes(r.Value),
			Tombstone: r.Tombstone,
			Seq:       r.Seq,
		}
	}
}

//returns latest recs
func (m *Memtable) Get(key []byte) (Record, bool) {
	r, ok := m.byKey[string(key)]

	if !ok {
		return Record{}, false
	}

	r.Key = cloneBytes(r.Key)
	r.Value = cloneBytes(r.Value)
	return r, true
}

func (m *Memtable) KeysSorted() [][]byte {
	keys := make([][]byte, 0, len(m.byKey))
	for _, r := range m.byKey {
		keys = append(keys, cloneBytes(r.Key))
	}
	sortBytesSlices(keys)
	return keys
}

func sortBytesSlices(keys [][]byte) {
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if bytes.Compare(keys[j], keys[i]) < 0 {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
