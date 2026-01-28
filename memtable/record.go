package memtable

type Record struct {
	Key []byte
	Value []byte
	Tombstone bool
	Seq uint64
}

// seq is a monotonically increasing sequence number
//tombstone means the key is deleted at Seq
