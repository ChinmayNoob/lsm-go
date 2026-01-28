package db

type Options struct {
	Dir string //base dir
	SyncOnWrite bool //fsyncs the wal after each record
	MemtableMaxBytes int //triggers flush when it exceeds
	MaxSSTTables int // triggers compaction
	Verbose bool //bloom filter hit/miss
}

func DefaultOptions() Options {
	return Options{
		Dir: "",
		SyncOnWrite: true,
		MemtableMaxBytes: 0,
		MaxSSTTables: 0,
	}
}

