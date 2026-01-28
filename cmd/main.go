package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/ChinmayNoob/lsm-go/db"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	cmd := os.Args[1]

	fs := flag.NewFlagSet("lsm-go", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	dir := fs.String("dir", "data", "DB directory (WAL + SSTables live here)")
	memMax := fs.Int("mem", 0, "MemtableMaxBytes (0 disables flush)")
	maxSST := fs.Int("maxsst", 0, "MaxSSTables before compaction (0 disables)")
	syncOnWrite := fs.Bool("sync", true, "fsync WAL on each write")
	verbose := fs.Bool("verbose", false, "show Bloom filter behavior and SSTable checks")

	if err := fs.Parse(os.Args[2:]); err != nil {
		os.Exit(2)
	}
	args := fs.Args()

	opts := db.DefaultOptions()
	opts.Dir = *dir
	opts.MemtableMaxBytes = *memMax
	opts.MaxSSTTables = *maxSST
	opts.SyncOnWrite = *syncOnWrite
	opts.Verbose = *verbose

	d, err := db.Open(opts)
	if err != nil {
		fatal(err)
	}
	defer func() { _ = d.Close() }()

	switch cmd {
	case "put":
		if len(args) != 2 {
			usage()
			os.Exit(2)
		}
		if err := d.Put([]byte(args[0]), []byte(args[1])); err != nil {
			fatal(err)
		}
		fmt.Println("ok")
	case "get":
		if len(args) != 1 {
			usage()
			os.Exit(2)
		}
		v, ok, err := d.Get([]byte(args[0]))
		if err != nil {
			fatal(err)
		}
		if !ok {
			fmt.Println("(not found)")
			os.Exit(1)
		}
		fmt.Println(string(v))
	case "del":
		if len(args) != 1 {
			usage()
			os.Exit(2)
		}
		if err := d.Delete([]byte(args[0])); err != nil {
			fatal(err)
		}
		fmt.Println("ok")
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintln(os.Stderr, "  lsm-go [flags] put <key> <value>")
	fmt.Fprintln(os.Stderr, "  lsm-go [flags] get <key>")
	fmt.Fprintln(os.Stderr, "  lsm-go [flags] del <key>")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Flags:")
	fmt.Fprintln(os.Stderr, "  -dir     DB directory (default: data)")
	fmt.Fprintln(os.Stderr, "  -mem     MemtableMaxBytes (0 disables flush)")
	fmt.Fprintln(os.Stderr, "  -maxsst  Max SSTables before compaction (0 disables)")
	fmt.Fprintln(os.Stderr, "  -sync    fsync WAL on each write (default: true)")
	fmt.Fprintln(os.Stderr, "  -verbose show Bloom filter behavior (skipped SSTables)")
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
