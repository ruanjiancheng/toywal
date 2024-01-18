package toywal

import "os"

type Options struct {
	DirPath string

	SegmentSize       uint64
	SegmentFileSuffix string

	BlockCacheSize uint64

	Sync         bool
	BytesPerSync uint32
}

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

var DefaultOptions = Options{
	DirPath:           os.TempDir(),
	SegmentSize:       GB,
	SegmentFileSuffix: ".seg",
	BlockCacheSize:    32 * KB * 10,
	Sync:              false,
	BytesPerSync:      0,
}
