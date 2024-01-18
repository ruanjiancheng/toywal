package benchmark

import (
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/ruanjiancheng/toywal"
	"github.com/stretchr/testify/assert"
)

var walFile *toywal.WAL

func init() {
	dir, _ := os.MkdirTemp("", "wal-benchmark-test")
	opts := &toywal.Options{
		DirPath:           dir,
		SegmentFileSuffix: ".SEG",
		SegmentSize:       toywal.GB,
	}
	var err error
	walFile, err = toywal.New(opts)
	if err != nil {
		panic(err)
	}
}

func BenchmarkWAL_WriteLargeSize(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	content := []byte(strings.Repeat("X", 256*toywal.KB+500))
	for i := 0; i < b.N; i++ {
		_, err := walFile.Write(content)
		assert.Nil(b, err)
	}
}

func BenchmarkWAL_Write(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := walFile.Write([]byte("Hello World"))
		assert.Nil(b, err)
	}
}

func BenchmarkWAL_Read(b *testing.B) {
	var positions []*toywal.ChunkPosition
	for i := 0; i < 1000000; i++ {
		pos, err := walFile.Write([]byte("Hello World"))
		assert.Nil(b, err)
		positions = append(positions, pos)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := walFile.Read(positions[rand.Intn(len(positions))])
		assert.Nil(b, err)
	}
}
