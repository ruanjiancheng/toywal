package toywal

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func testWriteAndIterate(t *testing.T, wal *WAL, size int, valueSize int) {
	val := strings.Repeat("wal", valueSize)
	positions := make([]*ChunkPosition, size)
	for i := 0; i < size; i++ {
		pos, err := wal.Write([]byte(val))
		assert.Nil(t, err)
		positions[i] = pos
	}

	var count int
	reader := wal.NewReader()
	for {
		data, pos, err := reader.Next()
		if err != nil {
			break
		}
		assert.Equal(t, val, string(data))

		assert.Equal(t, positions[count].SegmentId, pos.SegmentId)
		assert.Equal(t, positions[count].BlockId, pos.BlockId)
		assert.Equal(t, positions[count].ChunkOffset, pos.ChunkOffset)

		count++
	}
	assert.Equal(t, size, count)
}

func testWriteAllIterate(t *testing.T, wal *WAL, size, valueSize int) {
	positions := make([]ChunkPosition, 0)
	for i := 0; i < size; i++ {
		val := strings.Repeat("wal", valueSize)
		pos, err := wal.Write([]byte(val))
		assert.Nil(t, err)
		positions = append(positions, *pos)
	}

	count := 0
	reader := wal.NewReader()
	for {
		data, pos, err := reader.Next()
		if err != nil {
			break
		}
		assert.Equal(t, strings.Repeat("wal", valueSize), string(data))

		assert.Equal(t, positions[count].SegmentId, pos.SegmentId)
		assert.Equal(t, positions[count].BlockId, pos.BlockId)
		assert.Equal(t, positions[count].ChunkOffset, pos.ChunkOffset)

		count++
	}
}

func setupWAL(t *testing.T, name string) *WAL {
	dir, _ := os.MkdirTemp("", name)
	opts := &Options{
		DirPath:           dir,
		SegmentFileSuffix: ".SEG",
		SegmentSize:       32 * MB,
		BlockCacheSize:    32 * KB * 10,
	}
	wal, err := New(opts)
	assert.Nil(t, err)
	return wal
}

func destroyWAL(wal *WAL) {
	if wal != nil {
		_ = wal.Close()
		_ = os.RemoveAll(wal.options.DirPath)
	}
}

func TestWAL_WriteALL(t *testing.T) {
	wal := setupWAL(t, "wal-test-write-batch-1")
	defer destroyWAL(wal)

	testWriteAllIterate(t, wal, 0, 10)
	assert.True(t, wal.IsEmpty())

	testWriteAllIterate(t, wal, 10000, 512)
	assert.False(t, wal.IsEmpty())
}

func TestWAL_Write(t *testing.T) {
	wal := setupWAL(t, "wal-test-write1")
	defer destroyWAL(wal)

	pos1, err := wal.Write([]byte("hello1"))
	assert.Nil(t, err)
	assert.NotNil(t, pos1)
	pos2, err := wal.Write([]byte("hello2"))
	assert.Nil(t, err)
	assert.NotNil(t, pos2)
	pos3, err := wal.Write([]byte("hello3"))
	assert.Nil(t, err)
	assert.NotNil(t, pos3)

	val, err := wal.Read(pos1)
	assert.Nil(t, err)
	assert.Equal(t, "hello1", string(val))
	val, err = wal.Read(pos2)
	assert.Nil(t, err)
	assert.Equal(t, "hello2", string(val))
	val, err = wal.Read(pos3)
	assert.Nil(t, err)
	assert.Equal(t, "hello3", string(val))
}

func TestWAL_Write_large(t *testing.T) {
	wal := setupWAL(t, "wal-test-write2")
	defer destroyWAL(wal)

	testWriteAndIterate(t, wal, 100000, 512)
}

func TestWAL_Write_large2(t *testing.T) {
	wal := setupWAL(t, "wal-test-write3")
	defer destroyWAL(wal)

	testWriteAndIterate(t, wal, 2000, 32*1024*3+10)
}

func TestWAL_Reader(t *testing.T) {
	wal := setupWAL(t, "wal-test-wal-reader")
	defer destroyWAL(wal)

	var size = 100000
	val := strings.Repeat("wal", 512)
	for i := 0; i < size; i++ {
		_, err := wal.Write([]byte(val))
		assert.Nil(t, err)
	}

	validate := func(walInner *WAL, size int) {
		var i = 0
		reader := walInner.NewReader()
		for {
			chunk, position, err := reader.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				panic(err)
			}
			assert.NotNil(t, chunk)
			assert.NotNil(t, position)
			assert.Equal(t, position.SegmentId, reader.CurrentSegmentId())
			i++
		}
		assert.Equal(t, i, size)
	}

	validate(wal, size)
}
