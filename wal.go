package toywal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
)

const (
	INITIAL_SEGMENT_ID = 0
)

type WAL struct {
	options           *Options
	mu                sync.RWMutex
	activeSegment     *Segment
	oldSegments       map[SegmentID]*Segment
	blockCache        *lru.Cache[uint64, []byte]
	bytesWriteCounter uint32
}

type Reader struct {
	segmentReaders []SegmentReader
	currentReader  uint32
}

func New(options *Options) (*WAL, error) {
	if !strings.HasPrefix(options.SegmentFileSuffix, ".") {
		options.SegmentFileSuffix = fmt.Sprintf(".%v", options.SegmentFileSuffix)
	}

	if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
		return nil, err
	}

	wal := &WAL{
		options:           options,
		mu:                sync.RWMutex{},
		oldSegments:       make(map[uint32]*Segment),
		bytesWriteCounter: 0,
	}

	if options.BlockCacheSize > 0 {
		cacheSize := (options.BlockCacheSize-1)/BLOCK_SIZE + 1

		cache, err := lru.New[uint64, []byte](int(cacheSize))
		if err != nil {
			return nil, err
		}
		wal.blockCache = cache
	}

	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}

	var segmentIDs []int
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var id int
		_, err := fmt.Sscanf(entry.Name(), "%d"+options.SegmentFileSuffix, &id)
		if err != nil {
			continue
		}
		segmentIDs = append(segmentIDs, id)
	}

	if len(segmentIDs) == 0 {
		segment, err := OpenSegmentFile(options.DirPath, options.SegmentFileSuffix,
			INITIAL_SEGMENT_ID, wal.blockCache)
		if err != nil {
			return nil, err
		}
		wal.activeSegment = segment
	} else {
		sort.Ints(segmentIDs)

		for i, segId := range segmentIDs {
			segment, err := OpenSegmentFile(options.DirPath, options.SegmentFileSuffix,
				uint32(segId), wal.blockCache)
			if err != nil {
				return nil, err
			}
			if i == len(segmentIDs)-1 {
				wal.activeSegment = segment
			} else {
				wal.oldSegments[segment.id] = segment
			}
		}
	}
	return wal, nil
}

func (wal *WAL) GetActiveSegmentID() SegmentID {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	return wal.activeSegment.id
}

func (wal *WAL) IsEmpty() bool {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	return len(wal.oldSegments) == 0 && wal.activeSegment.currentBlockSize == 0
}

func (wal *WAL) NewReaderWithMax(sid SegmentID) *Reader {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	segmentReaders := make([]SegmentReader, 0)
	for _, segment := range wal.oldSegments {
		if sid == 0 || segment.id <= sid {
			segmentReaders = append(segmentReaders, *segment.NewSegmentReader())
		}
	}
	if sid == 0 || wal.activeSegment.id <= sid {
		segmentReaders = append(segmentReaders, *wal.activeSegment.NewSegmentReader())
	}

	sort.Slice(segmentReaders, func(i, j int) bool {
		return segmentReaders[i].segment.id < segmentReaders[j].segment.id
	})

	return &Reader{
		segmentReaders: segmentReaders,
		currentReader:  0,
	}
}

func (wal *WAL) NewReader() *Reader {
	return wal.NewReaderWithMax(0)
}

func (wal *WAL) NewReaderWithStart(pos *ChunkPosition) (*Reader, error) {
	if pos == nil {
		return nil, errors.New("pos is nil")
	}

	wal.mu.RLock()
	defer wal.mu.Unlock()

	reader := wal.NewReader()
	for {
		if reader.CurrentSegmentId() < pos.SegmentId {
			reader.SkipCurrentSegment()
			continue
		}
		currentPos := reader.CurrentChunkPosition()
		if currentPos.BlockId >= pos.BlockId &&
			currentPos.ChunkOffset >= pos.ChunkOffset {
			break
		}

		if _, _, err := reader.Next(); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}
	return reader, nil
}

func (r *Reader) SkipCurrentSegment() {
	r.currentReader++
}

func (r *Reader) CurrentSegmentId() SegmentID {
	return r.segmentReaders[r.currentReader].segment.id
}

func (r *Reader) CurrentChunkPosition() *ChunkPosition {
	reader := r.segmentReaders[r.currentReader]
	return &ChunkPosition{
		SegmentId:   reader.segment.id,
		BlockId:     reader.blockID,
		ChunkOffset: reader.chunkOffset,
	}
}

func (wal *WAL) rotateActiveSegment() error {
	if err := wal.activeSegment.Sync(); err != nil {
		return err
	}
	wal.bytesWriteCounter = 0
	segment, err := OpenSegmentFile(wal.options.DirPath, wal.options.SegmentFileSuffix,
		wal.activeSegment.id+1, wal.blockCache)
	if err != nil {
		return err
	}
	wal.oldSegments[wal.activeSegment.id] = wal.activeSegment
	wal.activeSegment = segment
	return nil
}

func (r *Reader) Next() ([]byte, *ChunkPosition, error) {
	if int(r.currentReader) >= len(r.segmentReaders) {
		return nil, nil, io.EOF
	}

	data, position, err := r.segmentReaders[r.currentReader].Next()
	if err == io.EOF {
		r.currentReader++
		return r.Next()
	}
	return data, position, err
}

func (wal *WAL) isFull(delta int64) bool {
	return wal.activeSegment.Size()+uint64(wal.maxDataWriteSize(delta)) > wal.options.SegmentSize
}

func (wal *WAL) maxDataWriteSize(size int64) int64 {
	return CHUNK_HEADER_SIZE + size + (size/BLOCK_SIZE+1)*CHUNK_HEADER_SIZE
}

func (wal *WAL) Write(data []byte) (*ChunkPosition, error) {
	if len(data) > int(wal.options.SegmentSize) {
		return nil, errors.New("data is too large")
	}

	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.isFull(int64(len(data))) {
		if err := wal.rotateActiveSegment(); err != nil {
			return nil, err
		}
	}

	pos, err := wal.activeSegment.Write(data)
	if err != nil {
		return nil, err
	}

	wal.bytesWriteCounter += uint32(len(data))
	if wal.options.Sync || (wal.options.BytesPerSync <= wal.bytesWriteCounter && wal.options.BytesPerSync != 0) {
		if err := wal.activeSegment.Sync(); err != nil {
			return nil, err
		}
		wal.bytesWriteCounter = 0
	}

	return pos, nil
}

func (wal *WAL) Read(pos *ChunkPosition) ([]byte, error) {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	if segment, ok := wal.oldSegments[pos.SegmentId]; ok {
		data, err := segment.Read(pos.BlockId, pos.ChunkOffset)
		if err != nil {
			return nil, err
		}
		return data, nil
	} else if pos.SegmentId == wal.activeSegment.id {
		data, err := wal.activeSegment.Read(pos.BlockId, pos.ChunkOffset)
		if err != nil {
			return nil, err
		}
		return data, nil
	}

	return nil, errors.New("pos is not found")
}

func (wal *WAL) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.blockCache != nil {
		wal.blockCache.Purge()
	}
	wal.blockCache = nil

	for _, segment := range wal.oldSegments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	wal.oldSegments = nil

	if err := wal.activeSegment.Close(); err != nil {
		return err
	}
	wal.activeSegment = nil

	return nil
}

func (wal *WAL) Delete() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.blockCache != nil {
		wal.blockCache.Purge()
	}

	for _, segment := range wal.oldSegments {
		if err := segment.Remove(); err != nil {
			return err
		}
	}
	wal.oldSegments = nil

	return wal.activeSegment.Remove()
}

func (wal *WAL) Sync() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	return wal.activeSegment.Sync()
}
