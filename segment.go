package toywal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/valyala/bytebufferpool"
)

type ChunkType = byte
type SegmentID = uint32
type BlockID = uint32

const (
	ChunkTypeFull ChunkType = iota
	ChunkTypeFirst
	ChunkTypeMiddle
	ChunkTypeLast
)

var (
	ErrClosed = errors.New("the segment file is closed")
	ErrCRC    = errors.New("invalid crc, the data may be corrupted")
)

const (
	CHUNK_HEADER_SIZE = 7 * B   // chunk header is CRC(4B), length(2B), type(1B)
	BLOCK_SIZE        = 32 * KB // block size is 32KB
	FILE_MODE_PERM    = 0644    // mode: -rx-r--r--
	MAX_LEN           = binary.MaxVarintLen32*3 + binary.MaxVarintLen64
)

type Segment struct {
	// segment id
	id SegmentID
	// file fd
	fd *os.File
	// 当前 active 的 block id 和 size
	currentBlockID   BlockID
	currentBlockSize uint32

	closed    bool
	cache     *lru.Cache[uint64, []byte]
	header    []byte
	blockPool sync.Pool
}

type SegmentReader struct {
	segment     *Segment
	blockID     BlockID
	chunkOffset int64
}

type BlockAndHeader struct {
	block  []byte
	header []byte
}

type ChunkPosition struct {
	SegmentId   SegmentID
	BlockId     BlockID
	ChunkOffset int64
	ChunkSize   uint32
}

func OpenSegmentFile(dirPath string, fileSuffix string, id uint32, cache *lru.Cache[uint64, []byte]) (*Segment, error) {
	fd, err := os.OpenFile(
		SegmentFileName(dirPath, fileSuffix, id),
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		FILE_MODE_PERM,
	)
	if err != nil {
		return nil, err
	}

	offset, err := fd.Seek(io.SeekStart, io.SeekEnd)
	if err != nil {
		panic(fmt.Errorf("seek to the end of segment file %d%s failed: %v", id, fileSuffix, err))
	}

	return &Segment{
		id:               id,
		fd:               fd,
		currentBlockID:   uint32(offset / BLOCK_SIZE),
		currentBlockSize: uint32(offset % BLOCK_SIZE),
		cache:            cache,
		header:           make([]byte, CHUNK_HEADER_SIZE),
		blockPool:        sync.Pool{New: NewBlockAndHeader},
	}, nil
}

func NewBlockAndHeader() interface{} {
	return &BlockAndHeader{
		block:  make([]byte, BLOCK_SIZE),
		header: make([]byte, CHUNK_HEADER_SIZE),
	}
}

func (s *Segment) NewSegmentReader() *SegmentReader {
	return &SegmentReader{
		segment:     s,
		blockID:     0,
		chunkOffset: 0,
	}
}

func (s *Segment) Sync() error {
	if s.closed {
		return nil
	}
	return s.fd.Sync()
}

func (s *Segment) Remove() error {
	if !s.closed {
		s.closed = true
		err := s.fd.Close()
		if err != nil {
			return err
		}
	}
	return os.Remove(s.fd.Name())
}

func (s *Segment) Size() uint64 {
	return uint64(s.currentBlockID)*BLOCK_SIZE + uint64(s.currentBlockSize)
}

func (s *Segment) writeToBuffer(data []byte, chunkBuffer *bytebufferpool.ByteBuffer) (*ChunkPosition, error) {
	startBufferLen := chunkBuffer.Len()
	padding := uint32(0)

	if s.closed {
		return nil, ErrClosed
	}

	// 当前 block 已经放不下 chunk header, 直接填满
	if s.currentBlockSize+CHUNK_HEADER_SIZE >= BLOCK_SIZE {
		if s.currentBlockSize < BLOCK_SIZE {
			p := make([]byte, BLOCK_SIZE-s.currentBlockSize)
			chunkBuffer.B = append(chunkBuffer.B, p...)
			padding += BLOCK_SIZE - s.currentBlockSize

			s.currentBlockID += 1
			s.currentBlockSize = 0
		}
	}

	position := &ChunkPosition{
		SegmentId:   s.id,
		BlockId:     s.currentBlockID,
		ChunkOffset: int64(s.currentBlockSize),
	}

	dataSize := uint32(len(data))

	// 当前 block 可以保存下全部的 data 数据，不需要分区
	if s.currentBlockSize+dataSize+CHUNK_HEADER_SIZE <= BLOCK_SIZE {
		s.appendChunkBuffer(chunkBuffer, data, ChunkTypeFull)
		position.ChunkSize = dataSize + CHUNK_HEADER_SIZE
	} else {
		var (
			// 剩余没有写入 buffer 的 data 大小
			leftSize             = dataSize
			blockCount    uint32 = 0
			currBlockSize        = s.currentBlockSize
		)

		for leftSize > 0 {
			chunkSize := BLOCK_SIZE - currBlockSize - CHUNK_HEADER_SIZE
			if chunkSize > leftSize {
				chunkSize = leftSize
			}

			var chunkType ChunkType
			switch leftSize {
			case dataSize: // First chunk
				chunkType = ChunkTypeFirst
			case chunkSize: // Last chunk
				chunkType = ChunkTypeLast
			default: // Middle chunk
				chunkType = ChunkTypeMiddle
			}

			end := dataSize - leftSize + chunkSize
			if end > dataSize {
				end = dataSize
			}

			s.appendChunkBuffer(chunkBuffer, data[dataSize-leftSize:end], chunkType)

			leftSize = leftSize - chunkSize
			blockCount = blockCount + 1
			currBlockSize = (currBlockSize + chunkSize + CHUNK_HEADER_SIZE) % BLOCK_SIZE
		}
		position.ChunkSize = blockCount*CHUNK_HEADER_SIZE + dataSize
	}

	// the buffer length must be equal to chunkSize+padding length
	endBufferLen := chunkBuffer.Len()
	if position.ChunkSize+padding != uint32(endBufferLen-startBufferLen) {
		panic(fmt.Sprintf("wrong!!! the chunk size %d is not equal to the buffer len %d",
			position.ChunkSize+padding, endBufferLen-startBufferLen))
	}

	// 更新 segment 信息
	s.currentBlockSize += position.ChunkSize
	if s.currentBlockSize >= BLOCK_SIZE {
		s.currentBlockID += s.currentBlockSize / BLOCK_SIZE
		s.currentBlockSize = s.currentBlockSize % BLOCK_SIZE
	}
	return position, nil
}

func (s *Segment) WriteAll(data [][]byte) (positions []*ChunkPosition, err error) {
	if s.closed {
		return nil, ErrClosed
	}

	originBlockID := s.currentBlockID
	originBlockSize := s.currentBlockSize

	chunkBuffer := bytebufferpool.Get()
	chunkBuffer.Reset()
	defer func() {
		if err != nil {
			s.currentBlockID = originBlockID
			s.currentBlockSize = originBlockSize
		}
		bytebufferpool.Put(chunkBuffer)
	}()

	var pos *ChunkPosition
	positions = make([]*ChunkPosition, len(data))
	for i := 0; i < len(positions); i++ {
		pos, err = s.writeToBuffer(data[i], chunkBuffer)
		if err != nil {
			return
		}
		positions[i] = pos
	}
	if err = s.writeChunkBuffer(chunkBuffer); err != nil {
		return
	}
	return
}

func (s *Segment) Write(data []byte) (pos *ChunkPosition, err error) {
	if s.closed {
		return nil, ErrClosed
	}

	originBlockID := s.currentBlockID
	originBlockSize := s.currentBlockSize

	chunkBuffer := bytebufferpool.Get()
	chunkBuffer.Reset()
	defer func() {
		if err != nil {
			s.currentBlockID = originBlockID
			s.currentBlockSize = originBlockSize
		}
		bytebufferpool.Put(chunkBuffer)
	}()

	pos, err = s.writeToBuffer(data, chunkBuffer)
	if err != nil {
		return
	}

	if err = s.writeChunkBuffer(chunkBuffer); err != nil {
		return
	}

	return
}

func (s *Segment) appendChunkBuffer(buffer *bytebufferpool.ByteBuffer, data []byte, chunkType ChunkType) {
	// Length	2 Bytes	index:4-5
	binary.LittleEndian.PutUint16(s.header[4:6], uint16(len(data)))
	// Type	1 Byte	index:6
	s.header[6] = chunkType
	// Checksum	4 Bytes index:0-3
	sum := crc32.ChecksumIEEE(s.header[4:])
	sum = crc32.Update(sum, crc32.IEEETable, data)
	binary.LittleEndian.PutUint32(s.header[:4], sum)

	// append the header and data to segment chunk buffer
	buffer.B = append(buffer.B, s.header...)
	buffer.B = append(buffer.B, data...)
}

func (s *Segment) writeChunkBuffer(buffer *bytebufferpool.ByteBuffer) error {
	if s.currentBlockSize > BLOCK_SIZE {
		panic("wrong! can not exceed the block size")
	}

	_, err := s.fd.Write(buffer.Bytes())
	if err != nil {
		return err
	}
	// log.Printf("write %v into segment file %v", size, s.fd.Name())
	return nil
}

func (s *Segment) Close() error {
	if s.closed {
		return fmt.Errorf("segment file %v is already closed", s.fd.Name())
	}

	s.closed = true
	return s.fd.Close()
}

func (s *Segment) getCacheKey(blockID BlockID) uint64 {
	return (uint64(s.id) << 32) | uint64(blockID)
}

func (s *Segment) readBlockWithCache(blockID BlockID, chunkOffset int64, bh *BlockAndHeader) error {
	offset := int64(blockID) * BLOCK_SIZE
	size := int64(s.Size()) - offset
	if size > BLOCK_SIZE {
		size = BLOCK_SIZE
	}

	if chunkOffset >= size {
		return io.EOF
	}

	if s.cache != nil {
		if cachedBlock, ok := s.cache.Get(s.getCacheKey(blockID)); ok {
			copy(bh.block, cachedBlock)
		}
	}
	_, err := s.fd.ReadAt(bh.block[0:size], offset)
	if err != nil {
		return err
	}

	if s.cache != nil && size == BLOCK_SIZE {
		cacheBlock := make([]byte, BLOCK_SIZE)
		copy(cacheBlock, bh.block)
		// log.Printf("load block id:%v from disk into cache", blockID)
		s.cache.Add(s.getCacheKey(blockID), cacheBlock)
	}

	return nil
}

func (s *Segment) readInternal(blockID uint32, chunkOffset int64) ([]byte, *ChunkPosition, error) {
	if s.closed {
		return nil, nil, ErrClosed
	}

	var (
		result    []byte
		bh        = s.blockPool.Get().(*BlockAndHeader)
		nextChunk = &ChunkPosition{SegmentId: s.id}
	)
	defer func() {
		s.blockPool.Put(bh)
	}()

	for {
		err := s.readBlockWithCache(blockID, chunkOffset, bh)
		if err != nil {
			return nil, nil, err
		}

		// header
		copy(bh.header, bh.block[chunkOffset:chunkOffset+CHUNK_HEADER_SIZE])

		// length
		length := binary.LittleEndian.Uint16(bh.header[4:6])

		// copy data
		start := chunkOffset + CHUNK_HEADER_SIZE
		result = append(result, bh.block[start:start+int64(length)]...)

		// check sum
		checksumEnd := chunkOffset + CHUNK_HEADER_SIZE + int64(length)
		checksum := crc32.ChecksumIEEE(bh.block[chunkOffset+4 : checksumEnd])
		savedSum := binary.LittleEndian.Uint32(bh.header[:4])
		if savedSum != checksum {
			return nil, nil, ErrCRC
		}

		// type
		chunkType := bh.header[6]

		if chunkType == ChunkTypeFull || chunkType == ChunkTypeLast {
			nextChunk.BlockId = blockID
			nextChunk.ChunkOffset = checksumEnd
			// 后面部分是 padding，可以直接跳过
			if checksumEnd+CHUNK_HEADER_SIZE >= BLOCK_SIZE {
				nextChunk.BlockId += 1
				nextChunk.ChunkOffset = 0
			}
			break
		}
		blockID += 1
		chunkOffset = 0
	}
	return result, nextChunk, nil
}

func (s *Segment) Read(blockID BlockID, chunkOffset int64) ([]byte, error) {
	value, _, err := s.readInternal(blockID, chunkOffset)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (r *SegmentReader) Next() ([]byte, *ChunkPosition, error) {
	if r.segment.closed {
		return nil, nil, ErrClosed
	}

	value, nextChunk, err := r.segment.readInternal(r.blockID, r.chunkOffset)
	if err != nil {
		return nil, nil, err
	}

	chunkPosition := &ChunkPosition{
		SegmentId:   r.segment.id,
		BlockId:     r.blockID,
		ChunkOffset: r.chunkOffset,
		ChunkSize:   (nextChunk.BlockId*BLOCK_SIZE + uint32(nextChunk.ChunkOffset)) - (r.blockID*BLOCK_SIZE + uint32(r.chunkOffset)),
	}

	r.blockID = nextChunk.BlockId
	r.chunkOffset = nextChunk.ChunkOffset

	return value, chunkPosition, nil
}

func (cp *ChunkPosition) Encode() []byte {
	return cp.encode(true)
}

func (cp *ChunkPosition) EncodeFixedSize() []byte {
	return cp.encode(false)
}

func (cp *ChunkPosition) encode(shrink bool) []byte {
	buf := make([]byte, MAX_LEN)

	var index = 0

	// SegmentId | BlockId | ChunkOffset | ChunkSize
	index += binary.PutUvarint(buf[index:], uint64(cp.SegmentId))
	index += binary.PutUvarint(buf[index:], uint64(cp.BlockId))
	index += binary.PutUvarint(buf[index:], uint64(cp.ChunkOffset))
	index += binary.PutUvarint(buf[index:], uint64(cp.ChunkSize))

	if shrink {
		return buf[:index]
	}
	return buf
}

func DecodeChunkPosition(buf []byte) *ChunkPosition {
	if len(buf) == 0 {
		return nil
	}

	var index = 0
	// SegmentId
	segmentId, n := binary.Uvarint(buf[index:])
	index += n
	// BlockID
	blockId, n := binary.Uvarint(buf[index:])
	index += n
	// ChunkOffset
	chunkOffset, n := binary.Uvarint(buf[index:])
	index += n
	// ChunkSize
	chunkSize, n := binary.Uvarint(buf[index:])
	index += n

	return &ChunkPosition{
		SegmentId:   uint32(segmentId),
		BlockId:     uint32(blockId),
		ChunkOffset: int64(chunkOffset),
		ChunkSize:   uint32(chunkSize),
	}
}
