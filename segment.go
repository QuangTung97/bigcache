package bigcache

import (
	"github.com/QuangTung97/bigcache/memhash"
	"sync"
	"sync/atomic"
	"unsafe"
)

type segment struct {
	mu     sync.Mutex
	rb     ringBuf
	kv     map[uint32]int
	total  uint64
	getNow func() uint32
}

type entryHeader struct {
	hash       uint32
	accessTime uint32
	keyLen     uint16
	deleted    bool
	valLen     uint32
	valCap     uint32
}

const entryHeaderSize = int(unsafe.Sizeof(entryHeader{}))
const entryHeaderAlign = int(unsafe.Alignof(entryHeader{}))
const entryHeaderAlignMask = ^uint32(entryHeaderAlign - 1)

func initSegment(s *segment, bufSize int) {
	s.rb = newRingBuf(bufSize)
	s.kv = map[uint32]int{}
	s.getNow = getNowMono
}

func getNowMono() uint32 {
	return uint32(memhash.NanoTime() / 1000000000)
}

func (s *segment) put(hash uint32, key []byte, value []byte) {
	var headerData [entryHeaderSize]byte
	offset, existed := s.kv[hash]
	if existed {
		s.rb.readAt(headerData[:], offset)
		header := (*entryHeader)(unsafe.Pointer(&headerData[0]))
		if s.keyEqual(header, offset, key) {
			if len(value) <= int(header.valCap) {
				s.rb.writeAt(value, offset+entryHeaderSize+int(header.keyLen))
				header.valLen = uint32(len(value))
				header.accessTime = s.getNow()
				s.rb.writeAt(headerData[:], offset)
				return
			}
		}
		header.deleted = true
		s.rb.writeAt(headerData[:], offset)
	}

	keyLen := uint16(len(key))
	valLen := uint32(len(value))
	totalLen := uint32(keyLen) + valLen
	totalLenAligned := nextNumberAlignToHeader(totalLen)

	totalSize := entryHeaderSize + int(totalLenAligned)
	s.evacuate(totalSize)

	header := (*entryHeader)(unsafe.Pointer(&headerData[0]))
	header.hash = hash
	header.accessTime = s.getNow()
	header.keyLen = keyLen
	header.deleted = false
	header.valLen = valLen
	header.valCap = totalLenAligned - uint32(keyLen)

	offset = s.rb.append(headerData[:])
	s.rb.append(key)
	s.rb.append(value)
	s.rb.appendEmpty(int(header.valCap - header.valLen))
	s.kv[hash] = offset

	if !existed {
		atomic.AddUint64(&s.total, 1)
	}
}

func (s *segment) evacuate(expectedSize int) {
	var headerData [entryHeaderSize]byte
	for s.rb.getAvailable() < expectedSize {
		offset := s.rb.getBegin()
		s.rb.readAt(headerData[:], offset)
		header := (*entryHeader)(unsafe.Pointer(&headerData[0]))
		header.deleted = true
		s.rb.writeAt(headerData[:], offset)
		size := entryHeaderSize + int(header.keyLen) + int(header.valCap)
		s.rb.skip(size)
		delete(s.kv, header.hash)
		atomic.AddUint64(&s.total, ^uint64(0))
	}
}

func (s *segment) get(hash uint32, key []byte, value []byte) (n int, ok bool) {
	offset, ok := s.kv[hash]
	if !ok {
		return 0, false
	}

	var headerData [entryHeaderSize]byte
	s.rb.readAt(headerData[:], offset)
	header := (*entryHeader)(unsafe.Pointer(&headerData[0]))
	if !s.keyEqual(header, offset, key) {
		return 0, false
	}

	s.rb.readAt(value[:header.valLen], offset+entryHeaderSize+int(header.keyLen))

	header.accessTime = s.getNow()
	s.rb.writeAt(headerData[:], offset)
	return int(header.valLen), true
}

func (s *segment) keyEqual(header *entryHeader, offset int, key []byte) bool {
	if int(header.keyLen) != len(key) {
		return false
	}
	if ok := s.rb.bytesEqual(offset+entryHeaderSize, key); !ok {
		return false
	}
	return true
}

func (s *segment) getTotal() uint64 {
	return atomic.LoadUint64(&s.total)
}

func nextNumberAlignToHeader(n uint32) uint32 {
	return (n + uint32(entryHeaderAlign) - 1) & entryHeaderAlignMask
}
