package bigcache

import (
	"github.com/QuangTung97/bigcache/memhash"
	"sync"
	"unsafe"
)

type segment struct {
	mu sync.Mutex
	rb ringBuf
	kv map[uint32]int
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
}

func getNow() uint32 {
	return uint32(memhash.NanoTime() / 1000000000)
}

func (s *segment) put(hash uint32, key []byte, value []byte) {
	var headerData [entryHeaderSize]byte
	header := (*entryHeader)(unsafe.Pointer(&headerData))
	header.hash = hash
	header.accessTime = getNow()
	header.keyLen = uint16(len(key))
	header.deleted = false
	header.valLen = uint32(len(value))
	header.valCap = header.valLen

	keyLen := uint32(header.keyLen)
	totalLen := keyLen + header.valLen
	header.valCap = nextNumberAlignToHeader(totalLen) - keyLen

	offset := s.rb.append(headerData[:])
	s.rb.append(key)
	s.rb.append(value)
	s.rb.appendEmpty(int(header.valCap - header.valLen))
	s.kv[hash] = offset
}

func (s *segment) get(hash uint32, key []byte, value []byte) (n int, ok bool) {
	offset, ok := s.kv[hash]
	if !ok {
		return 0, false
	}

	var headerData [entryHeaderSize]byte
	s.rb.readAt(headerData[:], offset)
	header := (*entryHeader)(unsafe.Pointer(&headerData))
	if int(header.keyLen) != len(key) {
		return 0, false
	}
	if ok := s.rb.bytesEqual(offset+entryHeaderSize, key); !ok {
		return 0, false
	}

	s.rb.readAt(value[:header.valLen], offset+entryHeaderSize+int(header.keyLen))
	return int(header.valLen), true
}

func nextNumberAlignToHeader(n uint32) uint32 {
	return (n + uint32(entryHeaderAlign) - 1) & entryHeaderAlignMask
}
