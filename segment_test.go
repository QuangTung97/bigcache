package bigcache

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"unsafe"
)

func TestEntryHeaderAlign(t *testing.T) {
	assert.Equal(t, 20, entryHeaderSize)
	assert.Equal(t, 4, entryHeaderAlign)
}

func newSegment() *segment {
	s := &segment{}
	initSegment(s, 1024)
	return s
}

func (s *segment) getHeader(hash uint32) *entryHeader {
	offset := s.kv[hash]
	var headerData [entryHeaderSize]byte
	s.rb.readAt(headerData[:], offset)
	return (*entryHeader)(unsafe.Pointer(&headerData[0]))
}

func TestNextNumberAlignToHeader(t *testing.T) {
	result := nextNumberAlignToHeader(7)
	assert.Equal(t, uint32(8), result)

	result = nextNumberAlignToHeader(12)
	assert.Equal(t, uint32(12), result)
}

func TestSegment_Simple_Set_Get(t *testing.T) {
	s := newSegment()
	s.put(40, []byte{1, 2, 3}, []byte{10, 11, 12, 13})

	data := make([]byte, 10)
	n, ok := s.get(40, []byte{1, 2, 3}, data)
	assert.Equal(t, true, ok)
	assert.Equal(t, 4, n)
	assert.Equal(t, []byte{10, 11, 12, 13}, data[:n])

	assert.Equal(t, 0, s.rb.getBegin())
	assert.Equal(t, entryHeaderSize+8, s.rb.getEnd())
	assert.Equal(t, 1024-entryHeaderSize-8, s.rb.getAvailable())
}

func TestSegment_Set_Get_Not_Equal_Hash(t *testing.T) {
	s := newSegment()
	s.put(40, []byte{1, 2, 3}, []byte{10, 11, 12, 13})

	n, ok := s.get(50, []byte{1, 2, 3}, nil)
	assert.Equal(t, false, ok)
	assert.Equal(t, 0, n)
}

func TestSegment_Set_Get_Key_Not_Equal_Length(t *testing.T) {
	s := newSegment()
	s.put(40, []byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{10, 11, 12, 13})

	n, ok := s.get(40, []byte{1, 2, 3}, nil)
	assert.Equal(t, false, ok)
	assert.Equal(t, 0, n)
}

func TestSegment_Set_Get_Not_Equal_Key(t *testing.T) {
	s := newSegment()
	s.put(40, []byte{1, 2, 3}, []byte{10, 11, 12, 13})

	n, ok := s.get(40, []byte{1, 2, 4}, nil)
	assert.Equal(t, false, ok)
	assert.Equal(t, 0, n)
}

func TestSegment_Get_Access_Time(t *testing.T) {
	s := newSegment()
	s.getNow = func() uint32 { return 120 }
	s.put(40, []byte{1, 2, 3}, []byte{10, 11, 12, 13})
	header := s.getHeader(40)
	assert.Equal(t, &entryHeader{
		hash:       40,
		accessTime: 120,
		keyLen:     3,
		deleted:    false,
		valLen:     4,
		valCap:     5,
	}, header)
}

func TestSegment_Get_Update_Access_Time(t *testing.T) {
	s := newSegment()
	s.getNow = func() uint32 { return 120 }

	s.put(40, []byte{1, 2, 3}, []byte{10, 11, 12, 13})

	s.getNow = func() uint32 { return 140 }
	data := make([]byte, 4)
	s.get(40, []byte{1, 2, 3}, data)

	header := s.getHeader(40)
	assert.Equal(t, &entryHeader{
		hash:       40,
		accessTime: 140,
		keyLen:     3,
		deleted:    false,
		valLen:     4,
		valCap:     5,
	}, header)
}
