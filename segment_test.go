package bigcache

import (
	"github.com/stretchr/testify/assert"
	"testing"
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
