package bigcache

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRingBuf(t *testing.T) {
	rb := newRingBuf(16)

	assert.Equal(t, 0, rb.getEnd())
	rb.append([]byte{10, 11, 12, 13})
	assert.Equal(t, 4, rb.getEnd())
	assert.Equal(t, 12, rb.getAvailable(0))

	data := make([]byte, 4)
	rb.readAt(data, 0)

	assert.Equal(t, []byte{10, 11, 12, 13}, data)

	rb.append([]byte{20, 21})
	assert.Equal(t, 6, rb.getEnd())
	assert.Equal(t, 10, rb.getAvailable(0))

	rb.append([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	rb.readAt(data, 6)
	assert.Equal(t, []byte{1, 2, 3, 4}, data)

	assert.Equal(t, 14, rb.getEnd())

	assert.Equal(t, 2, rb.getAvailable(0))
	assert.Equal(t, 2, rb.getAvailable(2))
	assert.Equal(t, 0, rb.getAvailable(3))

	rb.skip(4)
	assert.Equal(t, 6, rb.getAvailable(0))
	assert.Equal(t, 4, rb.getAvailable(4))
}

func TestRingBuf_WrapAround(t *testing.T) {
	rb := newRingBuf(16)
	rb.append([]byte{20, 21, 22, 23})
	rb.append([]byte{20, 21, 22, 23})
	rb.append([]byte{20, 21, 22, 23})

	rb.skip(8)
	rb.append([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	assert.Equal(t, 4, rb.getEnd())

	rb.append([]byte{10, 11})
	assert.Equal(t, 6, rb.getEnd())
	assert.Equal(t, 2, rb.getAvailable(0))

	data := make([]byte, 6)
	rb.readAt(data, 0)
	assert.Equal(t, []byte{5, 6, 7, 8, 10, 11}, data)

	data = make([]byte, 8)
	rb.readAt(data, 12)
	assert.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7, 8}, data)
}

func TestRingBuf_Skip_Wrap_Around(t *testing.T) {
	rb := newRingBuf(16)
	rb.append([]byte{20, 21, 22, 23})
	rb.append([]byte{20, 21, 22, 23})
	rb.append([]byte{20, 21, 22, 23})
	rb.skip(4)
	rb.skip(4)
	rb.append([]byte{20, 21, 22, 23})

	rb.skip(4)
	rb.skip(4)
	rb.skip(4)
	rb.append([]byte{20, 21, 22, 23})
	rb.append([]byte{20, 21, 22, 23})
	rb.append([]byte{20, 21, 22, 23})

	assert.Equal(t, 4, rb.getBegin())
	assert.Equal(t, 12, rb.getEnd())
}

func TestRingBuf_Append_Align(t *testing.T) {
	rb := newRingBuf(16)

	begin := rb.appendAlign([]byte{10, 11, 12, 13}, 4)
	assert.Equal(t, 0, begin)

	begin = rb.appendAlign([]byte{10, 11, 12, 13}, 4)
	assert.Equal(t, 4, begin)

	begin = rb.appendAlign([]byte{1, 2, 3, 4, 5, 6}, 4)
	assert.Equal(t, 8, begin)

	rb.skip(4)
	begin = rb.appendAlign([]byte{30, 31, 32, 33}, 4)
	assert.Equal(t, 0, begin)

	assert.Equal(t, 0, rb.getAvailable(4))
}

func TestRingBuf_BytesEqual(t *testing.T) {
	rb := newRingBuf(16)
	rb.append([]byte{10, 11, 12, 13})

	result := rb.bytesEqual(0, []byte{1, 2})
	assert.Equal(t, false, result)

	result = rb.bytesEqual(0, []byte{10, 11})
	assert.Equal(t, true, result)

	rb.append([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	rb.append([]byte{30, 31, 32, 33})

	// Wrap Around

	result = rb.bytesEqual(13, []byte{9, 10, 11, 50, 51})
	assert.Equal(t, false, result)

	result = rb.bytesEqual(13, []byte{31, 32, 33, 50, 51})
	assert.Equal(t, false, result)

	result = rb.bytesEqual(13, []byte{31, 32, 33, 10, 11})
	assert.Equal(t, true, result)
}
