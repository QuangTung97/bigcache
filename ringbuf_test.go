package bigcache

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRingBuf(t *testing.T) {
	rb := newRingBuf(16)

	assert.Equal(t, 0, rb.getEnd())
	offset := rb.append([]byte{10, 11, 12, 13})
	assert.Equal(t, 0, offset)
	assert.Equal(t, 4, rb.getEnd())
	assert.Equal(t, 12, rb.getAvailable())

	data := make([]byte, 4)
	rb.readAt(data, 0)

	assert.Equal(t, []byte{10, 11, 12, 13}, data)

	offset = rb.append([]byte{20, 21})
	assert.Equal(t, 4, offset)
	assert.Equal(t, 6, rb.getEnd())
	assert.Equal(t, 10, rb.getAvailable())

	rb.append([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	rb.readAt(data, 6)
	assert.Equal(t, []byte{1, 2, 3, 4}, data)

	assert.Equal(t, 14, rb.getEnd())

	assert.Equal(t, 2, rb.getAvailable())

	rb.skip(4)
	assert.Equal(t, 6, rb.getAvailable())
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
	assert.Equal(t, 2, rb.getAvailable())

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

	result = rb.bytesEqual(13, []byte{31, 32, 8, 10, 11})
	assert.Equal(t, false, result)

	result = rb.bytesEqual(13, []byte{31, 32, 33, 10, 11})
	assert.Equal(t, true, result)
}

func TestRingBuf_Evacuate_Simple(t *testing.T) {
	rb := newRingBuf(16)
	rb.append([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	rb.evacuate(4)

	assert.Equal(t, 4, rb.getBegin())
	assert.Equal(t, 12, rb.getEnd())

	data := make([]byte, 8)
	rb.readAt(data, 4)
	assert.Equal(t, []byte{5, 6, 7, 8, 1, 2, 3, 4}, data)
}

func TestRingBuf_Evacuate_WrapAround(t *testing.T) {
	rb := newRingBuf(16)
	rb.append([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	rb.append([]byte{21, 22, 23, 24, 25, 26})
	rb.evacuate(6)

	assert.Equal(t, 6, rb.getBegin())
	assert.Equal(t, 4, rb.getEnd())

	data := make([]byte, 6)
	rb.readAt(data, 14)
	assert.Equal(t, []byte{1, 2, 3, 4, 5, 6}, data)
}

func TestRingBuf_Evacuate_Backward(t *testing.T) {
	rb := newRingBuf(16)
	rb.append([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	rb.append([]byte{21, 22, 23, 24, 25, 26})
	rb.skip(4)
	rb.append([]byte{31, 32, 33, 34})

	assert.Equal(t, 4, rb.getBegin())
	assert.Equal(t, 2, rb.getEnd())

	rb.evacuate(5)
	data := make([]byte, 5)
	rb.readAt(data, 2)
	assert.Equal(t, []byte{5, 6, 7, 8, 21}, data)
	assert.Equal(t, 9, rb.getBegin())
	assert.Equal(t, 7, rb.getEnd())
}

func TestRingBuf_Evacuate_Begin_Near_Max(t *testing.T) {
	rb := newRingBuf(16)
	rb.append([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	rb.append([]byte{21, 22, 23, 24, 25})

	rb.evacuate(10)
	assert.Equal(t, 10, rb.getBegin())
	assert.Equal(t, 7, rb.getEnd())

	data := make([]byte, 13)
	rb.readAt(data, 10)
	assert.Equal(t, []byte{23, 24, 25, 1, 2, 3, 4, 5, 6, 7, 8, 21, 22}, data)

	rb.evacuate(8)
	assert.Equal(t, 2, rb.getBegin())
	assert.Equal(t, 15, rb.getEnd())

	data = make([]byte, 13)
	rb.readAt(data, 2)
	assert.Equal(t, []byte{6, 7, 8, 21, 22, 23, 24, 25, 1, 2, 3, 4, 5}, data)
}

func TestRingBuf_WriteAt_WrapAround(t *testing.T) {
	rb := newRingBuf(7)

	rb.writeAt([]byte{10, 11, 12, 13, 14}, 5)

	assert.Equal(t, []byte{12, 13, 14, 0, 0, 10, 11}, rb.data)

	data := make([]byte, 5)
	rb.readAt(data, 5)
	assert.Equal(t, []byte{10, 11, 12, 13, 14}, data)
}
