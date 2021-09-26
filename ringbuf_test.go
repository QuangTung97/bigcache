package bigcache

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRingBuf_Append(t *testing.T) {
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

func TestRingBuf_Append_WrapAround(t *testing.T) {
	rb := newRingBuf(16)
	rb.append([]byte{20, 21, 22, 23})
	rb.append([]byte{20, 21, 22, 23})
	rb.append([]byte{20, 21, 22, 23})

	rb.skip(8)
	rb.append([]byte{1, 2, 3, 4, 5, 6, 7})
	assert.Equal(t, 3, rb.getEnd())

	rb.append([]byte{10, 11})
	assert.Equal(t, 5, rb.getEnd())
	assert.Equal(t, 3, rb.getAvailable())

	data := make([]byte, 5)
	rb.readAt(data, 0)
	assert.Equal(t, []byte{5, 6, 7, 10, 11}, data)

	data = make([]byte, 7)
	rb.readAt(data, 12)
	assert.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7}, data)
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

func TestRingBuf_Evacuate_End_WrapAround(t *testing.T) {
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

	prevEnd := rb.evacuate(5)
	assert.Equal(t, 2, prevEnd)

	data := make([]byte, 5)
	rb.readAt(data, 2)
	assert.Equal(t, []byte{5, 6, 7, 8, 21}, data)
	assert.Equal(t, 9, rb.getBegin())
	assert.Equal(t, 7, rb.getEnd())
}

func TestRingBuf_Evacuate_Both_From_And_To_Are_Wrap_Around_End_Smaller_Than_Begin(t *testing.T) {
	rb := newRingBuf(16)
	rb.append([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	rb.append([]byte{9, 10, 11, 12, 13, 14, 15, 16})
	rb.skip(13)
	rb.append([]byte{
		51, 52, 53, 54,
		55, 56, 57, 58,
		59, 60, 61,
	})
	rb.evacuate(6)
	data := make([]byte, 6)
	rb.readAt(data, 11)
	assert.Equal(t, []byte{14, 15, 16, 51, 52, 53}, data)
	assert.Equal(t, 1, rb.getEnd())
	assert.Equal(t, 3, rb.getBegin())
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

	prevEnd := rb.evacuate(8)
	assert.Equal(t, 7, prevEnd)
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
