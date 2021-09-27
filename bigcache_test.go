package bigcache

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNextPowerOfTwo(t *testing.T) {
	assert.Equal(t, 1, nextPowerOfTwo(1))
	assert.Equal(t, 2, nextPowerOfTwo(2))
	assert.Equal(t, 4, nextPowerOfTwo(3))
	assert.Equal(t, 16, nextPowerOfTwo(15))
	assert.Equal(t, 16, nextPowerOfTwo(12))
	assert.Equal(t, 16, nextPowerOfTwo(16))
	assert.Equal(t, 64, nextPowerOfTwo(59))
}

func TestGetSegmentIndex(t *testing.T) {
	mask, shift := computeSegmentMask(1)
	assert.Equal(t, uint64(0x0), mask)
	assert.Equal(t, 64, shift)

	mask, shift = computeSegmentMask(16)
	assert.Equal(t, uint64(0xf<<60), mask)
	assert.Equal(t, 60, shift)

	assert.Equal(t, 0, getSegmentIndex(0xf<<60, 60, 0x0033445500000000))
	assert.Equal(t, 2, getSegmentIndex(0xf<<60, 60, 0x2033445500000000))

	mask, shift = computeSegmentMask(64)
	assert.Equal(t, uint64(0xfc<<56), mask)
	assert.Equal(t, 58, shift)

	assert.Equal(t, 0b010110, getSegmentIndex(mask, shift, 0x5933445500000000))
}

func TestCache(t *testing.T) {
	c := New(3, 12345)

	c.Put([]byte{10, 11, 12}, []byte{20, 21, 22, 20})
	c.Put([]byte{10, 11, 13}, []byte{20, 21, 22, 21})
	c.Put([]byte{10, 11, 14}, []byte{20, 21, 22, 22})

	value := make([]byte, 20)

	n, ok := c.Get([]byte{10, 11, 12}, value)
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte{20, 21, 22, 20}, value[:n])

	n, ok = c.Get([]byte{10, 11, 13}, value)
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte{20, 21, 22, 21}, value[:n])

	n, ok = c.Get([]byte{10, 11, 15}, value)
	assert.Equal(t, false, ok)
	assert.Equal(t, []byte{}, value[:n])

	assert.Equal(t, uint64(3), c.GetTotal())
	assert.Equal(t, uint64(2), c.GetHitCount())

	ok = c.Delete([]byte{10, 11, 12})
	assert.Equal(t, true, ok)

	ok = c.Delete([]byte{10, 11, 12})
	assert.Equal(t, false, ok)

	n, ok = c.Get([]byte{10, 11, 12}, value)
	assert.Equal(t, false, ok)
	assert.Equal(t, []byte{}, value[:n])

	assert.Equal(t, uint64(2), c.GetTotal())
	assert.Equal(t, uint64(2), c.GetHitCount())
	assert.Equal(t, uint64(4), c.GetAccessCount())
}

func TestCache_New_Panic(t *testing.T) {
	assert.PanicsWithValue(t, "numSegments must not be < 1", func() {
		New(0, 12345)
	})
}
