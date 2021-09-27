package bigcache

import (
	"github.com/QuangTung97/bigcache/memhash"
	"math/bits"
)

// Cache ...
type Cache struct {
	segments []segment

	segmentMask  uint64
	segmentShift int
}

// New ...
func New(numSegments int, segmentSize int) *Cache {
	if numSegments < 1 {
		panic("numSegments must not be < 1")
	}
	numSegments = nextPowerOfTwo(numSegments)

	segments := make([]segment, numSegments)
	for i := range segments {
		initSegment(&segments[i], segmentSize)
	}

	mask, shift := computeSegmentMask(numSegments)
	return &Cache{
		segments:     segments,
		segmentMask:  mask,
		segmentShift: shift,
	}
}

func (c *Cache) getSegment(key []byte) (*segment, uint64) {
	hash := memhash.Hash(key)
	index := getSegmentIndex(c.segmentMask, c.segmentShift, hash)
	return &c.segments[index], hash
}

// Put ...
func (c *Cache) Put(key []byte, value []byte) {
	seg, hash := c.getSegment(key)

	seg.mu.Lock()
	seg.put(uint32(hash), key, value)
	seg.mu.Unlock()
}

// Get ...
func (c *Cache) Get(key []byte, value []byte) (int, bool) {
	seg, hash := c.getSegment(key)

	seg.mu.Lock()
	n, ok := seg.get(uint32(hash), key, value)
	seg.mu.Unlock()

	return n, ok
}

// Delete ...
func (c *Cache) Delete(key []byte) bool {
	seg, hash := c.getSegment(key)

	seg.mu.Lock()
	affected := seg.delete(uint32(hash), key)
	seg.mu.Unlock()

	return affected
}

// GetHitCount ...
func (c *Cache) GetHitCount() uint64 {
	count := uint64(0)
	for i := range c.segments {
		count += c.segments[i].getHitCount()
	}
	return count
}

// GetAccessCount ...
func (c *Cache) GetAccessCount() uint64 {
	count := uint64(0)
	for i := range c.segments {
		count += c.segments[i].getAccessCount()
	}
	return count
}

func nextPowerOfTwo(n int) int {
	num := uint32(n)
	return 1 << bits.Len32(num-1)
}

func computeSegmentMask(n int) (uint64, int) {
	shift := 64 - bits.Len64(uint64(n-1))
	return ^uint64(0) << shift, shift
}

func getSegmentIndex(mask uint64, shift int, hash uint64) int {
	return int((hash & mask) >> shift)
}
