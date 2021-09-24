package bigcache

import (
	"bytes"
)

type ringBuf struct {
	begin int
	size  int
	data  []byte
}

func newRingBuf(size int) ringBuf {
	return ringBuf{
		begin: 0,
		size:  0,
		data:  make([]byte, size),
	}
}

func (r *ringBuf) append(data []byte) int {
	n := len(data)
	max := len(r.data)
	end := r.getEnd()
	copy(r.data[end:], data)
	if end+n > max {
		copy(r.data, data[end+n-max:])
	}
	r.size += len(data)
	return end
}

func (r *ringBuf) appendEmpty(n int) {
	r.size += n
}

func (r *ringBuf) readAt(data []byte, offset int) {
	n := len(data)
	max := len(r.data)
	copy(data, r.data[offset:])
	if offset+n > max {
		copy(data[max-offset:], r.data[:])
	}
}

func (r *ringBuf) getBegin() int {
	return r.begin
}

func (r *ringBuf) getEnd() int {
	return (r.begin + r.size) % len(r.data)
}

func (r *ringBuf) getAvailable() int {
	return len(r.data) - r.size
}

func (r *ringBuf) increaseBegin(n int) {
	r.begin = (r.begin + n) % len(r.data)
}

func (r *ringBuf) skip(n int) {
	r.increaseBegin(n)
	r.size -= n
}

func (r *ringBuf) bytesEqual(from int, data []byte) bool {
	n := len(data)
	toOffset := from + n
	max := len(r.data)
	if toOffset > max {
		firstPart := max - from
		secondPart := n - firstPart
		if !bytes.Equal(r.data[from:], data[:firstPart]) {
			return false
		}
		return bytes.Equal(r.data[:secondPart], data[firstPart:])
	}
	return bytes.Equal(r.data[from:toOffset], data)
}

func (r *ringBuf) evacuate(size int) {
	begin := r.getBegin()
	end := r.getEnd()
	max := len(r.data)

	if end+size > max {
		firstPart := max - end
		secondPart := size - firstPart
		copy(r.data[end:], r.data[begin:])
		copy(r.data[:secondPart], r.data[begin+firstPart:])
	} else if begin+size > max {
		firstPart := max - begin
		secondPart := size - firstPart
		copy(r.data[end:], r.data[begin:])
		copy(r.data[end+firstPart:], r.data[:secondPart])
	} else {
		copy(r.data[end:end+size], r.data[begin:])
	}

	r.increaseBegin(size)
}
