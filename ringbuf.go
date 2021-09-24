package bigcache

import "bytes"

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

func (r *ringBuf) append(data []byte) {
	n := len(data)
	max := len(r.data)
	end := r.getEnd()
	copy(r.data[end:], data)
	if end+n > max {
		copy(r.data, data[end+n-max:])
	}
	r.size += len(data)
}

func (r *ringBuf) appendAlign(data []byte, headerSize int) int {
	max := len(r.data)
	offset := r.getEnd()
	if offset+headerSize > max {
		r.size += max - offset
		offset = 0
	}
	copy(r.data[offset:], data)
	r.size += len(data)
	return offset
}

func (r *ringBuf) readAt(data []byte, offset int) {
	n := len(data)
	max := len(r.data)
	copy(data, r.data[offset:])
	if offset+n > max {
		copy(data[offset+n-max:], r.data[:])
	}
}

func (r *ringBuf) getBegin() int {
	return r.begin
}

func (r *ringBuf) getEnd() int {
	return (r.begin + r.size) % len(r.data)
}

func (r *ringBuf) getAvailable(headerSize int) int {
	end := r.getEnd()
	n := len(r.data)
	if end+headerSize > n {
		return end - r.size
	}
	return n - r.size
}

func (r *ringBuf) skip(n int) {
	r.begin = (r.begin + n) % len(r.data)
	r.size -= n
}

func (r *ringBuf) bytesEqual(from int, data []byte) bool {
	n := len(data)
	offset := from + n
	if offset > len(r.data) {
		firstBytes := len(r.data) - from
		if !bytes.Equal(r.data[from:], data[:firstBytes]) {
			return false
		}
		return bytes.Equal(r.data[:n-firstBytes], data[firstBytes:])
	}
	return bytes.Equal(r.data[from:from+n], data)
}
