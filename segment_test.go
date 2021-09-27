package bigcache

import (
	"fmt"
	"github.com/QuangTung97/bigcache/memhash"
	"github.com/stretchr/testify/assert"
	"math/rand"
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

func newSegmentSize(bufSize int) *segment {
	s := &segment{}
	initSegment(s, bufSize)
	return s
}

func (s *segment) getHeader(hash uint32) *entryHeader {
	offset := s.kv[hash]
	var headerData [entryHeaderSize]byte
	s.rb.readAt(headerData[:], offset)
	return (*entryHeader)(unsafe.Pointer(&headerData[0]))
}

func (s *segment) getSumTotalAccessTime() uint64 {
	totalAccess := uint64(0)
	for _, offset := range s.kv {
		header := s.getHeaderAtOffset(offset)
		totalAccess += uint64(header.accessTime)
	}
	return totalAccess
}

func (s *segment) getHeaderAtOffset(offset int) *entryHeader {
	var headerData [entryHeaderSize]byte
	s.rb.readAt(headerData[:], offset)
	return (*entryHeader)(unsafe.Pointer(&headerData[0]))
}

func monoGetNow(start uint32) func() uint32 {
	now := start
	return func() uint32 {
		now++
		return now
	}
}

func TestNextNumberAlignToHeader(t *testing.T) {
	result := nextNumberAlignToHeader(7)
	assert.Equal(t, uint32(8), result)

	result = nextNumberAlignToHeader(12)
	assert.Equal(t, uint32(12), result)
}

func TestSegmentSizeAlignToCacheLine(t *testing.T) {
	assert.Equal(t, 64*2, int(unsafe.Sizeof(segment{})))
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

	assert.Equal(t, uint64(1), s.getAccessCount())
	assert.Equal(t, uint64(1), s.getHitCount())
}

func TestSegment_Set_Get_Not_Equal_Hash(t *testing.T) {
	s := newSegment()
	s.put(40, []byte{1, 2, 3}, []byte{10, 11, 12, 13})

	n, ok := s.get(50, []byte{1, 2, 3}, nil)
	assert.Equal(t, false, ok)
	assert.Equal(t, 0, n)

	assert.Equal(t, uint64(1), s.getAccessCount())
	assert.Equal(t, uint64(0), s.getHitCount())
}

func TestSegment_Set_Get_Key_Not_Equal_Length(t *testing.T) {
	s := newSegment()
	s.put(40, []byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{10, 11, 12, 13})

	assert.Equal(t, uint64(0), s.getAccessCount())

	n, ok := s.get(40, []byte{1, 2, 3}, nil)
	assert.Equal(t, false, ok)
	assert.Equal(t, 0, n)

	assert.Equal(t, uint64(1), s.getAccessCount())
	assert.Equal(t, uint64(0), s.getHitCount())
}

func TestSegment_Set_Get_Not_Equal_Key(t *testing.T) {
	s := newSegment()
	s.put(40, []byte{1, 2, 3}, []byte{10, 11, 12, 13})

	n, ok := s.get(40, []byte{1, 2, 4}, nil)
	assert.Equal(t, false, ok)
	assert.Equal(t, 0, n)

	assert.Equal(t, uint64(1), s.getAccessCount())
	assert.Equal(t, uint64(0), s.getHitCount())
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

func TestSegment_Put_With_Exist_Key_Same_Length(t *testing.T) {
	s := newSegment()
	s.put(40, []byte{1, 2, 3}, []byte{10, 11, 12, 13})
	prevAvail := s.rb.getAvailable()
	s.put(40, []byte{1, 2, 3}, []byte{20, 21, 22, 23})

	assert.Equal(t, 1, len(s.kv))
	assert.Equal(t, uint64(1), s.getTotal())
	assert.Equal(t, entryHeaderSize+8, s.rb.getEnd())
	assert.Equal(t, prevAvail, s.rb.getAvailable())

	data := make([]byte, 100)
	n, ok := s.get(40, []byte{1, 2, 3}, data)
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte{20, 21, 22, 23}, data[:n])
}

func TestSegment_Put_With_Exist_Key_Same_Length_Different_Length_Still_In_Cap(t *testing.T) {
	s := newSegment()
	s.getNow = func() uint32 { return 100 }
	s.put(40, []byte{1, 2, 3}, []byte{10, 11, 12, 13})

	prevAvail := s.rb.getAvailable()

	s.getNow = func() uint32 { return 110 }
	s.put(40, []byte{1, 2, 3}, []byte{20, 21, 22, 23, 24})

	assert.Equal(t, 1, len(s.kv))
	assert.Equal(t, uint64(1), s.getTotal())
	assert.Equal(t, entryHeaderSize+8, s.rb.getEnd())
	assert.Equal(t, prevAvail, s.rb.getAvailable())

	header := s.getHeader(40)
	assert.Equal(t, &entryHeader{
		hash:       40,
		accessTime: 110,
		keyLen:     3,
		valLen:     5,
		valCap:     5,
	}, header)

	data := make([]byte, 100)
	n, ok := s.get(40, []byte{1, 2, 3}, data)
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte{20, 21, 22, 23, 24}, data[:n])
}

func TestSegment_Put_With_Exist_Key_Not_In_Cap(t *testing.T) {
	s := newSegment()

	s.put(40, []byte{1, 2, 3}, []byte{10, 11, 12, 13})

	prevAvail := s.rb.getAvailable()
	s.put(40, []byte{1, 2, 3}, []byte{20, 21, 22, 23, 24, 25})

	assert.Equal(t, 1, len(s.kv))
	assert.Equal(t, uint64(1), s.getTotal())
	assert.Equal(t, entryHeaderSize*2+8+12, s.rb.getEnd())
	assert.Equal(t, prevAvail-entryHeaderSize-12, s.rb.getAvailable())

	data := make([]byte, 100)
	n, ok := s.get(40, []byte{1, 2, 3}, data)
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte{20, 21, 22, 23, 24, 25}, data[:n])

	header := s.getHeaderAtOffset(0)
	assert.Equal(t, true, header.deleted)
}

func TestSegment_Put_Same_Hash_Diff_Key(t *testing.T) {
	s := newSegment()
	s.put(40, []byte{1, 2, 3}, []byte{10, 11, 12})
	s.put(40, []byte{5, 6, 7, 8, 9}, []byte{20, 21, 22, 23})

	header := s.getHeaderAtOffset(0)
	assert.Equal(t, true, header.deleted)

	data := make([]byte, 100)
	n, ok := s.get(40, []byte{1, 2, 3}, data)
	assert.Equal(t, false, ok)
	assert.Equal(t, 0, n)

	data = make([]byte, 100)
	n, ok = s.get(40, []byte{5, 6, 7, 8, 9}, data)
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte{20, 21, 22, 23}, data[:n])
}

func TestSegment_Put_Evacuate(t *testing.T) {
	s := newSegmentSize(entryHeaderSize*3 + 8 + 12 + 8)
	s.getNow = monoGetNow(0)

	s.put(40, []byte{1, 2, 3}, []byte{10, 11, 12})
	s.put(41, []byte{5, 6, 7}, []byte{20, 21, 22, 23, 24, 25})
	s.put(42, []byte{8, 9, 0}, []byte{30, 31})
	s.put(43, []byte{100, 101, 102}, []byte{40, 41, 42})

	assert.Equal(t, 3, len(s.kv))
	assert.Equal(t, uint64(3), s.getTotal())
	assert.Equal(t, entryHeaderSize+8, s.rb.getBegin())

	data := make([]byte, 100)
	n, ok := s.get(40, []byte{1, 2, 3}, data)
	assert.Equal(t, false, ok)
	assert.Equal(t, 0, n)

	assert.Equal(t, s.totalAccessTime, s.getSumTotalAccessTime())
}

func TestSegment_Put_Evacuate_Skip_Recent_Used(t *testing.T) {
	const entrySize = entryHeaderSize + 8
	s := newSegmentSize(entrySize * 5)
	s.getNow = monoGetNow(0)

	s.put(40, []byte{1, 2, 0}, []byte{101, 102, 103, 100})
	s.put(41, []byte{1, 2, 1}, []byte{101, 102, 103, 101})
	s.put(42, []byte{1, 2, 2}, []byte{101, 102, 103, 102})
	s.put(43, []byte{1, 2, 3}, []byte{101, 102, 103, 103})
	s.put(44, []byte{1, 2, 4}, []byte{101, 102, 103, 104})

	assert.Equal(t, 0, s.rb.getAvailable())
	assert.Equal(t, uint64(5), s.getTotal())

	data := make([]byte, 100)

	s.get(40, []byte{1, 2, 0}, data)
	s.put(45, []byte{1, 2, 5}, []byte{101, 102, 103, 105})

	assert.Equal(t, uint64(5), s.getTotal())

	data = make([]byte, 100)
	n, ok := s.get(40, []byte{1, 2, 0}, data)
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte{101, 102, 103, 100}, data[:n])

	_, ok = s.get(41, []byte{1, 2, 1}, data)
	assert.Equal(t, false, ok)

	_, ok = s.get(42, []byte{1, 2, 2}, data)
	assert.Equal(t, true, ok)

	assert.Equal(t, s.totalAccessTime, s.getSumTotalAccessTime())
}

func TestSegment_Put_Evacuate_Reach_Max_Evacuation(t *testing.T) {
	const entrySize = entryHeaderSize + 8
	s := newSegmentSize(entrySize * 12)
	s.getNow = monoGetNow(0)

	s.put(40, []byte{1, 2, 0}, []byte{101, 102, 103, 100})
	s.put(41, []byte{1, 2, 1}, []byte{101, 102, 103, 101})
	s.put(42, []byte{1, 2, 2}, []byte{101, 102, 103, 102})
	s.put(43, []byte{1, 2, 3}, []byte{101, 102, 103, 103})

	s.put(44, []byte{1, 2, 4}, []byte{101, 102, 103, 104})
	s.put(45, []byte{1, 2, 5}, []byte{101, 102, 103, 105})
	s.put(46, []byte{1, 2, 6}, []byte{101, 102, 103, 106})
	s.put(47, []byte{1, 2, 7}, []byte{101, 102, 103, 107})

	s.put(48, []byte{1, 2, 8}, []byte{101, 102, 103, 108})
	s.put(49, []byte{1, 2, 9}, []byte{101, 102, 103, 109})
	s.put(50, []byte{1, 2, 10}, []byte{101, 102, 103, 110})
	s.put(51, []byte{1, 2, 11}, []byte{101, 102, 103, 111})

	data := make([]byte, 100)
	s.get(40, []byte{1, 2, 0}, data)
	s.get(41, []byte{1, 2, 1}, data)
	s.get(42, []byte{1, 2, 2}, data)
	s.get(43, []byte{1, 2, 3}, data)

	s.get(44, []byte{1, 2, 4}, data)
	s.get(45, []byte{1, 2, 5}, data)

	s.put(52, []byte{1, 2, 12}, []byte{101, 102, 103, 112})

	data = make([]byte, 100)
	n, ok := s.get(45, []byte{1, 2, 5}, data)
	assert.Equal(t, false, ok)
	assert.Equal(t, []byte{}, data[:n])

	data = make([]byte, 100)
	n, ok = s.get(46, []byte{1, 2, 6}, data)
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte{101, 102, 103, 106}, data[:n])

	assert.Equal(t, s.totalAccessTime, s.getSumTotalAccessTime())
}

func TestSegment_Put_Existing_Check_Total_Access_Time(t *testing.T) {
	s := newSegment()
	s.getNow = monoGetNow(200)

	s.put(40, []byte{1, 2, 3}, []byte{101, 102, 103, 104})
	s.put(40, []byte{1, 2, 3}, []byte{101, 102, 103, 0})

	assert.Equal(t, s.totalAccessTime, s.getSumTotalAccessTime())
}

func TestSegment_Put_Same_Hash_Diff_Key_Check_Total_Access_Time(t *testing.T) {
	s := newSegment()
	s.getNow = monoGetNow(200)

	s.put(40, []byte{1, 2, 3}, []byte{101, 102, 103, 104})
	s.put(40, []byte{1, 2, 4}, []byte{101, 102, 103, 0})

	assert.Equal(t, uint64(1), s.getTotal())
	assert.Equal(t, s.totalAccessTime, s.getSumTotalAccessTime())
}

func TestSegment_Delete_Simple(t *testing.T) {
	s := newSegment()
	s.getNow = monoGetNow(200)

	s.put(40, []byte{1, 2, 3}, []byte{101, 102, 103, 104})
	affected := s.delete(40, []byte{1, 2, 3})
	assert.Equal(t, true, affected)

	assert.Equal(t, uint64(0), s.getTotal())

	data := make([]byte, 100)
	n, ok := s.get(40, []byte{1, 2, 3}, data)
	assert.Equal(t, false, ok)
	assert.Equal(t, 0, n)
	assert.Equal(t, s.totalAccessTime, s.getSumTotalAccessTime())
}

func TestSegment_Delete_Different_Hash(t *testing.T) {
	s := newSegment()
	s.getNow = monoGetNow(200)

	s.put(40, []byte{1, 2, 3}, []byte{101, 102, 103, 104})
	s.put(41, []byte{1, 2, 4}, []byte{101, 102, 103, 105})

	affected := s.delete(42, []byte{1, 2, 3})
	assert.Equal(t, false, affected)

	assert.Equal(t, uint64(2), s.getTotal())

	data := make([]byte, 100)
	n, ok := s.get(40, []byte{1, 2, 3}, data)
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte{101, 102, 103, 104}, data[:n])
	assert.Equal(t, s.totalAccessTime, s.getSumTotalAccessTime())
}

func TestSegment_Delete_Same_Hash_Diff_Key(t *testing.T) {
	s := newSegment()
	s.getNow = monoGetNow(200)

	s.put(40, []byte{1, 2, 3}, []byte{101, 102, 103, 104})
	s.put(41, []byte{1, 2, 4}, []byte{101, 102, 103, 105})

	affected := s.delete(40, []byte{1, 2, 5})
	assert.Equal(t, false, affected)

	assert.Equal(t, uint64(2), s.getTotal())

	data := make([]byte, 100)
	n, ok := s.get(40, []byte{1, 2, 3}, data)
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte{101, 102, 103, 104}, data[:n])
	assert.Equal(t, s.totalAccessTime, s.getSumTotalAccessTime())
}

func TestSegment_Delete_Already_Deleted(t *testing.T) {
	s := newSegment()
	s.getNow = monoGetNow(200)

	s.put(40, []byte{1, 2, 3}, []byte{101, 102, 103, 104})
	s.put(41, []byte{1, 2, 4}, []byte{101, 102, 103, 105})

	affected := s.delete(40, []byte{1, 2, 3})
	assert.Equal(t, true, affected)

	assert.Equal(t, uint64(1), s.getTotal())

	affected = s.delete(40, []byte{1, 2, 3})
	assert.Equal(t, false, affected)

	assert.Equal(t, uint64(1), s.getTotal())

	data := make([]byte, 100)
	n, ok := s.get(40, []byte{1, 2, 3}, data)
	assert.Equal(t, false, ok)
	assert.Equal(t, 0, n)
}

func TestSegment_Put_Evacuate_After_Deleted(t *testing.T) {
	const entrySize = entryHeaderSize + 8
	s := newSegmentSize(entrySize * 5)
	s.getNow = monoGetNow(0)

	s.put(40, []byte{1, 2, 0}, []byte{101, 102, 103, 100})
	s.put(41, []byte{1, 2, 1}, []byte{101, 102, 103, 101})
	s.put(42, []byte{1, 2, 2}, []byte{101, 102, 103, 102})
	s.put(43, []byte{1, 2, 3}, []byte{101, 102, 103, 103})
	s.put(44, []byte{1, 2, 4}, []byte{101, 102, 103, 104})

	data := make([]byte, 100)
	s.get(40, []byte{1, 2, 0}, data)
	s.delete(40, []byte{1, 2, 0})

	assert.Equal(t, uint64(4), s.getTotal())
	assert.Equal(t, 4, len(s.kv))

	s.put(45, []byte{1, 2, 5}, []byte{101, 102, 103, 105})

	assert.Equal(t, uint64(5), s.getTotal())
	assert.Equal(t, 5, len(s.kv))

	data = make([]byte, 100)
	n, ok := s.get(41, []byte{1, 2, 1}, data)
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte{101, 102, 103, 101}, data[:n])
}

func TestSegment_Put_Evacuate_Need_Reset_ConsecutiveEvacuation(t *testing.T) {
	const entrySize = entryHeaderSize + 8
	s := newSegmentSize(entrySize * 12)
	s.maxConsecutiveEvacuation = 2
	s.getNow = monoGetNow(0)

	s.put(40, []byte{1, 2, 0}, []byte{101, 102, 103, 100})
	s.put(41, []byte{1, 2, 1}, []byte{101, 102, 103, 101})
	s.put(42, []byte{1, 2, 2}, []byte{101, 102, 103, 102})
	s.put(43, []byte{1, 2, 3}, []byte{101, 102, 103, 103})

	s.put(44, []byte{1, 2, 4}, []byte{101, 102, 103, 104})
	s.put(45, []byte{1, 2, 5}, []byte{101, 102, 103, 105})
	s.put(46, []byte{1, 2, 6}, []byte{101, 102, 103, 106})
	s.put(47, []byte{1, 2, 7}, []byte{101, 102, 103, 107})

	s.put(48, []byte{1, 2, 8}, []byte{101, 102, 103, 108})
	s.put(49, []byte{1, 2, 9}, []byte{101, 102, 103, 109})
	s.put(50, []byte{1, 2, 10}, []byte{101, 102, 103, 110})
	s.put(51, []byte{1, 2, 11}, []byte{101, 102, 103, 111})

	data := make([]byte, 100)
	s.get(40, []byte{1, 2, 0}, data)
	s.get(41, []byte{1, 2, 1}, data)
	s.get(42, []byte{1, 2, 2}, data)
	s.get(43, []byte{1, 2, 3}, data)

	s.get(44, []byte{1, 2, 4}, data)

	s.put(60, []byte{5, 5, 5}, []byte{10, 10, 10, 10, 10, 10})

	_, ok := s.get(40, []byte{1, 2, 0}, data)
	assert.Equal(t, true, ok)
	_, ok = s.get(41, []byte{1, 2, 1}, data)
	assert.Equal(t, true, ok)
	_, ok = s.get(42, []byte{1, 2, 2}, data)
	assert.Equal(t, false, ok)
	_, ok = s.get(43, []byte{1, 2, 3}, data)
	assert.Equal(t, true, ok)
	_, ok = s.get(44, []byte{1, 2, 4}, data)
	assert.Equal(t, true, ok)
	_, ok = s.get(45, []byte{1, 2, 5}, data)
	assert.Equal(t, false, ok)

	assert.Equal(t, 11, len(s.kv))
	assert.Equal(t, uint64(11), s.getTotal())
	assert.Equal(t, s.totalAccessTime, s.getSumTotalAccessTime())
}

func fillRandom(data []byte) {
	_, err := rand.Read(data)
	if err != nil {
		panic(err)
	}
}

func appendKeyValue(key []byte, val []byte) []byte {
	data := make([]byte, len(key)+len(val)-1)
	copy(data, key)
	copy(data[len(key):], val[1:])
	return data
}

func TestAppendKeyValue(t *testing.T) {
	data := appendKeyValue([]byte{1, 2, 3}, []byte{4, 5, 6, 7})
	assert.Equal(t, []byte{1, 2, 3, 5, 6, 7}, data)
}

func TestSegment_Stress_Testing(t *testing.T) {
	s := newSegmentSize(123456)
	s.getNow = monoGetNow(0)

	type keyUsed struct {
		hash uint64
		key  []byte
	}

	const keyCount = 10000
	const touchCount = 5

	keyUsedList := make([]keyUsed, 0, keyCount)

	placeholder := make([]byte, 1000)

	for i := 0; i < keyCount; i++ {
		keyLen := 2 + rand.Intn(10)
		key := make([]byte, keyLen)
		fillRandom(key)
		hash := memhash.Hash(key) & 0xfff
		keyUsedList = append(keyUsedList, keyUsed{
			key:  key,
			hash: hash,
		})
	}

	for i := 0; i < keyCount*3; i++ {
		valueLen := 1 + rand.Intn(50)
		value := make([]byte, valueLen)
		fillRandom(value)

		for k := 0; k < touchCount; k++ {
			index := rand.Intn(keyCount)
			e := keyUsedList[index]
			s.get(uint32(e.hash), e.key, placeholder)
		}

		index := rand.Intn(keyCount)
		e := keyUsedList[index]

		h := memhash.Hash(appendKeyValue(e.key, value))
		value[0] = uint8(h)

		putOrDelete := rand.Intn(3)
		if putOrDelete < 2 {
			s.put(uint32(e.hash), e.key, value)
		} else {
			s.delete(uint32(e.hash), e.key)
		}
	}

	for _, e := range keyUsedList {
		data := make([]byte, 100)
		n, ok := s.get(uint32(e.hash), e.key, data)
		if ok {
			value := data[:n]
			h := memhash.Hash(appendKeyValue(e.key, value))
			assert.Equal(t, uint8(h), value[0])
		}
	}

	assert.Equal(t, uint64((touchCount*3+1)*keyCount), s.getAccessCount())
	assert.Equal(t, len(s.kv), int(s.getTotal()))
	assert.Equal(t, s.totalAccessTime, s.getSumTotalAccessTime())

	fmt.Println(s.getHitCount())
	fmt.Println(s.getAccessCount())
	fmt.Println("TOTAL:", s.getTotal())
	fmt.Println("LEN:", len(s.kv))
}

func BenchmarkSegmentPut(b *testing.B) {
	b.StopTimer()

	key := make([]byte, 32)
	value := make([]byte, 200)
	data := make([]byte, 500)

	num := (*uint64)(unsafe.Pointer(&key[0]))

	fillRandom(key)
	fillRandom(value)

	tmp := make([]byte, len(key))
	copy(tmp, key)

	s := newSegmentSize(2000000)

	const actionCount = 10000

	b.StartTimer()
	hitCount := 0
	for n := 0; n < b.N; n++ {
		for i := 0; i < actionCount; i++ {
			*num++
			hash := memhash.Hash(key)
			s.put(uint32(hash), key, value)
		}

		copy(key, tmp)

		for i := 0; i < actionCount; i++ {
			*num++
			hash := memhash.Hash(key)
			_, ok := s.get(uint32(hash), key, data)
			if ok {
				hitCount++
			}
		}
	}
	fmt.Println(hitCount, b.N)
}
