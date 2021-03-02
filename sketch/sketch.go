package sketch

import (
	"math/bits"
)

// A mixture of seeds from FNV-1a, CityHash, and Murmur3
var seed = []uint64{
	0xc3a5c85c97cb3127,
	0xb492b66fbe98f273,
	0x9ae16a3b2f90404f,
	0xcbf29ce484222325,
}

// Sketch ...
type Sketch struct {
	table      []uint64
	tableMask  uint64
	size       uint64
	sampleSize uint64
}

const (
	innerMask uint64 = 0xf
	resetMask uint64 = 0x7777777777777777
	oneMask   uint64 = 0x1111111111111111
)

// New ...
func New(numCounters uint64, cacheSize uint64) *Sketch {
	n := (1<<bits.Len64(numCounters-1) + 15) >> 4
	table := make([]uint64, n)
	for i := range table {
		table[i] = 0
	}
	return &Sketch{
		table:      table,
		tableMask:  uint64(n) - 1,
		size:       0,
		sampleSize: 10 * cacheSize,
	}
}

// UpdateCacheSize ...
func (s *Sketch) UpdateCacheSize(size uint64) {
	s.sampleSize = 10 * size
}

func (s *Sketch) increaseAt(tableIndex uint64, innerIndex uint64) uint32 {
	offset := innerIndex << 2
	mask := innerMask << offset
	if (s.table[tableIndex] & mask) != mask {
		s.table[tableIndex] += 1 << offset
		return 1
	}
	return 0
}

func (s *Sketch) getCounterAt(tableIndex uint64, innerIndex uint64) uint32 {
	offset := innerIndex << 2
	return uint32((s.table[tableIndex] >> offset) & innerMask)
}

func indexOf(item uint64, i uint64, tableMask uint64) uint64 {
	hash := (item + seed[i]) * seed[i]
	hash += hash >> 32
	return hash & tableMask
}

// Increase ...
func (s *Sketch) Increase(hash uint64) {
	start := (hash & 3) << 2

	index0 := indexOf(hash, 0, s.tableMask)
	index1 := indexOf(hash, 1, s.tableMask)
	index2 := indexOf(hash, 2, s.tableMask)
	index3 := indexOf(hash, 3, s.tableMask)

	added := s.increaseAt(index0, start)
	added |= s.increaseAt(index1, start+1)
	added |= s.increaseAt(index2, start+2)
	added |= s.increaseAt(index3, start+3)

	if added != 0 {
		s.size++
		if s.size == s.sampleSize {
			s.reset()
		}
	}
}

func (s *Sketch) reset() {
	count := uint64(0)
	for i := range s.table {
		count += uint64(bits.OnesCount64(s.table[i] & oneMask))
		s.table[i] = (s.table[i] >> 1) & resetMask
	}
	s.size = s.size>>1 - count>>2
}

func minUint32(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}

// Frequency ...
func (s *Sketch) Frequency(hash uint64) uint32 {
	start := (hash & 3) << 2

	index0 := indexOf(hash, 0, s.tableMask)
	index1 := indexOf(hash, 1, s.tableMask)
	index2 := indexOf(hash, 2, s.tableMask)
	index3 := indexOf(hash, 3, s.tableMask)

	min := s.getCounterAt(index0, start)
	min = minUint32(min, s.getCounterAt(index1, start+1))
	min = minUint32(min, s.getCounterAt(index2, start+2))
	min = minUint32(min, s.getCounterAt(index3, start+3))

	return min
}
