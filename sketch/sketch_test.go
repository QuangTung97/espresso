package sketch

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewSketch(t *testing.T) {
	s := New(200, 400)
	assert.Equal(t, 16, len(s.table))
	assert.Equal(t, uint64(0), s.size)
	assert.Equal(t, uint64(4000), s.sampleSize)
	assert.Equal(t, uint64(15), s.tableMask)

	s.UpdateCacheSize(500)
	assert.Equal(t, uint64(5000), s.sampleSize)

	s = New(16, 1)
	assert.Equal(t, []uint64{0}, s.table)

	s = New(17, 1)
	assert.Equal(t, []uint64{0, 0}, s.table)

	s = New(1, 1)
	assert.Equal(t, []uint64{0}, s.table)
}

func TestSketch_IncreaseAt(t *testing.T) {
	s := New(64, 1)
	assert.Equal(t, []uint64{0, 0, 0, 0}, s.table)

	ok := s.increaseAt(0, 0)
	assert.Equal(t, uint32(1), ok)
	assert.Equal(t, []uint64{1, 0, 0, 0}, s.table)

	ok = s.increaseAt(2, 0)
	assert.Equal(t, uint32(1), ok)
	assert.Equal(t, []uint64{1, 0, 1, 0}, s.table)

	ok = s.increaseAt(2, 0)
	assert.Equal(t, uint32(1), ok)
	assert.Equal(t, []uint64{1, 0, 2, 0}, s.table)

	ok = s.increaseAt(2, 4)
	assert.Equal(t, uint32(1), ok)
	assert.Equal(t, []uint64{1, 0, 1<<16 + 2, 0}, s.table)

	ok = s.increaseAt(2, 4)
	assert.Equal(t, uint32(1), ok)
	assert.Equal(t, []uint64{1, 0, 2<<16 + 2, 0}, s.table)

	s.table = []uint64{1, 0, 15<<16 + 2, 0}
	ok = s.increaseAt(2, 4)
	assert.Equal(t, uint32(0), ok)
	assert.Equal(t, []uint64{1, 0, 15<<16 + 2, 0}, s.table)
}

func TestSketch_Reset(t *testing.T) {
	s := New(64, 1)
	s.size = 100
	s.table = []uint64{5, 8<<12 + 4, 15<<16 + 2, 3<<4 + 5}

	s.reset()
	assert.Equal(t, []uint64{2, 4<<12 + 2, 7<<16 + 1, 1<<4 + 2}, s.table)
	assert.Equal(t, uint64(50-1), s.size)

	s = New(64, 1)
	s.size = 100
	s.table = []uint64{5, 8<<12 + 4, 15<<16 + 2, 3<<4 + 4}

	s.reset()
	assert.Equal(t, []uint64{2, 4<<12 + 2, 7<<16 + 1, 1<<4 + 2}, s.table)
	assert.Equal(t, uint64(50), s.size)
}

func TestSketch_IndexOf(t *testing.T) {
	v := (234 + seed[0]) * seed[0]
	v += v >> 32
	v = v & 15
	assert.Equal(t, v, indexOf(234, 0, 15))

	v = (234 + seed[1]) * seed[1]
	v += v >> 32
	v = v & 15
	assert.Equal(t, v, indexOf(234, 1, 15))
}

func TestSketch_Increase(t *testing.T) {
	s := New(64, 5)
	s.Increase(1237)
	assert.Equal(t, []uint64{1<<24 + 1<<16, 1 << 20, 0, 1 << 28}, s.table)

	s.table = []uint64{15<<24 + 15<<16, 15 << 20, 0, 15 << 28}
	assert.Equal(t, uint64(1), s.size)
	s.Increase(1237)
	assert.Equal(t, []uint64{15<<24 + 15<<16, 15 << 20, 0, 15 << 28}, s.table)
	assert.Equal(t, uint64(1), s.size)

	s.table = []uint64{14<<24 + 15<<16, 15 << 20, 0, 15 << 28}
	s.size = 49
	s.Increase(1237)
	assert.Equal(t, uint64(50/2-4/4), s.size)
	assert.Equal(t, []uint64{7<<24 + 7<<16, 7 << 20, 0, 7 << 28}, s.table)
}

func TestSketch_Frequency(t *testing.T) {
	s := New(64, 5)
	s.table = []uint64{8<<24 + 3<<16, 7 << 20, 0, 10 << 28}
	assert.Equal(t, uint32(3), s.Frequency(1237))

	s.table = []uint64{8<<24 + 11<<16, 7 << 20, 0, 10 << 28}
	assert.Equal(t, uint32(7), s.Frequency(1237))

	s.table = []uint64{12<<24 + 11<<16, 15 << 20, 0, 10 << 28}
	assert.Equal(t, uint32(10), s.Frequency(1237))
}
