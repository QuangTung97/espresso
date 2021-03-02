package allocator

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFinMinSizeLog(t *testing.T) {
	slabs := []SlabConfig{
		{
			ElemSize:     88,
			ChunkSizeLog: 12,
		},
	}
	assert.Equal(t, uint32(12), findMinSizeLog(slabs))

	slabs = []SlabConfig{
		{
			ElemSize:     88,
			ChunkSizeLog: 14,
		},
		{
			ElemSize:     88,
			ChunkSizeLog: 11,
		},
	}
	assert.Equal(t, uint32(11), findMinSizeLog(slabs))
}

func TestFindSizeMultiple(t *testing.T) {
	table := []struct {
		name     string
		minSize  uint32
		limit    int
		expected uint32
	}{
		{
			name:     "normal",
			minSize:  12,
			limit:    1234 << 10,
			expected: 309,
		},
		{
			name:     "near-next-size-multiple",
			minSize:  12,
			limit:    1235 << 10,
			expected: 309,
		},
		{
			name:     "next-size-multiple",
			minSize:  12,
			limit:    1236 << 10,
			expected: 309,
		},
		{
			name:     "next-size-multiple",
			minSize:  12,
			limit:    1237 << 10,
			expected: 310,
		},
		{
			name:     "more-than-4GB",
			minSize:  12,
			limit:    5 << 30,
			expected: 5 << 18,
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			result := findSizeMultiple(e.minSize, e.limit)
			assert.Equal(t, e.expected, result)
		})
	}
}

func TestAllocateData(t *testing.T) {
	table := []struct {
		name         string
		minSizeLog   uint32
		sizeMultiple uint32
		expected     int
	}{
		{
			name:         "normal",
			minSizeLog:   6,
			sizeMultiple: 20,
			expected:     20,
		},
		{
			name:         "normal",
			minSizeLog:   8,
			sizeMultiple: 20,
			expected:     20 << 2,
		},
		//{
		//	name:         "5-GB",
		//	minSizeLog:   12,
		//	sizeMultiple: 5 << 18,
		//	expected:     5 << 24,
		//},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			data := allocateData(e.minSizeLog, e.sizeMultiple)
			assert.Equal(t, e.expected, len(data))
		})
	}
}

func TestAllocatorNew(t *testing.T) {
	conf := Config{
		MemLimit: 1234 << 10,
		Slabs: []SlabConfig{
			{
				ElemSize:     88,
				ChunkSizeLog: 12,
			},
			{
				ElemSize:     102,
				ChunkSizeLog: 12,
			},
			{
				ElemSize:     128,
				ChunkSizeLog: 13,
			},
		},
	}

	alloc := New(conf)
	assert.Equal(t, []uint32{88, 102, 128}, alloc.slabSizeList)

	assert.Equal(t, uint32(309), alloc.buddy.sizeMultiple)
	assert.Equal(t, uint32(12), alloc.buddy.minSize)

	assert.Equal(t, 3, len(alloc.slabs))

	assert.Equal(t, uint32(88), alloc.slabs[0].elemSize)
	assert.Equal(t, uint32(12), alloc.slabs[0].chunkSizeLog)

	assert.Equal(t, uint32(102), alloc.slabs[1].elemSize)
	assert.Equal(t, uint32(12), alloc.slabs[1].chunkSizeLog)

	assert.Equal(t, uint32(128), alloc.slabs[2].elemSize)
	assert.Equal(t, uint32(13), alloc.slabs[2].chunkSizeLog)

	assert.Equal(t, uint64(0), alloc.memoryUsage)
}

func TestFindSlabIndex(t *testing.T) {
	table := []struct {
		name     string
		sizes    []uint32
		value    uint32
		expected int
	}{
		{
			name:     "empty",
			sizes:    []uint32{},
			value:    0,
			expected: 0,
		},
		{
			name:     "single",
			sizes:    []uint32{23},
			value:    0,
			expected: 0,
		},
		{
			name:     "single",
			sizes:    []uint32{23},
			value:    23,
			expected: 0,
		},
		{
			name:     "two",
			sizes:    []uint32{23, 55},
			value:    23,
			expected: 0,
		},
		{
			name:     "two",
			sizes:    []uint32{23, 55},
			value:    24,
			expected: 1,
		},
		{
			name:     "multiple",
			sizes:    []uint32{23, 55, 88, 99, 103, 202},
			value:    98,
			expected: 3,
		},
		{
			name:     "multiple",
			sizes:    []uint32{23, 55, 88, 99, 103, 202},
			value:    201,
			expected: 5,
		},
	}
	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			result := findSlabIndex(e.sizes, e.value)
			assert.Equal(t, e.expected, result)
		})
	}

}

func TestAllocator_Allocate_Deallocate(t *testing.T) {
	conf := Config{
		MemLimit: 17 << 12,
		Slabs: []SlabConfig{
			{
				ElemSize:     88,
				ChunkSizeLog: 12,
			},
			{
				ElemSize:     102,
				ChunkSizeLog: 12,
			},
			{
				ElemSize:     128,
				ChunkSizeLog: 13,
			},
		},
	}
	a := New(conf)

	p1, ok := a.Allocate(87)
	assert.True(t, ok)
	assert.Equal(t, uint32(16<<12), p1)
	assert.Equal(t, uint64(48+88), a.GetMemUsage())

	p2, ok := a.Allocate(87)
	assert.True(t, ok)
	assert.Equal(t, uint32(16<<12+88), p2)
	assert.Equal(t, uint64(48+2*88), a.GetMemUsage())

	p3, ok := a.Allocate(101)
	assert.True(t, ok)
	assert.Equal(t, uint32(0), p3)
	assert.Equal(t, uint64(48+2*88+16+102), a.GetMemUsage())

	movedAddr, needMove := a.Deallocate(p1, 87)
	assert.True(t, needMove)
	assert.Equal(t, p2, movedAddr)
	assert.Equal(t, uint64(48+88+16+102), a.GetMemUsage())

	movedAddr, needMove = a.Deallocate(p3, 101)
	assert.False(t, needMove)
	assert.Equal(t, uint32(0), movedAddr)
	assert.Equal(t, uint64(48+88), a.GetMemUsage())

	movedAddr, needMove = a.Deallocate(p1, 87)
	assert.False(t, needMove)
	assert.Equal(t, uint32(0), movedAddr)
	assert.Equal(t, uint64(0), a.GetMemUsage())
}
