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
		{
			name:         "5-GB",
			minSizeLog:   12,
			sizeMultiple: 5 << 18,
			expected:     5 << 24,
		},
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
	assert.Equal(t, uint32(309), alloc.buddy.sizeMultiple)
	assert.Equal(t, uint32(12), alloc.buddy.minSize)

	assert.Equal(t, 3, len(alloc.slabs))

	assert.Equal(t, uint32(88), alloc.slabs[0].elemSize)
	assert.Equal(t, uint32(12), alloc.slabs[0].chunkSizeLog)

	assert.Equal(t, uint32(102), alloc.slabs[1].elemSize)
	assert.Equal(t, uint32(12), alloc.slabs[1].chunkSizeLog)

	assert.Equal(t, uint32(128), alloc.slabs[2].elemSize)
	assert.Equal(t, uint32(13), alloc.slabs[2].chunkSizeLog)

	assert.Equal(t, 0, alloc.memoryUsage)
}
