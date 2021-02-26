package allocator

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAllocatorNew(t *testing.T) {
	conf := Config{
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
				ChunkSizeLog: 12,
			},
		},
	}

	alloc := New(conf)
	assert.Equal(t, 3, len(alloc.slabs))
}
