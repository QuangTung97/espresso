package espresso

import (
	"github.com/QuangTung97/espresso/allocator"
	"github.com/QuangTung97/espresso/lru"
	"github.com/stretchr/testify/assert"
	"testing"
	"unsafe"
)

func TestNewPartition(t *testing.T) {
	conf := PartitionConfig{
		AllocatorConfig: allocator.Config{
			MemLimit:     16 << 12,
			LRUEntrySize: uint32(unsafe.Sizeof(lru.ListHead{})),
			Slabs: []allocator.SlabConfig{
				{
					ElemSize:     88,
					ChunkSizeLog: 12,
				},
				{
					ElemSize:     102,
					ChunkSizeLog: 12,
				},
			},
		},
	}
	assert.Equal(t, uint32(16), conf.AllocatorConfig.LRUEntrySize)

	p := NewPartition(conf)
	assert.NotNil(t, p.allocator)
	assert.NotNil(t, p.contentMap)
}
