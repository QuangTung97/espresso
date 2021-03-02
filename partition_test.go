package espresso

import (
	"github.com/QuangTung97/espresso/allocator"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewPartition(t *testing.T) {
	conf := PartitionConfig{
		AllocatorConfig: allocator.Config{
			MemLimit: 16 << 12,
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
	p := NewPartition(conf)
	assert.NotNil(t, p.allocator)
	assert.NotNil(t, p.contentMap)
}
