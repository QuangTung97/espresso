package espresso

import (
	"github.com/QuangTung97/espresso/allocator"
	"github.com/QuangTung97/espresso/lru"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
	"unsafe"
)

func TestNewPartition(t *testing.T) {
	conf := PartitionConfig{
		InitAdmissionLimit: 123,
		ProtectedRatio:     NewRational(80, 100),
		MinProtectedLimit:  50,
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

	assert.NotNil(t, p.admission)
	assert.Equal(t, uint32(123), p.admission.Limit())

	assert.NotNil(t, p.protected)
	assert.Equal(t, uint32(50), p.protected.Limit())

	assert.NotNil(t, p.probation)
	assert.Equal(t, uint32(math.MaxUint32), p.probation.Limit())
}

func TestPartition_PutLease(t *testing.T) {
	conf := PartitionConfig{
		InitAdmissionLimit: 123,
		ProtectedRatio:     NewRational(80, 100),
		MinProtectedLimit:  50,
		AllocatorConfig: allocator.Config{
			MemLimit:     16 << 12,
			LRUEntrySize: uint32(unsafe.Sizeof(lru.ListHead{})),
			Slabs: []allocator.SlabConfig{
				{
					ElemSize:     96,
					ChunkSizeLog: 12,
				},
			},
		},
	}

	p := NewPartition(conf)
	ok := p.putLease(1100, []byte{1, 2, 3}, 22)
	assert.True(t, ok)
	assert.Equal(t, []uint64{1100}, p.admission.GetLRUList())
	contentMap := map[uint64]uint32{
		1100: 1 << 12,
	}
	assert.Equal(t, contentMap, p.contentMap)

}
