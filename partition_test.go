package espresso

import (
	"github.com/QuangTung97/espresso/allocator"
	"github.com/QuangTung97/espresso/lru"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
	"unsafe"
)

func TestSizeOfEntryHeader(t *testing.T) {
	assert.Equal(t, uintptr(32), unsafe.Sizeof(entryHeader{}))
}

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

var lruEntrySize = uint32(unsafe.Sizeof(lru.ListHead{}))

func TestPartition_PutLease(t *testing.T) {
	conf := PartitionConfig{
		InitAdmissionLimit: 3,
		ProtectedRatio:     NewRational(80, 100),
		MinProtectedLimit:  50,
		AllocatorConfig: allocator.Config{
			MemLimit:     16 << 12,
			LRUEntrySize: lruEntrySize,
			Slabs: []allocator.SlabConfig{
				{
					ElemSize:     96,
					ChunkSizeLog: 12,
				},
			},
		},
	}

	p := NewPartition(conf)

	ok := p.putLease(1100, []byte{1, 2, 3}, 11)
	assert.True(t, ok)
	assert.Equal(t, []uint64{1100}, p.admission.GetLRUList())
	contentMap := map[uint64]uint32{
		1100: 1 << 12,
	}
	assert.Equal(t, contentMap, p.contentMap)

	result, ok := p.get(1100)
	assert.True(t, ok)

	assert.Equal(t, entryStatusLeasing, result.status)
	assert.Equal(t, lruListAdmission, result.lruList)

	assert.Equal(t, uint64(1100), result.hash)
	assert.Equal(t, uint64(11), result.leaseID)
	assert.Equal(t, []byte{1, 2, 3}, result.key)
	assert.Equal(t, []byte{}, result.value)

	ok = p.putLease(2200, []byte{5, 6, 7}, 22)
	assert.True(t, ok)
	assert.Equal(t, []uint64{2200, 1100}, p.admission.GetLRUList())
	contentMap = map[uint64]uint32{
		1100: 1 << 12,
		2200: 1<<12 + 96,
	}
	assert.Equal(t, contentMap, p.contentMap)

	ok = p.putLease(3300, []byte{8, 9, 10}, 33)
	assert.True(t, ok)
	assert.Equal(t, []uint64{3300, 2200, 1100}, p.admission.GetLRUList())
	contentMap = map[uint64]uint32{
		1100: 1 << 12,
		2200: 1<<12 + 96,
		3300: 1<<12 + 2*96,
	}
	assert.Equal(t, contentMap, p.contentMap)

	ok = p.putLease(4400, []byte{11, 12, 13}, 44)
	assert.True(t, ok)
	assert.Equal(t, []uint64{4400, 3300, 2200}, p.admission.GetLRUList())
	assert.Equal(t, []uint64{1100}, p.probation.GetLRUList())

	result, ok = p.get(1100)
	assert.True(t, ok)
	assert.Equal(t, lruListProbation, result.lruList)
}

func TestPartition_Update(t *testing.T) {
	conf := PartitionConfig{
		InitAdmissionLimit: 3,
		ProtectedRatio:     NewRational(80, 100),
		MinProtectedLimit:  50,
		AllocatorConfig: allocator.Config{
			MemLimit:     16 << 12,
			LRUEntrySize: lruEntrySize,
			Slabs: []allocator.SlabConfig{
				{
					ElemSize:     96,
					ChunkSizeLog: 12,
				},
			},
		},
	}

	p := NewPartition(conf)

	p.putLease(1100, []byte{1, 2, 3}, 11)
	// TODO
	// p.update(1100, []byte{})
}
