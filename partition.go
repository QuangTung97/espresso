package espresso

import (
	"github.com/QuangTung97/espresso/allocator"
	"github.com/QuangTung97/espresso/lru"
	"math"
)

// PartitionConfig ...
type PartitionConfig struct {
	InitAdmissionLimit uint32
	ProtectedRatio     Rational
	MinProtectedLimit  uint32
	AllocatorConfig    allocator.Config
}

// Partition ...
type Partition struct {
	allocator  *allocator.Allocator
	contentMap map[uint64]uint32

	admission *lru.LRU
	protected *lru.LRU
	probation *lru.LRU
}

// NewPartition ...
func NewPartition(conf PartitionConfig) *Partition {
	alloc := allocator.New(conf.AllocatorConfig)
	return &Partition{
		allocator:  alloc,
		contentMap: map[uint64]uint32{},

		admission: lru.New(alloc.GetLRUSlab(), conf.InitAdmissionLimit),
		protected: lru.New(alloc.GetLRUSlab(), conf.MinProtectedLimit),
		probation: lru.New(alloc.GetLRUSlab(), math.MaxUint32),
	}
}
