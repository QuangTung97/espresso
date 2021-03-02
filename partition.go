package espresso

import "github.com/QuangTung97/espresso/allocator"

// PartitionConfig ...
type PartitionConfig struct {
	AllocatorConfig allocator.Config
}

// Partition ...
type Partition struct {
	allocator  *allocator.Allocator
	contentMap map[uint64]uint32
}

// NewPartition ...
func NewPartition(conf PartitionConfig) *Partition {
	return &Partition{
		allocator:  allocator.New(conf.AllocatorConfig),
		contentMap: map[uint64]uint32{},
	}
}
