package espresso

import (
	"github.com/QuangTung97/espresso/allocator"
	"github.com/QuangTung97/espresso/lru"
	"math"
	"unsafe"
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

type entryHeader struct {
	size             uint32
	keySize          uint32
	leaseIDOrVersion uint64
	hash             uint64
	lruAddr          uint32
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

func (p *Partition) putLease(hash uint64, key []byte, leaseID uint64) bool {
	size := uint32(unsafe.Sizeof(entryHeader{})) + uint32(len(key))
	lruAddr, ok := p.admission.Put(hash)
	if !ok {
		// TODO loop until enough space
		return false
	}

	addr, ok := p.allocator.Allocate(size)
	if !ok {
		// TODO loop until enough space
		return false
	}
	p.contentMap[hash] = addr

	header := (*entryHeader)(p.allocator.ToRealAddr(addr))
	*header = entryHeader{
		size:             size,
		keySize:          uint32(len(key)),
		leaseIDOrVersion: leaseID,
		hash:             hash,
		lruAddr:          lruAddr,
	}

	return true
}
