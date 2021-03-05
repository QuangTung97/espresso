package espresso

import (
	"github.com/QuangTung97/espresso/allocator"
	"github.com/QuangTung97/espresso/lru"
	"math"
	"reflect"
	"unsafe"
)

type entryStatus uint16

const (
	entryStatusLeasing entryStatus = 0
	entryStatusValid   entryStatus = 1
	entryStatusInvalid entryStatus = 2
)

type lruListType uint16

const (
	lruListAdmission lruListType = 0
	lruListProtected lruListType = 1
	lruListProbation lruListType = 2
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
	size    uint32 // size is the size of the whole entry (including header)
	keySize uint32 // keySize is the size of key only
	leaseID uint64 // leaseID or version
	hash    uint64 // hash
	lruAddr uint32 // address of LRU List Head
	status  entryStatus
	lruList lruListType
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

func (p *Partition) getBytes(addr uint32, length uint32) []byte {
	var result []byte
	header := (*reflect.SliceHeader)(unsafe.Pointer(&result))
	header.Data = uintptr(p.allocator.ToRealAddr(addr))
	header.Len = int(length)
	header.Cap = int(length)
	return result
}

func assertTrue(b bool) {
	if !b {
		panic("must be true")
	}
}

func (p *Partition) putLease(hash uint64, key []byte, leaseID uint64) bool {
	size := uint32(unsafe.Sizeof(entryHeader{})) + uint32(len(key))

	var lruAddr uint32
	var lruList lruListType

	for p.admission.Size() >= p.admission.Limit() {
		lastAddr, lastHash := p.admission.Last()
		p.admission.Delete(lastAddr)

		// Can NOT be false here
		lastAddr, ok := p.probation.Put(lastHash)
		assertTrue(ok)

		entryAddr := p.contentMap[lastHash]
		header := (*entryHeader)(p.allocator.ToRealAddr(entryAddr))
		header.lruList = lruListProbation
	}

	lruAddr, ok := p.admission.Put(hash)
	if !ok {
		// TODO loop until enough space
		return false
	}
	lruList = lruListAdmission

	addr, ok := p.allocator.Allocate(size)
	if !ok {
		// TODO loop until enough space
		return false
	}
	p.contentMap[hash] = addr

	header := (*entryHeader)(p.allocator.ToRealAddr(addr))
	*header = entryHeader{
		size:    size,
		keySize: uint32(len(key)),
		leaseID: leaseID,
		hash:    hash,
		lruAddr: lruAddr,
		status:  entryStatusLeasing,
		lruList: lruList,
	}

	keyAddr := addr + uint32(unsafe.Sizeof(entryHeader{}))
	keyLen := header.keySize

	copy(p.getBytes(keyAddr, keyLen), key)

	return true
}

func (p *Partition) putValue(hash uint64, key []byte, version uint64, value []byte) bool {
	entryAddr := p.contentMap[hash]
	header := (*entryHeader)(p.allocator.ToRealAddr(entryAddr))
	header.status = entryStatusValid
	header.leaseID = version

	newSize := uint32(unsafe.Sizeof(entryHeader{})) + uint32(len(key)) + uint32(len(value))

	if p.allocator.GetSlabSize(header.size) != p.allocator.GetSlabSize(newSize) {
		oldSize := header.size

		newAddr, ok := p.allocator.Allocate(newSize)
		if !ok {
			// TODO loop until enough space
			return false
		}
		newHeader := (*entryHeader)(p.allocator.ToRealAddr(newAddr))
		*newHeader = *header
		header = newHeader

		p.contentMap[hash] = newAddr

		keyAddr := newAddr + uint32(unsafe.Sizeof(entryHeader{}))
		keyLen := uint32(len(key))
		copy(p.getBytes(keyAddr, keyLen), key)

		valueAddr := keyAddr + keyLen
		valueLen := uint32(len(value))
		copy(p.getBytes(valueAddr, valueLen), value)

		p.allocator.Deallocate(entryAddr, oldSize)
		// TODO need move
	} else {
		valueAddr := entryAddr + uint32(unsafe.Sizeof(entryHeader{})) + uint32(len(key))
		valueLen := uint32(len(value))
		bytes := p.getBytes(valueAddr, valueLen)
		copy(bytes, value)
	}

	header.size = newSize

	return true
}

type getResult struct {
	status  entryStatus
	lruList lruListType
	leaseID uint64 // or version
	hash    uint64
	key     []byte
	value   []byte
}

func (p *Partition) get(hash uint64) (getResult, bool) {
	addr, ok := p.contentMap[hash]
	if !ok {
		return getResult{}, false
	}
	header := (*entryHeader)(p.allocator.ToRealAddr(addr))

	keyAddr := addr + uint32(unsafe.Sizeof(entryHeader{}))
	keyLen := header.keySize

	valueAddr := keyAddr + keyLen
	valueLen := header.size - uint32(unsafe.Sizeof(entryHeader{})) - keyLen

	return getResult{
		status:  header.status,
		lruList: header.lruList,
		hash:    header.hash,
		leaseID: header.leaseID,
		key:     p.getBytes(keyAddr, keyLen),
		value:   p.getBytes(valueAddr, valueLen),
	}, true
}
