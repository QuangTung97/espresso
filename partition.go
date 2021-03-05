package espresso

import (
	"bytes"
	"github.com/QuangTung97/espresso/allocator"
	"github.com/QuangTung97/espresso/lru"
	"github.com/QuangTung97/espresso/sketch"
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

// LeaseGetStatus ...
type LeaseGetStatus uint32

const (
	// LeaseGetStatusLeaseGranted ...
	LeaseGetStatusLeaseGranted LeaseGetStatus = 1
	// LeaseGetStatusLeaseRejected ...
	LeaseGetStatusLeaseRejected LeaseGetStatus = 2
	// LeaseGetStatusExisted ...
	LeaseGetStatusExisted LeaseGetStatus = 3
)

// PartitionConfig ...
type PartitionConfig struct {
	InitAdmissionLimit uint32
	ProtectedRatio     Rational
	MinProtectedLimit  uint32
	NumCounters        uint64
	SketchMinCacheSize uint64
	AllocatorConfig    allocator.Config
}

// Partition ...
type Partition struct {
	allocator  *allocator.Allocator
	contentMap map[uint64]uint32
	sketch     *sketch.Sketch

	leaseIDSeq uint64

	admission *lru.LRU
	protected *lru.LRU
	probation *lru.LRU
}

// LeaseGetResult ...
type LeaseGetResult struct {
	Status  LeaseGetStatus
	LeaseID uint64
	Value   []byte
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

func validatePartitionConfig(conf PartitionConfig) {
	if conf.InitAdmissionLimit == 0 {
		panic("InitAdmissionLimit must > 0")
	}
	if conf.ProtectedRatio.Denominator == 0 || conf.ProtectedRatio.Nominator == 0 {
		panic("ProtectedRatio must not empty")
	}
	if conf.MinProtectedLimit == 0 {
		panic("MinProtectedLimit must > 0")
	}
	if conf.NumCounters == 0 {
		panic("NumCounters must > 0")
	}
	if conf.SketchMinCacheSize == 0 {
		panic("SketchMinCacheSize must > 0")
	}
}

// NewPartition ...
func NewPartition(conf PartitionConfig) *Partition {
	validatePartitionConfig(conf)

	alloc := allocator.New(conf.AllocatorConfig)
	return &Partition{
		allocator:  alloc,
		contentMap: map[uint64]uint32{},
		sketch:     sketch.New(conf.NumCounters, conf.SketchMinCacheSize),

		leaseIDSeq: 0,

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

func (p *Partition) evict() {
	if p.admission.Size() == 0 && p.probation.Size() == 0 {
		return
	}

	var lastAddr uint32
	if p.probation.Size() == 0 {
		admissionAddr, admissionHash := p.admission.Last()

		p.admission.Delete(admissionAddr)
		lastAddr = p.contentMap[admissionHash]
		delete(p.contentMap, admissionHash)
	} else if p.admission.Size() == 0 {
		probationAddr, probationHash := p.probation.Last()

		p.probation.Delete(probationAddr)
		lastAddr = p.contentMap[probationHash]
		delete(p.contentMap, probationHash)
	} else {
		admissionAddr, admissionHash := p.admission.Last()
		probationAddr, probationHash := p.probation.Last()

		if p.sketch.Frequency(admissionHash) <= p.sketch.Frequency(probationHash) {
			p.admission.Delete(admissionAddr)
			lastAddr = p.contentMap[admissionHash]
			delete(p.contentMap, admissionHash)
		} else {
			p.probation.Delete(probationAddr)
			lastAddr = p.contentMap[probationHash]
			delete(p.contentMap, probationHash)
		}
	}

	header := (*entryHeader)(p.allocator.ToRealAddr(lastAddr))
	size := header.size

	_, needMove := p.allocator.Deallocate(lastAddr, size)
	if needMove {
		p.contentMap[header.hash] = lastAddr
	}
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
		valueBytes := p.getBytes(valueAddr, valueLen)
		copy(valueBytes, value)
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

func (p *Partition) leaseGet(hash uint64, key []byte) LeaseGetResult {
	p.sketch.Increase(hash)

	result, existed := p.get(hash)
	if !existed {
		p.leaseIDSeq++

		// Must NOT be false
		ok := p.putLease(hash, key, p.leaseIDSeq)
		assertTrue(ok)

		return LeaseGetResult{
			Status:  LeaseGetStatusLeaseGranted,
			LeaseID: p.leaseIDSeq,
		}
	}

	if !bytes.Equal(result.key, key) {
		// TODO hash equals but key not equals
		return LeaseGetResult{}
	}

	if result.status == entryStatusLeasing {
		return LeaseGetResult{
			Status: LeaseGetStatusLeaseRejected,
		}
	}

	return LeaseGetResult{
		Status: LeaseGetStatusExisted,
		Value:  result.value,
	}
}

func (p *Partition) leaseSet(hash uint64, key []byte, leaseID uint64, version uint64, value []byte) {
	// TODO Must Not be false
	ok := p.putValue(hash, key, version, value)
	assertTrue(ok)
}
