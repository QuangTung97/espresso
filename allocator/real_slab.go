package allocator

import "unsafe"

// RealSlab ...
type RealSlab struct {
	buddy           *Buddy
	elemSize        uint32
	chunkSizeLog    uint32
	numElemPerChunk uint32
	unusedBytes     uint64
	memoryUsage     uint64

	freeList uint32
}

type realSlabListHead struct {
	next uint32
}

// NewRealSlab ...
func NewRealSlab(buddy *Buddy, elemSize uint32, chunkSizeLog uint32) *RealSlab {
	return &RealSlab{
		buddy:           buddy,
		elemSize:        elemSize,
		chunkSizeLog:    chunkSizeLog,
		numElemPerChunk: (1 << chunkSizeLog) / elemSize,
		unusedBytes:     uint64((1 << chunkSizeLog) % elemSize),
		memoryUsage:     0,

		freeList: buddyNullPtr,
	}
}

func (s *RealSlab) contentOfList() []uint32 {
	var result []uint32
	n := s.freeList
	for n != buddyNullPtr {
		result = append(result, n)
		list := (*realSlabListHead)(s.buddy.ToRealAddr(n))
		n = list.next
	}
	return result
}

func (s *RealSlab) initChunk(chunkAddr uint32) {
	s.freeList = chunkAddr
	for i := uint32(0); i < s.numElemPerChunk; i++ {
		addr := chunkAddr + i*s.elemSize
		list := (*realSlabListHead)(s.buddy.ToRealAddr(addr))
		if i == s.numElemPerChunk-1 {
			list.next = buddyNullPtr
		} else {
			list.next = addr + s.elemSize
		}
	}
	s.memoryUsage += s.unusedBytes
}

// Allocate ...
func (s *RealSlab) Allocate() (uint32, bool) {
	if s.freeList == buddyNullPtr {
		chunkAddr, ok := s.buddy.Allocate(s.chunkSizeLog)
		if !ok {
			return 0, false
		}
		s.initChunk(chunkAddr)
	}

	list := (*realSlabListHead)(s.buddy.ToRealAddr(s.freeList))
	result := s.freeList
	s.freeList = list.next
	s.memoryUsage += uint64(s.elemSize)

	return result, true
}

// Deallocate ...
func (s *RealSlab) Deallocate(addr uint32) {
	s.memoryUsage -= uint64(s.elemSize)
	list := (*realSlabListHead)(s.buddy.ToRealAddr(addr))
	list.next = s.freeList
	s.freeList = addr
}

// ToRealAddr ...
func (s *RealSlab) ToRealAddr(addr uint32) unsafe.Pointer {
	return s.buddy.ToRealAddr(addr)
}

// GetMemUsage ...
func (s *RealSlab) GetMemUsage() uint64 {
	return s.memoryUsage
}
