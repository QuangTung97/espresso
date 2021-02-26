package allocator

import (
	"math"
	"reflect"
	"unsafe"
)

// Slab ...
type Slab struct {
	buddy           *Buddy
	elemSize        uint32
	chunkSizeLog    uint32
	numElemPerChunk uint32

	currentChunkAddr uint32
	freeListIndex    uint32
}

// NewSlab ...
func NewSlab(buddy *Buddy, elemSize uint32, chunkSizeLog uint32) *Slab {
	return &Slab{
		buddy:           buddy,
		elemSize:        elemSize,
		chunkSizeLog:    chunkSizeLog,
		numElemPerChunk: (1 << chunkSizeLog) / elemSize,

		currentChunkAddr: buddyNullPtr,
		freeListIndex:    0,
	}
}

// SetElem ...
func (s *Slab) SetElem(addr uint32, data []byte) {
	dest := s.GetElem(addr)
	copy(dest, data)
}

// GetElem ...
func (s *Slab) GetElem(addr uint32) []byte {
	var result []byte
	p := (*reflect.SliceHeader)(unsafe.Pointer(&result))
	p.Data = uintptr(s.buddy.ToRealAddr(addr))
	p.Len = int(s.elemSize)
	p.Cap = int(s.elemSize)
	return result
}

// Allocate ...
func (s *Slab) Allocate() (uint32, bool) {
	if s.currentChunkAddr == buddyNullPtr {
		chunkAddr, ok := s.buddy.Allocate(s.chunkSizeLog)
		if !ok {
			return 0, false
		}
		s.currentChunkAddr = chunkAddr
	}

	result := s.currentChunkAddr + s.freeListIndex*s.elemSize
	s.freeListIndex++
	if s.freeListIndex >= s.numElemPerChunk {
		s.freeListIndex = 0
		s.currentChunkAddr = buddyNullPtr
	}

	return result, true
}

func (s *Slab) copyData(dest uint32, src uint32) {
	destElem := s.GetElem(dest)
	srcElem := s.GetElem(src)
	copy(destElem, srcElem)
}

func (s *Slab) putBackChunkToBuddyIfFree() {
	if s.freeListIndex == 0 {
		s.buddy.Deallocate(s.currentChunkAddr, s.chunkSizeLog)
		s.currentChunkAddr = buddyNullPtr
	}
}

// Deallocate can require move some item in an address to *addr*
// Can NOT access the *movedAddr*, the content already in the *addr*
func (s *Slab) Deallocate(addr uint32) (uint32, bool) {
	if s.currentChunkAddr == buddyNullPtr {
		mask := uint32(math.MaxUint32) << s.chunkSizeLog
		s.currentChunkAddr = addr & mask
		s.freeListIndex = s.numElemPerChunk
	}

	movedAddr := s.currentChunkAddr + (s.freeListIndex-1)*s.elemSize
	if movedAddr == addr {
		s.freeListIndex--
		s.putBackChunkToBuddyIfFree()
		return 0, false
	}

	s.freeListIndex--
	s.copyData(addr, movedAddr)
	s.putBackChunkToBuddyIfFree()

	return movedAddr, true
}
