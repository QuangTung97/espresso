package allocator

// Slab ...
type Slab struct {
	buddy           *Buddy
	elemSize        uint32
	chunkSizeLog    uint32
	numElemPerChunk uint32
	freeList        uint32
}

type slabListHead struct {
	next uint32
	prev uint32
}

// NewSlab ...
func NewSlab(buddy *Buddy, elemSize uint32, chunkSizeLog uint32) *Slab {
	return &Slab{
		buddy:           buddy,
		elemSize:        elemSize,
		chunkSizeLog:    chunkSizeLog,
		numElemPerChunk: (1 << chunkSizeLog) / elemSize,
		freeList:        buddyNullPtr,
	}
}

func (s *Slab) initChunk(chunkAddr uint32) {
	for i := uint32(0); i < s.numElemPerChunk; i++ {
		addr := chunkAddr + s.elemSize*i
		header := (*slabListHead)(s.buddy.ToRealAddr(addr))

		if i == 0 {
			header.prev = buddyNullPtr
		} else {
			header.prev = addr - s.elemSize
		}

		if i+1 == s.numElemPerChunk {
			header.next = buddyNullPtr
		} else {
			header.next = addr + s.elemSize
		}
	}
	s.freeList = chunkAddr
}

// Allocate ...
func (s *Slab) Allocate() (uint32, bool) {
	if s.freeList == buddyNullPtr {
		chunkAddr, ok := s.buddy.Allocate(s.chunkSizeLog)
		if !ok {
			return 0, false
		}
		s.initChunk(chunkAddr)
	}

	header := (*slabListHead)(s.buddy.ToRealAddr(s.freeList))
	if header.next != buddyNullPtr {
		next := (*slabListHead)(s.buddy.ToRealAddr(header.next))
		next.prev = buddyNullPtr
	}
	result := s.freeList
	s.freeList = header.next

	return result, true
}
