package allocator

import "unsafe"

// SlabConfig ...
type SlabConfig struct {
	ElemSize     uint32
	ChunkSizeLog uint32
}

// Config ...
type Config struct {
	MemLimit     int
	LRUEntrySize uint32
	Slabs        []SlabConfig
}

// Allocator ...
type Allocator struct {
	buddy Buddy

	lruSlab *RealSlab

	slabs        []*Slab
	slabSizeList []uint32

	memoryUsage uint64
}

func findMinSizeLog(slabs []SlabConfig) uint32 {
	min := slabs[0].ChunkSizeLog
	for _, s := range slabs[1:] {
		if min > s.ChunkSizeLog {
			min = s.ChunkSizeLog
		}
	}
	return min
}

func findSizeMultiple(minSizeLog uint32, limit int) uint32 {
	mask := 1<<minSizeLog - 1
	return uint32((limit + mask) >> minSizeLog)
}

func allocateData(minSizeLog uint32, sizeMultiple uint32) []uint64 {
	return make([]uint64, sizeMultiple<<(minSizeLog-6))
}

func allocatorValidateConfig(conf Config) {
	if conf.MemLimit <= 0 {
		panic("MemLimit must > 0")
	}
	if conf.LRUEntrySize == 0 {
		panic("LRUEntrySize must > 0")
	}
	if len(conf.Slabs) == 0 {
		panic("Slabs list must not empty")
	}
	for _, s := range conf.Slabs {
		if s.ElemSize == 0 {
			panic("ElemSize must > 0")
		}
		if s.ChunkSizeLog == 0 {
			panic("ChunkSizeLog must > 0")
		}
	}
}

// New ...
func New(conf Config) *Allocator {
	allocatorValidateConfig(conf)

	minSizeLog := findMinSizeLog(conf.Slabs)
	sizeMultiple := findSizeMultiple(minSizeLog, conf.MemLimit)

	data := allocateData(minSizeLog, sizeMultiple)

	result := &Allocator{}
	BuddyInit(&result.buddy, minSizeLog, sizeMultiple, unsafe.Pointer(&data[0]))

	result.lruSlab = NewRealSlab(&result.buddy, conf.LRUEntrySize, minSizeLog)

	slabs := make([]*Slab, 0, len(conf.Slabs))
	for _, slabConf := range conf.Slabs {
		slabs = append(slabs, NewSlab(&result.buddy, slabConf.ElemSize, slabConf.ChunkSizeLog))
	}
	result.slabs = slabs

	slabSizeList := make([]uint32, 0, len(conf.Slabs))
	for _, s := range conf.Slabs {
		slabSizeList = append(slabSizeList, s.ElemSize)
	}
	result.slabSizeList = slabSizeList

	result.memoryUsage = 0

	return result
}

func findSlabIndex(sizes []uint32, value uint32) int {
	first := 0
	last := len(sizes)
	for first != last {
		mid := (first + last) >> 1
		if sizes[mid] < value {
			first = mid + 1
		} else {
			last = mid
		}
	}
	return first
}

// GetMemUsage ...
func (a *Allocator) GetMemUsage() uint64 {
	return a.memoryUsage
}

// Allocate ...
func (a *Allocator) Allocate(size uint32) (uint32, bool) {
	index := findSlabIndex(a.slabSizeList, size)
	slab := a.slabs[index]

	prevUsage := slab.GetMemUsage()
	addr, ok := slab.Allocate()
	nextUsage := slab.GetMemUsage()

	a.memoryUsage += nextUsage - prevUsage
	return addr, ok
}

// Deallocate ...
func (a *Allocator) Deallocate(addr uint32, size uint32) (movedAddr uint32, needMove bool) {
	index := findSlabIndex(a.slabSizeList, size)
	slab := a.slabs[index]

	prevUsage := slab.GetMemUsage()
	movedAddr, needMove = slab.Deallocate(addr)
	nextUsage := slab.GetMemUsage()

	a.memoryUsage = a.memoryUsage - prevUsage + nextUsage

	return
}

// GetLRUSlab ...
func (a *Allocator) GetLRUSlab() *RealSlab {
	return a.lruSlab
}
