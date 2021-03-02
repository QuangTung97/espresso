package allocator

import "unsafe"

// SlabConfig ...
type SlabConfig struct {
	ElemSize     uint32
	ChunkSizeLog uint32
}

// Config ...
type Config struct {
	MemLimit int
	Slabs    []SlabConfig
}

// Allocator ...
type Allocator struct {
	buddy Buddy
	slabs []*Slab

	memoryUsage int
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

// New ...
func New(conf Config) *Allocator {
	minSizeLog := findMinSizeLog(conf.Slabs)
	sizeMultiple := findSizeMultiple(minSizeLog, conf.MemLimit)

	data := allocateData(minSizeLog, sizeMultiple)

	result := &Allocator{}
	BuddyInit(&result.buddy, minSizeLog, sizeMultiple, unsafe.Pointer(&data[0]))

	slabs := make([]*Slab, 0, len(conf.Slabs))
	for _, slabConf := range conf.Slabs {
		slabs = append(slabs, NewSlab(&result.buddy, slabConf.ElemSize, slabConf.ChunkSizeLog))
	}
	result.slabs = slabs
	result.memoryUsage = 0

	return result
}
