package allocator

import "unsafe"

// SlabConfig ...
type SlabConfig struct {
	ElemSize     uint32
	ChunkSizeLog uint32
}

// Config ...
type Config struct {
	Slabs []SlabConfig
}

// Allocator ...
type Allocator struct {
	buddy Buddy
	slabs []*Slab
}

// New ...
func New(conf Config) *Allocator {
	// TODO continue New
	data := make([]uint64, 1<<17)

	result := &Allocator{}
	BuddyInit(&result.buddy, 12, 20, unsafe.Pointer(&data[0]))

	slabs := make([]*Slab, 0, len(conf.Slabs))
	for _, slabConf := range conf.Slabs {
		slabs = append(slabs, NewSlab(&result.buddy, slabConf.ElemSize, slabConf.ChunkSizeLog))
	}
	result.slabs = slabs

	return result
}
