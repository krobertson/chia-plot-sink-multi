// Copyright © 2024 Ken Robertson <ken@invalidlogic.com>

package main

import (
	"cmp"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

type plotPath struct {
	path       string
	busy       atomic.Bool
	paused     atomic.Bool
	freeSpace  uint64
	totalSpace uint64
	mutex      sync.Mutex
}

// updateFreeSpace will get the filesystem stats and update the free and total
// space on the plotPath. This primarily should be done with the plotPath mutex
// locked.
func (p *plotPath) updateFreeSpace() {
	var stat unix.Statfs_t
	unix.Statfs(p.path, &stat)

	p.freeSpace = stat.Bavail * uint64(stat.Bsize)
	p.totalSpace = stat.Blocks * uint64(stat.Bsize)
}

// pause is used to temporarily pause selecting the specified path as an option
// for storing plots. This is primarily used if storing a plot fails. It may be
// an intermittiend issue, but this allows retrying it later.
func (p *plotPath) pause() {
	p.paused.Store(true)
	time.AfterFunc(5*time.Minute, func() {
		p.paused.Store(false)
	})
}

// sortPaths will update the order of the plotPaths inside the sink's
// sortedPaths slice. This should be done after every file transfer when the
// free space is updated.
func (s *sink) sortPaths() {
	s.sortMutex.Lock()
	defer s.sortMutex.Unlock()

	slices.SortStableFunc(s.sortedPlots, func(a, b *plotPath) int {
		return cmp.Compare(b.freeSpace, a.freeSpace)
	})
}

// pickPlot will return which plot path would be most ideal for the current
// request. It will order the one with the most free space that doesn't already
// have an active transfer.
func (s *sink) pickPlot() *plotPath {
	s.sortMutex.Lock()
	defer s.sortMutex.Unlock()

	for _, v := range s.sortedPlots {
		if v.busy.Load() {
			continue
		}
		if v.paused.Load() {
			continue
		}
		return v
	}
	return nil
}
