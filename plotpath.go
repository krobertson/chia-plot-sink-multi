// Copyright © 2024 Ken Robertson <ken@invalidlogic.com>

package main

import (
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

type plotPath struct {
	path       string
	transfers  atomic.Int64
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
