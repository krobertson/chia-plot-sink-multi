// Copyright © 2024 Ken Robertson <ken@invalidlogic.com>

package main

import (
	"cmp"
	"log"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/dustin/go-humanize"
)

type plotGroup struct {
	name        string
	concurrency int64
	transfers   atomic.Int64

	sortedPlots []*plotPath
	sortMutex   sync.RWMutex
}

func newPlotGroup(cfg *configGroup, allowExcessConcurrency bool) (*plotGroup, error) {
	pg := &plotGroup{
		name:        cfg.name,
		concurrency: cfg.Concurrency,
		sortedPlots: make([]*plotPath, 0),
	}

	// validate the plots exist and add them in
	for _, p := range cfg.Paths {
		p, err := filepath.Abs(p)
		if err != nil {
			log.Printf("Path %s failed expansion, skipping: %v", p, err)
			continue
		}

		matches, err := filepath.Glob(p)
		if err != nil {
			log.Printf("Path %s failed globbing, skipping: %v", p, err)
			continue
		}

		for _, m := range matches {
			fi, err := os.Stat(m)
			if err != nil {
				log.Printf("Path %s failed validation, skipping: %v", m, err)
				continue
			}

			if !fi.IsDir() {
				log.Printf("Path %s is not a directory, skipping", m)
				continue
			}

			// FIXME: add checking skip file

			pp := &plotPath{path: m}
			pp.updateFreeSpace()
			pg.sortedPlots = append(pg.sortedPlots, pp)

			log.Printf("Registred plot path: %s [%s free / %s total]",
				m, humanize.IBytes(pp.freeSpace), humanize.IBytes(pp.totalSpace))
		}
	}

	// ensure concurrency doesn't exceed paths
	if !allowExcessConcurrency && pg.concurrency > int64(len(pg.sortedPlots)) {
		pg.concurrency = int64(len(pg.sortedPlots))
	}

	// sort the paths
	pg.sortPaths()

	log.Printf("Plot Group %q ready with concurrency %d.", pg.name, pg.concurrency)

	return pg, nil
}

// sortPaths will update the order of the plotPaths inside the sink's
// sortedPaths slice. This should be done after every file transfer when the
// free space is updated.
func (pg *plotGroup) sortPaths() {
	pg.sortMutex.Lock()
	defer pg.sortMutex.Unlock()

	slices.SortStableFunc(pg.sortedPlots, func(a, b *plotPath) int {
		return cmp.Compare(b.freeSpace, a.freeSpace)
	})
}

// sortCachePaths will update the order of the plotPaths inside the group's
// sortedPaths slice according to the number of transfers. This is used with
// cache plotPaths rather than final destination ones.
func (pg *plotGroup) sortCachePaths() {
	pg.sortMutex.Lock()
	defer pg.sortMutex.Unlock()

	slices.SortStableFunc(pg.sortedPlots, func(a, b *plotPath) int {
		return cmp.Compare(a.transfers.Load(), b.transfers.Load())
	})
}

// pickPlot will return which plot path would be most ideal for the current
// request. It will order the one with the most free space that doesn't already
// have an active transfer.
func (pg *plotGroup) pickPlot(size uint64) *plotPath {
	pg.sortMutex.RLock()
	defer pg.sortMutex.RUnlock()

	if pg.transfers.Load() >= pg.concurrency {
		return nil
	}

	for _, v := range pg.sortedPlots {
		if v.busy.Load() {
			continue
		}
		if v.paused.Load() {
			continue
		}
		// this is sorted by free space, if this one doesn't have enough space,
		// no point to continue.
		if size > v.freeSpace {
			return nil
		}
		return v
	}
	return nil
}

// sortGroups will update the order of the plotGroups inside the sink's
// sortedGrups slice. This should be done after every file transfer when the
// number of transfers is updated.
func (s *sink) sortGroups() {
	s.sortMutex.Lock()
	defer s.sortMutex.Unlock()

	slices.SortStableFunc(s.sortedGroups, func(a, b *plotGroup) int {
		return cmp.Compare(a.transfers.Load(), b.transfers.Load())
	})
}

// pickPlot will return which plot path would be most ideal for the current
// request. It will loop over the available groups, sorted by the number of
// transfers they already have, and return an available plotPath to use.
func (s *sink) pickPlot(size uint64) (*plotGroup, *plotPath) {
	s.sortMutex.RLock()
	defer s.sortMutex.RUnlock()

	for _, pg := range s.sortedGroups {
		pp := pg.pickPlot(size)
		if pp != nil {
			return pg, pp
		}
	}
	return nil, nil
}
