// Copyright © 2024 Ken Robertson <ken@invalidlogic.com>

package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/brk0v/directio"
	"github.com/dustin/go-humanize"
)

type sink struct {
	sortedGroups []*plotGroup
	sortMutex    sync.RWMutex
	cacheGroup   *plotGroup
	listener     net.Listener
	wg           sync.WaitGroup
}

// newSink will create a the sink server process and validate all of
// the provided plot paths. It will return an error if any of the paths do not
// exist, or are not a directory.
func newSink(cfg *config) (*sink, error) {
	s := &sink{
		sortedGroups: make([]*plotGroup, 0),
	}

	// populate cache settings
	cfg.Cache.name = "cache"
	cacheGroup, err := newPlotGroup(cfg.Cache)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize cache group: %v", err)
	}
	s.cacheGroup = cacheGroup
	s.cacheGroup.sortCachePaths()

	// populage destination groups
	for n, dst := range cfg.Destinations {
		dst.name = n
		pg, err := newPlotGroup(dst)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize destination group: %v", err)
		}
		s.sortedGroups = append(s.sortedGroups, pg)
	}

	// bind to the port
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatal("Failed to bind to port", err)
	}
	log.Printf("Listening on %d...", port)
	s.listener = l

	return s, nil
}

// handleConnection faciliates the transfer of plot files from the plotters to
// the sink. It encapculates a single request and is ran within its own
// goroutine.
func (s *sink) handleConnection(conn net.Conn) {
	s.wg.Add(1)
	defer s.wg.Done()

	// receive the file size bytes
	sizeBytes := make([]byte, 8)
	_, err := conn.Read(sizeBytes)
	if err != nil {
		log.Printf("Failed to receive file size: %v", err)
		conn.Close()
		return
	}
	size := convertBytesToUInt64(sizeBytes)

	// pick a plot. This should return the one with the most free space that
	// isn't busy. we want to lock early
	pg, plot := s.pickPlot(size)
	if plot == nil {
		conn.Close()
		log.Printf("Request to store plot, but no eligible plot found (%s / %s)", humanize.Bytes(size), humanize.Bytes(plot.freeSpace))
		return
	}

	// try and lock. This is mostly to protect against a hypothetical race
	// condition where a second connection could pick the same plot before it is
	// locked and marked busy.
	//
	// Even if this was hit, it would self resolve once the first transfer was
	// done, but would cause a slowdown and lower overall throughput.
	if !plot.mutex.TryLock() {
		conn.Close()
		log.Print("Lock race condition hit! Closing and returning.")
		return
	}

	// lock and handle stuff, there is a lot
	defer plot.mutex.Unlock()
	plot.busy.Store(true)
	defer plot.busy.Store(false)
	s.cacheGroup.transfers.Add(1)
	defer s.cacheGroup.transfers.Add(-1)
	pg.transfers.Add(1)
	defer s.sortGroups()
	defer pg.transfers.Add(-1)
	s.sortGroups()

	// pick the cache plot
	cachePlot := s.cacheGroup.pickPlot(size)
	if cachePlot == nil {
		conn.Close()
		log.Print("Failed to get a cache plot to use")
		return
	}
	cachePlot.transfers.Add(1)
	defer s.cacheGroup.sortCachePaths()
	defer cachePlot.transfers.Add(-1)
	s.cacheGroup.sortCachePaths()

	// transfer the file to fast local storage
	filename, tmpfile, ok := s.handleTransfer(conn, cachePlot, plot)
	if !ok {
		// conn already closed
		return
	}

	// move it to final disk
	ok = s.handleMove(plot, filename, tmpfile)
	if ok {
		os.Remove(tmpfile)
	}

	// update free space
	plot.updateFreeSpace()
	cachePlot.updateFreeSpace()
	pg.sortPaths()
}

// handleTransfer takes care of receiving the plot from the remote host and
// storing on the temporary NVME/SSDs. It returns the filename of the plot, the
// path to the temp storage location, and a bool indicating success. At the end,
// it closes the remote connection regardless of success.
func (s *sink) handleTransfer(conn net.Conn, cachePlot, plot *plotPath) (string, string, bool) {
	defer conn.Close()

	// send response acknowledging to continue
	conn.Write([]byte{1})

	// receive filename length
	fnlenBytes := make([]byte, 2)
	_, err := conn.Read(fnlenBytes)
	if err != nil {
		log.Printf("Failed to receive filename length: %v", err)
		return "", "", false
	}
	fnlen := convertBytesToInt16(fnlenBytes)

	// receive filename
	filenameBytes := make([]byte, fnlen)
	_, err = conn.Read(filenameBytes)
	if err != nil {
		log.Printf("Failed to receive filename: %v", err)
		return "", "", false
	}
	filename := string(filenameBytes)

	// open the file and transfer
	tmpfile := filepath.Join(cachePlot.path, filename+".tmp")
	os.Remove(tmpfile)
	flags := os.O_WRONLY | os.O_EXCL | os.O_CREATE | syscall.O_DIRECT
	f, err := os.OpenFile(tmpfile, flags, 0644)
	if err != nil {
		log.Printf("Failed to open file at %s: %v", tmpfile, err)
		return "", "", false
	}
	defer f.Close()

	// open directio writter
	dio, err := directio.New(f)
	if err != nil {
		log.Printf("Failed to create directio writter: %v", err)
		return "", "", false
	}
	defer dio.Flush()

	// perform the copy
	log.Printf("Receiving plot %s from %s", filename, conn.RemoteAddr().String())
	start := time.Now()
	bytes, err := io.Copy(dio, conn)
	if err != nil {
		log.Printf("Failure while writing plot %s: %v", tmpfile, err)
		dio.Flush()
		f.Close()
		os.Remove(tmpfile)
		plot.pause()
		return "", "", false
	}

	// rename it so we know it was completed
	dstfile := filepath.Join(cachePlot.path, filename)
	err = os.Rename(tmpfile, dstfile)
	if err != nil {
		log.Printf("Failed to rename temp plot %s: %v", tmpfile, err)
		f.Close()
		os.Remove(tmpfile)
		plot.pause()
		return "", "", false
	}

	// log successful and some metrics
	seconds := time.Since(start).Seconds()
	log.Printf("Successfully stored %s:%s (%s, %f secs, %s/sec)",
		conn.RemoteAddr().String(), filename, humanize.IBytes(uint64(bytes)), seconds, humanize.Bytes(uint64(float64(bytes)/seconds)))

	cachePlot.updateFreeSpace()

	return filename, dstfile, true
}

// handleMove is responsible for moving the plot from the temp location to the
// final hard disk. It returns a bool to indicate success. On success, it will
// remove the temp location. On failure, the file should be moved to a reprocess
// queue to try another disk.
func (s *sink) handleMove(plot *plotPath, filename, tmpfile string) bool {
	tf, err := os.Open(tmpfile)
	if err != nil {
		log.Printf("Failed to open tmpfile: %v", err)
		return false
	}
	defer tf.Close()

	dstfile := filepath.Join(plot.path, filename)
	tmpdstfile := dstfile + ".tmp"

	flags := os.O_WRONLY | os.O_EXCL | os.O_CREATE | syscall.O_DIRECT
	f, err := os.OpenFile(tmpdstfile, flags, 0644)
	if err != nil {
		log.Printf("Failed to open dest file: %v", err)
		return false
	}

	// open directio writter
	dio, err := directio.New(f)
	if err != nil {
		log.Printf("Failed to create directio writter: %v", err)
		return false
	}

	// TODO: handle errors/failures at this point?

	// perform the copy
	start := time.Now()
	bytes, err := io.Copy(dio, tf)
	if err != nil {
		log.Printf("Failure while moving plot %s: %v", tmpfile, err)
		dio.Flush()
		f.Close()
		os.Remove(tmpdstfile)
		plot.pause()
		return false
	}

	// flush and close before rename
	dio.Flush()
	f.Close()

	// rename it so it can be used by the chia harvester
	err = os.Rename(tmpdstfile, dstfile)
	if err != nil {
		log.Printf("Failed to rename final plot %s: %v", tmpdstfile, err)
		os.Remove(tmpdstfile)
		plot.pause()
		return false
	}

	// success
	seconds := time.Since(start).Seconds()
	log.Printf("Moved plot %s (%s, %f secs, %s/sec)",
		dstfile, humanize.IBytes(uint64(bytes)), seconds, humanize.Bytes(uint64(float64(bytes)/seconds)))
	return true
}
