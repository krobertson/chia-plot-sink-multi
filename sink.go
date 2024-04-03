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
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
)

type sink struct {
	plots       map[string]*plotPath
	sortedPlots []*plotPath
	sortMutex   sync.Mutex
	transfers   atomic.Int64
	listener    net.Listener
}

// newSink will create a the sink server process and validate all of
// the provided plot paths. It will return an error if any of the paths do not
// exist, or are not a directory.
func newSink(paths []string) (*sink, error) {
	s := &sink{
		plots:       make(map[string]*plotPath),
		sortedPlots: make([]*plotPath, 0),
	}

	// validate the plots exist and add them in
	for _, p := range paths {
		p, err := filepath.Abs(p)
		if err != nil {
			log.Printf("Path %s failed expansion, skipping: %v", p, err)
			continue
		}

		fi, err := os.Stat(p)
		if err != nil {
			log.Printf("Path %s failed validation, skipping: %v", p, err)
			continue
		}

		if !fi.IsDir() {
			log.Printf("Path %s is not a directory, skipping", p)
			continue
		}

		pp := &plotPath{path: p}
		pp.updateFreeSpace()
		s.plots[p] = pp
		s.sortedPlots = append(s.sortedPlots, pp)

		log.Printf("Registred plot path: %s [%s free / %s total]",
			p, humanize.IBytes(pp.freeSpace), humanize.IBytes(pp.totalSpace))
	}

	// ensure we have at least one
	if len(s.sortedPlots) == 0 {
		return nil, fmt.Errorf("at least one valid plot path must be specified")
	}

	// sort the paths
	s.sortPaths()

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
	// pick a plot. This should return the one with the most free space that
	// isn't busy. we want to lock early
	plot := s.pickPlot()
	if plot == nil {
		conn.Close()
		return
	}

	// check if we're maxed on concurrent transfers
	if s.transfers.Load() >= int64(concurrency) {
		log.Print("Request to store plot, but at max transfers")
		conn.Close()
		return
	}

	// lock the path
	plot.mutex.Lock()
	defer plot.mutex.Unlock()
	plot.busy.Store(true)
	defer plot.busy.Store(false)
	s.transfers.Add(1)
	defer s.transfers.Add(-1)

	// transfer the file to fast local storage
	filename, tmpfile, ok := s.handleTransfer(conn, plot)
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
	s.sortPaths()
}

func (s *sink) handleTransfer(conn net.Conn, plot *plotPath) (string, string, bool) {
	defer conn.Close()

	// receive the file size bytes
	sizeBytes := make([]byte, 8)
	_, err := conn.Read(sizeBytes)
	if err != nil {
		log.Printf("Failed to receive file size: %v", err)
		return "", "", false
	}
	size := convertBytesToUInt64(sizeBytes)

	// check if we have enough free space
	if plot.freeSpace <= size {
		log.Printf("Request to store plot, but not enough space (%s / %s)", humanize.Bytes(size), humanize.Bytes(plot.freeSpace))
		return "", "", false
	}

	// send response
	conn.Write([]byte{1})

	// receive filename length
	fnlenBytes := make([]byte, 2)
	_, err = conn.Read(fnlenBytes)
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
	tmpfile := filepath.Join(tmpdir, filename+".tmp")
	os.Remove(tmpfile)
	f, err := os.Create(tmpfile)
	if err != nil {
		log.Printf("Failed to open file at %s: %v", tmpfile, err)
		return "", "", false
	}
	defer f.Close()

	// perform the copy
	log.Printf("Receiving plot at %s", tmpfile)
	start := time.Now()
	bytes, err := io.Copy(f, conn)
	if err != nil {
		log.Printf("Failure while writing plot %s: %v", tmpfile, err)
		f.Close()
		os.Remove(tmpfile)
		return "", "", false
	}

	// log successful and some metrics
	seconds := time.Since(start).Seconds()
	log.Printf("Successfully stored %s (%s, %f secs, %s/sec)",
		tmpfile, humanize.IBytes(uint64(bytes)), seconds, humanize.Bytes(uint64(float64(bytes)/seconds)))

	return filename, tmpfile, true
}

func (s *sink) handleMove(plot *plotPath, filename, tmpfile string) bool {
	tf, err := os.Open(tmpfile)
	if err != nil {
		log.Printf("Failed to open tmpfile: %v", err)
		return false
	}
	defer tf.Close()

	dstfile := filepath.Join(plot.path, filename)
	tmpdstfile := dstfile + ".tmp"

	f, err := os.Create(tmpdstfile)
	if err != nil {
		log.Printf("Failed to open dest file: %v", err)
		return false
	}
	defer f.Close()

	// TODO: handle errors/failures at this point?

	// perform the copy
	start := time.Now()
	bytes, err := io.Copy(f, tf)
	if err != nil {
		log.Printf("Failure while moving plot %s: %v", tmpfile, err)
		os.Remove(tmpdstfile)
		plot.pause()
		return false
	}

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
