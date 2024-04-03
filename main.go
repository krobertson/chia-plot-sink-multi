package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"
)

var (
	port        int
	tmpdir      string
	concurrency int
	srcPaths    arrayFlags
)

func main() {
	flag.IntVar(&port, "p", 1337, "port to listen on")
	flag.StringVar(&tmpdir, "t", "", "directory to temporarily store files")
	flag.IntVar(&concurrency, "c", 5, "maximum concurrent copies from tmp to final")
	flag.Var(&srcPaths, "d", "Plots directories")
	flag.Parse()

	// process srcPaths
	plotPaths := make([]string, 0)
	for _, ep := range srcPaths {
		p, err := filepath.Abs(ep)
		if err != nil {
			log.Printf("Failed to resolve path %s, skipping: %v", ep, err)
			continue
		}

		items, err := os.ReadDir(p)
		if err != nil {
			log.Fatalf("Failed to evaluate path %s, skipping: %v", p, err)
			continue
		}
		for _, de := range items {
			if !de.IsDir() {
				continue
			}

			plotPaths = append(plotPaths, filepath.Join(p, de.Name()))
		}
	}

	// intialize server
	s, err := newSink(plotPaths)
	if err != nil {
		log.Fatal("Failed to initialize sink", err)
	}

	// loop for connections
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Print("Failed to accept connection", conn)
			return
		}
		go s.handleConnection(conn)
	}
}
