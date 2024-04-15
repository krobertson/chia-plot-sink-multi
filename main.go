package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"gopkg.in/yaml.v3"
)

var (
	port    int
	cfgFile string
)

func main() {
	flag.IntVar(&port, "p", 1337, "port to listen on")
	flag.StringVar(&cfgFile, "c", "config.yaml", "config file for locations")
	flag.Parse()

	// read config file
	b, err := os.ReadFile(cfgFile)
	if err != nil {
		log.Fatal("Failed to read config file", err)
	}
	var cfg *config
	err = yaml.Unmarshal(b, &cfg)
	if err != nil {
		log.Fatal("Failed to parse configuration", err)
	}

	// intialize server
	s, err := newSink(cfg)
	if err != nil {
		log.Fatal("Failed to initialize sink", err)
	}

	// add signal handler for shutdown
	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt, syscall.SIGTERM)
		<-sigint

		// close the listener
		s.listener.Close()
	}()

	// loop for connections
	log.Print("Ready")
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Print("Failed to accept connection", conn)
			break
		}
		go s.handleConnection(conn)
	}

	// wait for existing transfers to finish
	s.wg.Wait()
}
