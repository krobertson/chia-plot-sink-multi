// Copyright © 2024 Ken Robertson <ken@invalidlogic.com>

package main

import (
	"bytes"
	"encoding/binary"
	"strings"
)

func convertBytesToUInt64(b []byte) uint64 {
	var n uint64
	buf := bytes.NewBuffer(b)
	binary.Read(buf, binary.LittleEndian, &n)
	return n
}

func convertBytesToInt16(b []byte) int16 {
	var n int16
	buf := bytes.NewBuffer(b)
	binary.Read(buf, binary.LittleEndian, &n)
	return n
}

// arrayFlags can be used with flags.Var to specify the a command line argument
// multiple timmes.
type arrayFlags []string

// String returns a basic string concationation of all values.
func (i *arrayFlags) String() string {
	return strings.Join(*i, ", ")
}

// Set is used to append a new value to the array by flags.Var.
func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}
