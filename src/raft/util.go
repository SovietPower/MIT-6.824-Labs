package raft

import "log"

// Debugging
const Debug = 0

// TODO:
// TestBackup2B TestFigure8Unreliable2C

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
