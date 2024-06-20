package raft

import "log"

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func entry(logs []LogEntry) []LogEntry {
	if len(logs) < 10 {
		return logs
	}
	return logs[len(logs)-10:]
}
