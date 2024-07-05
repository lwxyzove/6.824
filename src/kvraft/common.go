package kvraft

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
)

const (
	OpGet    = "Get"
	OpPut    = "Put"
	OpAppend = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string

	OpType     string
	ClientId   int64
	OpSequence int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string

	ClientId   int64
	OpSequence int
}

type GetReply struct {
	Err   Err
	Value string
}

func WithLock(l sync.Locker, f func()) {
	l.Lock()
	defer l.Unlock()
	f()
}

func maxI64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
