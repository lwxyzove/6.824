package shardkv

import "log"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongGroup     = "ErrWrongGroup"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrTimeOut        = "ErrTimeOut"
	ErrWorngConfigVer = "ErrWorngConfigVer"
)

const (
	OpGet    = "Get"
	OpPut    = "Put"
	OpAppend = "Append"

	OpConfig    = "Config"
	OpMerge     = "Merge"
	OpErase     = "Erase"
	OpToServing = "Serving"
)

const (
	Severing = iota + 1
	Fetching
	Abandoning
	Waiting
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	OpType     string
	ClientId   int64
	OpSequence int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	ClientId   int64
	OpSequence int
}

type GetReply struct {
	Err   Err
	Value string
}

type FetchRequst struct {
	ConfigNum int
	ShardIds  []int
}

type FetchReply struct {
	Err           Err
	ConfigNum     int
	ShardsKvStore map[int]map[string]string
	ClientAcks    map[int64]int
}

type NotifyMigrateOkRequest struct {
	ConfigNum int
	ShardIds  []int
}

type NotifyMigrateOkReply struct {
	Err Err
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func mapi64iCopy(src map[int64]int) map[int64]int {
	dst := make(map[int64]int, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func mapCopy(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
