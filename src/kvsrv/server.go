package kvsrv

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

type KVServer struct {
	mu sync.Mutex

	kvs    map[string]string
	record map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.kvs[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch args.MsgType {
	case NOTIFY:
		delete(kv.record, args.Sequence)
	case MODIFY:
		if v, ok := kv.record[args.Sequence]; ok {
			reply.Value = v
		} else {
			kv.record[args.Sequence] = args.Value
			kv.kvs[args.Key] = args.Value
			reply.Value = args.Value
		}
	}

	DPrintf("after Put: %v", kv.kvs)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch args.MsgType {
	case NOTIFY:
		delete(kv.record, args.Sequence)
	case MODIFY:
		if v, ok := kv.record[args.Sequence]; ok {
			reply.Value = v
		} else {
			reply.Value = kv.kvs[args.Key]
			kv.record[args.Sequence] = kv.kvs[args.Key]
			kv.kvs[args.Key] += args.Value
		}
	}
	DPrintf("after Append: %v", kv.kvs)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvs = map[string]string{}
	kv.record = map[int64]string{}
	return kv
}
