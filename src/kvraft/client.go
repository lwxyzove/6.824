package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	cid         int64
	knownLeader int
	opSequence  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.cid = nrand()
	ck.knownLeader = -1
	ck.opSequence = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	reply := GetReply{}

	ck.opSequence++
	args.OpSequence = ck.opSequence
	args.Key = key
	args.ClientId = ck.cid

	for i := 0; ; i = (i + 1) % len(ck.servers) {
		if ck.knownLeader != -1 {
			i = ck.knownLeader
		}
		server := ck.servers[i]

		if !server.Call("KVServer.Get", &args, &reply) {
			ck.knownLeader = -1
		} else {
			switch reply.Err {
			case OK:
				DPrintf("Clerk-%d call Get response server=%d, ... args = %v, reply=%v", ck.cid, i, args, reply)
				ck.knownLeader = i
				return reply.Value
			case ErrNoKey:
				ck.knownLeader = i
				return ""
			case ErrWrongLeader:
				ck.knownLeader = -1
			case ErrTimeOut:
				ck.knownLeader = -1
				time.Sleep(50 * time.Millisecond)
			}
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{}
	reply := PutAppendReply{}

	ck.opSequence++
	args.Key = key
	args.Value = value
	args.ClientId = ck.cid
	args.OpSequence = ck.opSequence
	args.OpType = op

	for i := 0; ; i = (i + 1) % len(ck.servers) {
		if ck.knownLeader != -1 {
			i = ck.knownLeader
		}
		server := ck.servers[i]
		if !server.Call("KVServer."+op, &args, &reply) {
			ck.knownLeader = -1
		} else {
			DPrintf("Clerk-%d call PutAppend response server=%d, ... args = %v, reply=%v", ck.cid, i, args, reply)
			switch reply.Err {
			case OK:
				ck.knownLeader = i
				return
			case ErrNoKey:
				ck.knownLeader = i
				return
			case ErrWrongLeader:
				ck.knownLeader = -1
			case ErrTimeOut:
				ck.knownLeader = -1
				time.Sleep(50 * time.Millisecond)
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
