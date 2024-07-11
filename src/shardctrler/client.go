package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	cid         int64
	knownLeader int
	opSequence  int64
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
	// Your code here.
	ck.cid = nrand()
	ck.knownLeader = -1
	ck.opSequence = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	atomic.AddInt64(&ck.opSequence, 1)
	args.Num = num
	args.ClientId = ck.cid
	args.OpSequence = int(ck.opSequence)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			if ck.knownLeader != -1 {
				srv = ck.servers[ck.knownLeader]
			}
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			} else if reply.WrongLeader {
				ck.knownLeader = -1
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	atomic.AddInt64(&ck.opSequence, 1)
	args.Servers = servers
	args.ClientId = ck.cid
	args.OpSequence = int(ck.opSequence)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			if ck.knownLeader != -1 {
				srv = ck.servers[ck.knownLeader]
			}
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			} else if reply.WrongLeader {
				ck.knownLeader = -1
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	atomic.AddInt64(&ck.opSequence, 1)
	args.GIDs = gids
	args.ClientId = ck.cid
	args.OpSequence = int(ck.opSequence)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			if ck.knownLeader != -1 {
				srv = ck.servers[ck.knownLeader]
			}
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			} else if reply.WrongLeader {
				ck.knownLeader = -1
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	atomic.AddInt64(&ck.opSequence, 1)
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.cid
	args.OpSequence = int(ck.opSequence)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			if ck.knownLeader != -1 {
				srv = ck.servers[ck.knownLeader]
			}
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			} else if reply.WrongLeader {
				ck.knownLeader = -1
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
