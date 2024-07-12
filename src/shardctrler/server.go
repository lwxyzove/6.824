package shardctrler

import (
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
	mAcks   map[int64]int
	mAckCh  map[int]chan struct{}
}

const (
	OpJoin = iota + 1
	OpLeave
	OpMove
	OpGet
)

type Op struct {
	// Your data here.
	OpType  int
	Servers map[int][]string
	Gids    []int
	Shard   int
	Gid     int
	Num     int

	ClientId   int64
	OpSequence int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	sc.mu.Lock()
	if args.OpSequence <= sc.mAcks[args.ClientId] {
		sc.mu.Unlock()
		reply.Err = OK
		return
	}
	logIdx, _, isLeader := sc.rf.Start(Op{
		OpType:     OpJoin,
		Servers:    args.Servers,
		ClientId:   args.ClientId,
		OpSequence: args.OpSequence,
	})
	if !isLeader {
		sc.mu.Unlock()
		reply.WrongLeader = true
		return
	}
	defer func() {
		DPrintf("Leader ShardCtrler-%d Received Join Request %v, Reply: %v", sc.me, args, reply)
	}()
	if _, ok := sc.mAckCh[logIdx]; !ok {
		sc.mAckCh[logIdx] = make(chan struct{})
	}
	rc := sc.mAckCh[logIdx]
	sc.mu.Unlock()

	select {
	case <-rc:
		reply.Err = OK
	case <-time.After(200 * time.Millisecond):
		reply.Err = Timeout
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	if args.OpSequence <= sc.mAcks[args.ClientId] {
		sc.mu.Unlock()
		reply.Err = OK
		return
	}
	logIdx, _, isLeader := sc.rf.Start(Op{
		OpType:     OpLeave,
		Gids:       args.GIDs,
		ClientId:   args.ClientId,
		OpSequence: args.OpSequence,
	})
	if !isLeader {
		sc.mu.Unlock()
		reply.WrongLeader = true
		return
	}
	defer func() {
		DPrintf("Leader ShardCtrler-%d Received Leave Request %v, Reply: %v", sc.me, args, reply)
	}()

	if _, ok := sc.mAckCh[logIdx]; !ok {
		sc.mAckCh[logIdx] = make(chan struct{})
	}
	rc := sc.mAckCh[logIdx]
	sc.mu.Unlock()

	select {
	case <-rc:
		reply.Err = OK
	case <-time.After(200 * time.Millisecond):
		reply.Err = Timeout
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	if args.OpSequence <= sc.mAcks[args.ClientId] {
		sc.mu.Unlock()
		reply.Err = OK
		return
	}
	logIdx, _, isLeader := sc.rf.Start(Op{
		OpType:     OpMove,
		Shard:      args.Shard,
		Gid:        args.GID,
		ClientId:   args.ClientId,
		OpSequence: args.OpSequence,
	})
	if !isLeader {
		sc.mu.Unlock()
		reply.WrongLeader = true
		return
	}
	defer func() {
		DPrintf("Leader ShardCtrler-%d Received Move Request %v, Reply: %v", sc.me, args, reply)
	}()
	if _, ok := sc.mAckCh[logIdx]; !ok {
		sc.mAckCh[logIdx] = make(chan struct{})
	}
	rc := sc.mAckCh[logIdx]
	sc.mu.Unlock()

	select {
	case <-rc:
		reply.Err = OK
	case <-time.After(200 * time.Millisecond):
		reply.Err = Timeout
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	logIdx, _, isLeader := sc.rf.Start(Op{
		OpType:     OpGet,
		Num:        args.Num,
		ClientId:   args.ClientId,
		OpSequence: args.OpSequence,
	})
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	defer func() {
		DPrintf("Leader ShardCtrler-%d Received Query Request %v, Reply: %v, logIndex=%d", sc.me, args, reply, logIdx)
	}()
	sc.mu.Lock()
	if _, ok := sc.mAckCh[logIdx]; !ok {
		sc.mAckCh[logIdx] = make(chan struct{})
	}
	rc := sc.mAckCh[logIdx]
	sc.mu.Unlock()

	select {
	case <-rc:
		reply.Err = OK
		sc.mu.Lock()
		if args.Num == -1 || args.Num > len(sc.configs)-1 {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
		sc.mu.Unlock()
	case <-time.After(200 * time.Millisecond):
		reply.Err = Timeout
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.mAckCh = map[int]chan struct{}{}
	sc.mAcks = map[int64]int{}
	go sc.Process()

	return sc
}

func (sc *ShardCtrler) Process() {
	for applyMsg := range sc.applyCh {
		if applyMsg.CommandValid {
			msg := applyMsg.Command.(Op)
			logIdx := (applyMsg.CommandIndex)

			sc.mu.Lock()
			//		DPrintf("applying msg: %v, kv.msessions: %d == nil: %v", msg, msg.ClientId, kv.msessions[msg.ClientId] == nil)
			if msg.OpType != OpGet && msg.OpSequence <= sc.mAcks[msg.ClientId] {
				sc.mu.Unlock()
				continue
			}

			switch msg.OpType {
			case OpJoin:
				lastConfig := sc.configs[len(sc.configs)-1]
				nGroups := make(map[int][]string)
				for gid, servers := range lastConfig.Groups {
					nGroups[gid] = servers
				}
				for join, servers := range msg.Servers {
					nGroups[join] = servers
				}
				sc.configs = append(sc.configs, Config{Num: len(sc.configs), Shards: lastConfig.Shards, Groups: nGroups})
				sc.rebalance()
			case OpLeave:
				lastConfig := sc.configs[len(sc.configs)-1]
				nGroups := make(map[int][]string)
				for gid, servers := range lastConfig.Groups {
					nGroups[gid] = servers
				}
				for _, leave := range msg.Gids {
					delete(nGroups, leave)
				}
				sc.configs = append(sc.configs, Config{Num: len(sc.configs), Shards: lastConfig.Shards, Groups: nGroups})
				sc.rebalance()
			case OpMove:
				lastConfig := sc.configs[len(sc.configs)-1]
				nGroups := make(map[int][]string)
				for gid, servers := range lastConfig.Groups {
					nGroups[gid] = servers
				}
				cfg := Config{Num: len(sc.configs), Shards: lastConfig.Shards, Groups: nGroups}
				//	DPrintf("rf.me: %d, msg.Num: %d, msg.Gid: %d, msg.Sequence: %d", sc.me, msg.Num, msg.Gid, msg.OpSequence)
				cfg.Shards[msg.Shard] = msg.Gid
				sc.configs = append(sc.configs, cfg)
			case OpGet:
			}
			sc.mAcks[msg.ClientId] = msg.OpSequence

			_, isLeader := sc.rf.GetState()
			if isLeader {
				DPrintf("after rebalance: %v, groups: %v", sc.configs[len(sc.configs)-1].Shards, sc.configs[len(sc.configs)-1].Groups)
			}
			//		DPrintf("KVServer-%d Excute CkId=%d Msg=%v isLeader=%v", kv.me, msg.ClientId, msg, isLeader)
			ackCh, ok := sc.mAckCh[logIdx]
			sc.mu.Unlock()

			if isLeader && ok {
				//				DPrintf("ShardCtrler-%d Process Excute CkId=%d Msg=%v", sc.me, msg.ClientId, msg)
				select {
				case ackCh <- struct{}{}:
					//					DPrintf("ShardCtrler-%d Process Notify CkId=%d msg=%v logidx=%d", sc.me, msg.ClientId, msg, logIdx)
				case <-time.After(50 * time.Millisecond):
					DPrintf("ShardCtrler-%d Notify CkId=%d msg=%v logidx=%d timeout", sc.me, msg.ClientId, msg, logIdx)
				}
			}
		}
	}
}

func (sc *ShardCtrler) rebalance() {
	conf := &sc.configs[len(sc.configs)-1]
	if len(conf.Groups) == 0 {
		return
	}

	avg := len(conf.Shards) / len(conf.Groups)
	rest := len(conf.Shards) % len(conf.Groups)

	gShardsMap := make(map[int][]int, len(conf.Groups))
	for i, g := range conf.Shards {
		if _, ok := conf.Groups[g]; ok {
			gShardsMap[g] = append(gShardsMap[g], i)
		} else {
			conf.Shards[i] = 0
		}
	}
	gShards := make([]int, 0, len(conf.Groups))
	for g, _ := range conf.Groups {
		gShards = append(gShards, g)
	}

	sort.Slice(gShards, func(i, j int) bool {
		if len(gShardsMap[gShards[i]]) == len(gShardsMap[gShards[j]]) {
			return gShards[i] < gShards[j]
		} else {
			return len(gShardsMap[gShards[i]]) < len(gShardsMap[gShards[j]])
		}
	})

	avalid := []int{}
	for i := range conf.Shards {
		if conf.Shards[i] == 0 || gShardsMap[conf.Shards[i]] == nil {
			avalid = append(avalid, i)
		}
	}

	for i := len(gShards) - 1; i >= 0; i-- {
		ishards := gShardsMap[gShards[i]]
		offset := avg
		if rest > 0 {
			offset = avg + 1
			rest--
		}
		if len(ishards) > offset {
			avalid = append(avalid, ishards[offset:]...)
		} else if len(ishards) < offset {
			diff := min(offset-len(ishards), len(avalid))
			for _, shard := range avalid[:diff] {
				conf.Shards[shard] = gShards[i]
			}
			avalid = avalid[diff:]
		}
	}
}
