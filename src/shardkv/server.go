package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType     string
	ClientId   int64
	OpSequence int
	Key        string
	Value      string

	Config *shardctrler.Config
}

type OpResult struct {
	Error Err
	Value string
}

type shard struct {
	state   int
	kvStore map[string]string
}

func (s *shard) Get(key string) string  { return s.kvStore[key] }
func (s *shard) Put(key, val string)    { s.kvStore[key] = val }
func (s *shard) Append(key, val string) { s.kvStore[key] += val }
func (s *shard) Copy() map[string]string {
	cp := make(map[string]string, len(s.kvStore))
	for k, v := range s.kvStore {
		cp[k] = v
	}
	return cp
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead        int32 // set by Kill()
	persister   *raft.Persister
	shards      map[int]*shard
	sc          *shardctrler.Clerk
	lastConf    shardctrler.Config
	currentConf shardctrler.Config
	lastApplied int

	mAcks  map[int64]int
	mAckCh map[int]chan OpResult
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	shardid := key2shard(args.Key)
	kv.mu.Lock()
	if _, ok := kv.shards[shardid]; !ok {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	logIdx, _, isLeader := kv.rf.Start(Op{
		ClientId:   args.ClientId,
		Key:        args.Key,
		OpType:     OpGet,
		OpSequence: args.OpSequence,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if _, ok := kv.mAckCh[logIdx]; !ok {
		kv.mAckCh[logIdx] = make(chan OpResult)
	}
	rc := kv.mAckCh[logIdx]
	kv.mu.Unlock()

	DPrintf("KVServer-%d Received Req Get %v, logIndex=%d", kv.me, args, logIdx)
	select {
	case res := <-rc:
		kv.mu.Lock()
		delete(kv.mAckCh, logIdx)
		kv.mu.Unlock()
		reply.Err, reply.Value = res.Error, res.Value
	case <-time.After(300 * time.Millisecond):
		reply.Err = ErrTimeOut
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	shardid := key2shard(args.Key)
	kv.mu.Lock()
	if _, ok := kv.shards[shardid]; !ok {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if args.OpSequence <= kv.mAcks[args.ClientId] {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	logIdx, _, isLeader := kv.rf.Start(Op{
		ClientId:   args.ClientId,
		Key:        args.Key,
		Value:      args.Value,
		OpType:     OpPut,
		OpSequence: args.OpSequence,
	})
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	if _, ok := kv.mAckCh[logIdx]; !ok {
		kv.mAckCh[logIdx] = make(chan OpResult)
	}
	rc := kv.mAckCh[logIdx]
	kv.mu.Unlock()

	DPrintf("KVServer-%d Received Req Put %v, logIndex=%d", kv.me, args, logIdx)

	select {
	case res := <-rc:
		kv.mu.Lock()
		delete(kv.mAckCh, logIdx)
		kv.mu.Unlock()
		reply.Err = res.Error
	case <-time.After(200 * time.Millisecond):
		reply.Err = ErrTimeOut
	}
}

func (kv *ShardKV) snapShot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shards)
	e.Encode(kv.mAcks) //持久化每个client的最大已执行过的写请求
	e.Encode(kv.lastApplied)
	e.Encode(kv.lastConf)
	e.Encode(kv.currentConf)
	return w.Bytes()
}

func (kv *ShardKV) parseSnapShot(snapShot []byte) {
	if len(snapShot) != 0 {
		r := bytes.NewBuffer(snapShot)
		e := labgob.NewDecoder(r)
		e.Decode(&kv.shards)
		e.Decode(&kv.mAcks)
		e.Decode(&kv.lastApplied)
		e.Decode(&kv.lastConf)
		e.Decode(&kv.currentConf)
	}
}

func (kv *ShardKV) trySnapShot() {
	if kv.maxraftstate == -1 {
		return
	}
	if kv.persister.RaftStateSize() > kv.maxraftstate {
		kv.rf.Snapshot(kv.lastApplied, kv.snapShot())
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.sc = shardctrler.MakeClerk(ctrlers)
	kv.mAcks = map[int64]int{}
	kv.mAckCh = map[int]chan OpResult{}
	kv.parseSnapShot(persister.ReadSnapshot())

	go kv.Process()
	go kv.loop(kv.updateConfig, 100*time.Millisecond)
	go kv.loop(kv.migrateShards, 100*time.Millisecond)

	return kv
}

func (kv *ShardKV) loop(fn func(), t time.Duration) {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			fn()
		}
		time.Sleep(t)
	}
}

func (kv *ShardKV) updateConfig() {
	kv.mu.Lock()
	for _, shard := range kv.shards {
		if shard.state != Severing {
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()
	nConf := kv.sc.Query(kv.currentConf.Num + 1)

	kv.mu.Lock()
	mConfNUm := kv.currentConf.Num
	kv.mu.Unlock()
	if nConf.Num == mConfNUm+1 {
		kv.rf.Start(Op{OpType: OpConfig, Config: &nConf})
		DPrintf("ShardKv: %d updating config: %+v", kv.me, nConf)
	}
}

func (kv *ShardKV) migrateShards()

func (kv *ShardKV) Process() {
	for applyMsg := range kv.applyCh {
		if applyMsg.CommandValid {
			msg := applyMsg.Command.(Op)
			kv.mu.Lock()
			if applyMsg.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = applyMsg.CommandIndex
			switch msg.OpType {
			case OpGet, OpPut, OpAppend:
				kv.ProcessClientRequest(&msg)
			case OpConfig:
				kv.ProcessConfigUpdate(&msg)
			}
			kv.trySnapShot()
			kv.mu.Unlock()
		} else if applyMsg.SnapshotValid {
			kv.mu.Lock()
			kv.parseSnapShot(applyMsg.Snapshot)
			kv.lastApplied = applyMsg.SnapshotIndex
			kv.mu.Unlock()
			kv.rf.Snapshot(applyMsg.SnapshotIndex, applyMsg.Snapshot)
		}
	}
}

func (kv *ShardKV) ProcessClientRequest(op *Op) {
	shardid := key2shard(op.Key)
	reply := OpResult{Error: OK}
	if shard, ok := kv.shards[shardid]; !ok || shard.state != Severing {
		reply.Error = ErrWrongGroup
	} else {
		switch op.OpType {
		case OpPut:
			shard.Put(op.Key, op.Value)
		case OpAppend:
			shard.Append(op.Key, op.Value)
		case OpGet:
			reply.Value = shard.Get(op.Key)
		}
	}

	_, isLeader := kv.rf.GetState()
	ackCh, ok := kv.mAckCh[kv.lastApplied]
	if isLeader && ok {
		select {
		case ackCh <- reply:
			DPrintf("KVServer-%d Process Notify CkId=%d msg=%v logidx=%d", kv.me, op.ClientId, op, kv.lastApplied)
		default: //do not wait in cass holding lock long time
			DPrintf("KVServer-%d Notify CkId=%d msg=%v logidx=%d timeout", kv.me, op.ClientId, op, kv.lastApplied)
		}
	}
}

func (kv *ShardKV) ProcessConfigUpdate(op *Op) {
	nConf := op.Config
	kv.lastConf, kv.currentConf = kv.currentConf, *nConf

}
