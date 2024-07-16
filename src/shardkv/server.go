package shardkv

import (
	"bytes"
	"fmt"
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

	Config        *shardctrler.Config
	ConfigNum     int
	ClientAck     map[int64]int
	ShardsKvStore map[int]map[string]string
	ShardIds      []int
}

type OpResult struct {
	Error Err
	Value string
}

type Shard struct {
	State   int
	KvStore map[string]string
}

func (s *Shard) String() string          { return fmt.Sprintf("{state: %d, kvStore: %+v}", s.State, s.KvStore) }
func (s *Shard) Get(key string) string   { return s.KvStore[key] }
func (s *Shard) Put(key, val string)     { s.KvStore[key] = val }
func (s *Shard) Append(key, val string)  { s.KvStore[key] += val }
func (s *Shard) Copy() map[string]string { return mapCopy(s.KvStore) }

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
	shards      map[int]*Shard
	sc          *shardctrler.Clerk
	lastConf    shardctrler.Config
	currentConf shardctrler.Config
	lastApplied int

	mAcks  map[int64]int
	mAckCh map[int]chan OpResult
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	defer func() {
		DPrintf("[Group %d][Server %d] Get request args: %+v, reply: %+v", kv.gid, kv.me, *args, *reply)
	}()
	shardid := key2shard(args.Key)
	kv.mu.Lock()
	if kv.currentConf.Shards[shardid] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if _, ok := kv.shards[shardid]; !ok || (kv.shards[shardid].State != Severing && kv.shards[shardid].State != Waiting) {
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
	defer func() {
		DPrintf("[Group %d][Server %d] PutAppend request args: %+v, reply: %+v", kv.gid, kv.me, *args, *reply)
	}()
	shardid := key2shard(args.Key)
	kv.mu.Lock()
	if kv.currentConf.Shards[shardid] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if _, ok := kv.shards[shardid]; !ok || (kv.shards[shardid].State != Severing && kv.shards[shardid].State != Waiting) {
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
		OpType:     args.Op,
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

func (kv *ShardKV) FetchKvStore(args *FetchRequst, reply *FetchReply) {

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	defer func() {
		DPrintf("[Group %d][Server %d] FetchKvStore request args: %+v, reply: %+v", kv.gid, kv.me, *args, *reply)
	}()

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ConfigNum != kv.currentConf.Num {
		reply.Err = ErrWorngConfigVer
		return
	}

	shardsKvStore := map[int]map[string]string{}
	for _, id := range args.ShardIds {
		if kv.shards[id].State == Abandoning {
			shardsKvStore[id] = kv.shards[id].Copy()
		}
	}
	reply.Err = OK
	reply.ConfigNum = kv.currentConf.Num
	reply.ShardsKvStore = shardsKvStore
	reply.ClientAcks = mapi64iCopy(kv.mAcks)
}

func (kv *ShardKV) MigrateOk(args *NotifyMigrateOkRequest, reply *NotifyMigrateOkReply) {

	kv.mu.Lock()
	if args.ConfigNum != kv.currentConf.Num {
		kv.mu.Unlock()
		reply.Err = ErrWorngConfigVer
		return
	}
	kv.mu.Unlock()

	logIdx, _, isLeader := kv.rf.Start(Op{
		OpType:    OpErase,
		ConfigNum: args.ConfigNum,
		ShardIds:  args.ShardIds,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	defer func() {
		DPrintf("[Group %d][Server %d] MigrateOk request args: %+v, reply: %+v", kv.gid, kv.me, *args, *reply)
	}()

	kv.mu.Lock()
	if _, ok := kv.mAckCh[logIdx]; !ok {
		kv.mAckCh[logIdx] = make(chan OpResult)
	}
	rc := kv.mAckCh[logIdx]
	kv.mu.Unlock()
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
	kv.persister = persister

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.sc = shardctrler.MakeClerk(ctrlers)
	kv.mAcks = map[int64]int{}
	kv.mAckCh = map[int]chan OpResult{}
	kv.shards = map[int]*Shard{}
	kv.parseSnapShot(persister.ReadSnapshot())

	DPrintf("[Group %d][Server %d] start with snapShot lastApplied: %d, currentConf: %+v, currentShard: %+v", kv.gid, kv.me, kv.lastApplied, kv.currentConf, kv.shards)

	go kv.Process()
	go kv.loop(kv.updateConfig, 100*time.Millisecond)
	go kv.loop(kv.migrateShards, 100*time.Millisecond)
	go kv.loop(kv.notifyMigrate, 100*time.Millisecond)

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
		if shard.State != Severing {
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
	}
}

func (kv *ShardKV) migrateShards() {
	kv.mu.Lock()
	groupShards := map[int][]int{}
	for id, shard := range kv.shards {
		if shard.State == Fetching {
			lastOwnGroup := kv.lastConf.Shards[id]
			groupShards[lastOwnGroup] = append(groupShards[lastOwnGroup], id)
		}
	}

	var wg sync.WaitGroup
	for gid, shardids := range groupShards {
		wg.Add(1)
		confNum := kv.currentConf.Num
		servers := kv.lastConf.Groups[gid]
		shardIds := shardids
		go func() {
			defer wg.Done()
			for _, server := range servers {
				shardOwner := kv.make_end(server)
				args := FetchRequst{
					ConfigNum: confNum,
					ShardIds:  shardIds,
				}
				reply := FetchReply{}
				if shardOwner.Call("ShardKV.FetchKvStore", &args, &reply) && reply.Err == OK {
					kv.rf.Start(Op{OpType: OpMerge, ConfigNum: confNum, ShardsKvStore: reply.ShardsKvStore, ClientAck: reply.ClientAcks})
					break
				}
			}
		}()
	}
	kv.mu.Unlock()
	wg.Wait()
}

func (kv *ShardKV) notifyMigrate() {
	kv.mu.Lock()
	groupShards := map[int][]int{}
	for id, shard := range kv.shards {
		if shard.State == Waiting {
			lastOwnGroup := kv.lastConf.Shards[id]
			groupShards[lastOwnGroup] = append(groupShards[lastOwnGroup], id)
		}
	}

	var wg sync.WaitGroup
	for gid, shardids := range groupShards {
		wg.Add(1)
		confNum := kv.currentConf.Num
		servers := kv.lastConf.Groups[gid]
		shardIds := shardids
		go func() {
			defer wg.Done()
			for _, server := range servers {
				shardOwner := kv.make_end(server)
				args := NotifyMigrateOkRequest{
					ConfigNum: confNum,
					ShardIds:  shardIds,
				}
				reply := NotifyMigrateOkReply{}
				if shardOwner.Call("ShardKV.MigrateOk", &args, &reply) && reply.Err == OK {
					kv.rf.Start(Op{OpType: OpToServing, ConfigNum: confNum, ShardIds: args.ShardIds})
					break
				}
			}
		}()
	}
	kv.mu.Unlock()
	wg.Wait()
}

func (kv *ShardKV) Process() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
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
			case OpMerge:
				kv.ProcessKvStoreMerge(&msg)
			case OpErase:
				kv.ProcessKvStoreErase(&msg)
			case OpToServing:
				kv.ProcessShardServing(&msg)
			}
			// /	DPrintf("[Group %d][Server %d] Applys Op: %+v", kv.gid, kv.me, msg)
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
	if shard, ok := kv.shards[shardid]; !ok || (shard.State != Severing && shard.State != Waiting) {
		reply.Error = ErrWrongGroup
	} else if kv.mAcks[op.ClientId] < op.OpSequence { //考虑clientAck未更新前相同消息进入raft层
		switch op.OpType {
		case OpPut:
			shard.Put(op.Key, op.Value)
		case OpAppend:
			shard.Append(op.Key, op.Value)
		case OpGet:
			reply.Value = shard.Get(op.Key)
		}
		kv.mAcks[op.ClientId] = max(kv.mAcks[op.ClientId], op.OpSequence)
		if _, isLeader := kv.rf.GetState(); isLeader {
			DPrintf("[Group %d][Server %d] shard %d info %+v, mAcks: %d, sequence: %d", kv.gid, kv.me, shardid, kv.shards[shardid], kv.mAcks[op.ClientId], op.OpSequence)
		}
	}

	_, isLeader := kv.rf.GetState()
	ackCh, ok := kv.mAckCh[kv.lastApplied]
	if isLeader && ok {
		select {
		case ackCh <- reply:
		//	DPrintf("KVServer-%d Process Notify CkId=%d msg=%v logidx=%d", kv.me, op.ClientId, op, kv.lastApplied)
		default: //do not wait in cass holding lock long time
			//	DPrintf("KVServer-%d Notify CkId=%d msg=%v logidx=%d timeout", kv.me, op.ClientId, op, kv.lastApplied)
		}
	}
}

func (kv *ShardKV) ProcessConfigUpdate(op *Op) {
	defer func() {
		DPrintf("[Group %d][Server %d] updating config: %+v from config: %+v", kv.gid, kv.me, kv.currentConf, kv.lastConf)
	}()
	if op.Config.Num != kv.currentConf.Num+1 {
		return
	}
	kv.lastConf, kv.currentConf = kv.currentConf, *op.Config
	adds, subs := kv.shardChanges()
	for _, id := range adds {
		if kv.lastConf.Num == 0 {
			kv.shards[id] = &Shard{State: Severing, KvStore: map[string]string{}}
		} else {
			kv.shards[id] = &Shard{State: Fetching}
		}
	}
	for _, id := range subs {
		kv.shards[id].State = Abandoning
	}
}

func (kv *ShardKV) shardChanges() ([]int, []int) {
	new := []int{}
	for shardid, gid := range kv.currentConf.Shards {
		if gid == kv.gid {
			new = append(new, shardid)
		}
	}
	new = append(new, len(kv.currentConf.Shards))

	last := []int{}
	for shardid, gid := range kv.lastConf.Shards {
		if gid == kv.gid {
			last = append(last, shardid)
		}
	}
	last = append(last, len(kv.currentConf.Shards))

	var adds, subs []int
	for i, j := 0, 0; i < len(new) && j < len(last); {
		if new[i] == last[j] {
			i, j = i+1, j+1
		}
		for ; i < len(new) && new[i] < last[j]; i++ {
			adds = append(adds, new[i])
		}
		for ; j < len(last) && last[j] < new[i]; j++ {
			subs = append(subs, last[j])
		}
	}
	return adds, subs
}

func (kv *ShardKV) ProcessKvStoreMerge(op *Op) {
	if op.ConfigNum != kv.currentConf.Num {
		return
	}
	defer func() {
		DPrintf("[Group %d][Server %d] Config.Num: %d merged op: %+v", kv.gid, kv.me, kv.currentConf.Num, op)
	}()
	for shardid, kvstore := range op.ShardsKvStore {
		if kv.shards[shardid].State == Fetching {
			kv.shards[shardid].KvStore = mapCopy(kvstore)
			kv.shards[shardid].State = Waiting
		}
	}
	for cid, ack := range op.ClientAck {
		kv.mAcks[cid] = max(kv.mAcks[cid], ack)
	}
}

func (kv *ShardKV) ProcessKvStoreErase(op *Op) {
	reply := OpResult{Error: OK}
	if op.ConfigNum != kv.currentConf.Num {
		reply.Error = ErrWorngConfigVer
		return
	}
	for _, shardid := range op.ShardIds {
		if kv.shards[shardid] == nil || kv.shards[shardid].State == Abandoning {
			delete(kv.shards, shardid)
		}
	}

	_, isLeader := kv.rf.GetState()
	ackCh, ok := kv.mAckCh[kv.lastApplied]
	if isLeader && ok {
		select {
		case ackCh <- reply:
		//	DPrintf("KVServer-%d Process Notify CkId=%d msg=%v logidx=%d", kv.me, op.ClientId, op, kv.lastApplied)
		default: //do not wait in cass holding lock long time
			//	DPrintf("KVServer-%d Notify CkId=%d msg=%v logidx=%d timeout", kv.me, op.ClientId, op, kv.lastApplied)
		}
	}
}

func (kv *ShardKV) ProcessShardServing(op *Op) {
	if op.ConfigNum != kv.currentConf.Num {
		return
	}
	for _, shardid := range op.ShardIds {
		if kv.shards[shardid].State == Waiting {
			kv.shards[shardid].State = Severing
		}
	}
}
