package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType     string
	ClientId   int64
	OpSequence int64
	Key        string
	Value      string
}

type result struct {
	err Err
	val string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mKvs   map[string]string
	mAcks  map[int64]int64
	mAckCh map[int64]chan result
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	logIdx, _, isLeader := kv.rf.Start(Op{
		ClientId:   args.ClientId,
		Key:        args.Key,
		OpType:     "Get",
		OpSequence: args.OpSequence,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	var rc chan result
	WithLock(&kv.mu, func() {
		if _, ok := kv.mAckCh[int64(logIdx)]; !ok {
			kv.mAckCh[int64(logIdx)] = make(chan result)
		}
		rc = kv.mAckCh[int64(logIdx)]
	})

	DPrintf("KVServer-%d Received Req Get %v, logIndex=%d", kv.me, args, logIdx)
	select {
	case res := <-rc:
		reply.Err, reply.Value = res.err, res.val
		WithLock(&kv.mu, func() {
			kv.mAcks[args.ClientId] = maxI64(kv.mAcks[args.ClientId], args.OpSequence)
			delete(kv.mAckCh, int64(logIdx))
		})
	case <-time.After(300 * time.Millisecond):
		reply.Err = ErrTimeOut
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if args.OpSequence <= kv.mAcks[args.ClientId] {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	logIdx, _, isLeader := kv.rf.Start(Op{
		ClientId:   args.ClientId,
		Key:        args.Key,
		Value:      args.Value,
		OpType:     "Put",
		OpSequence: args.OpSequence,
	})
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	if _, ok := kv.mAckCh[int64(logIdx)]; !ok {
		kv.mAckCh[int64(logIdx)] = make(chan result)
	}
	rc := kv.mAckCh[int64(logIdx)]
	kv.mu.Unlock()

	DPrintf("KVServer-%d Received Req Put %v, logIndex=%d", kv.me, args, logIdx)

	select {
	case res := <-rc:
		reply.Err = res.err
		WithLock(&kv.mu, func() {
			delete(kv.mAckCh, int64(logIdx))
			kv.mAcks[args.ClientId] = maxI64(kv.mAcks[args.ClientId], args.OpSequence)
		})
	case <-time.After(200 * time.Millisecond):
		reply.Err = ErrTimeOut
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if args.OpSequence <= kv.mAcks[args.ClientId] {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	logIdx, _, isLeader := kv.rf.Start(Op{
		ClientId:   args.ClientId,
		Key:        args.Key,
		Value:      args.Value,
		OpType:     "Append",
		OpSequence: args.OpSequence,
	})
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	if _, ok := kv.mAckCh[int64(logIdx)]; !ok {
		kv.mAckCh[int64(logIdx)] = make(chan result)
	}
	rc := kv.mAckCh[int64(logIdx)]
	kv.mu.Unlock()

	DPrintf("KVServer-%d Received Req Append %v, logIndex=%d", kv.me, args, logIdx)

	select {
	case res := <-rc:
		reply.Err = res.err
		WithLock(&kv.mu, func() {
			delete(kv.mAckCh, int64(logIdx))
			kv.mAcks[args.ClientId] = maxI64(kv.mAcks[args.ClientId], args.OpSequence)
		})
	case <-time.After(200 * time.Millisecond):
		reply.Err = ErrTimeOut
	}
}

func (kv *KVServer) Process() {
	for applyMsg := range kv.applyCh {
		msg := applyMsg.Command.(Op)
		logIdx := int64(applyMsg.CommandIndex)

		kv.mu.Lock()

		//		DPrintf("applying msg: %v, kv.msessions: %d == nil: %v", msg, msg.ClientId, kv.msessions[msg.ClientId] == nil)
		if msg.OpType != "Get" && msg.OpSequence <= kv.mAcks[msg.ClientId] {
			kv.mu.Unlock()
			continue
		}
		res := result{err: OK}
		if _, ok := kv.mKvs[msg.Key]; !ok {
			res.err = ErrNoKey
		}
		switch msg.OpType {
		case "Get":
			res.val = kv.mKvs[msg.Key]
		case "Put":
			kv.mKvs[msg.Key] = msg.Value
		case "Append":
			kv.mKvs[msg.Key] += msg.Value
		}
		kv.mAcks[msg.ClientId] = msg.OpSequence

		_, isLeader := kv.rf.GetState()
		//		DPrintf("KVServer-%d Excute CkId=%d Msg=%v isLeader=%v", kv.me, msg.ClientId, msg, isLeader)
		ackCh, ok := kv.mAckCh[logIdx]
		kv.mu.Unlock()

		if isLeader && ok {
			DPrintf("KVServer-%d Process Excute CkId=%d Msg=%v, kvdata=%v", kv.me, msg.ClientId, msg, kv.mKvs[msg.Key])
			select {
			case ackCh <- res:
				DPrintf("KVServer-%d Process Notify CkId=%d msg=%v logidx=%d", kv.me, msg.ClientId, msg, logIdx)
			case <-time.After(50 * time.Millisecond):
				DPrintf("KVServer-%d Notify CkId=%d msg=%v logidx=%d timeout", kv.me, msg.ClientId, msg, logIdx)
			}
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.mKvs = map[string]string{}
	kv.mAcks = map[int64]int64{}
	kv.mAckCh = make(map[int64]chan result)

	go kv.Process()

	return kv
}
