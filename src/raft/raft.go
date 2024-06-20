package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func randElectTtl() time.Duration {
	ms := 150 + (rand.Int63() % 200)
	return time.Duration(ms) * time.Millisecond
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int

const (
	Leader State = iota + 1
	Follower
	Candidate
)

type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// state a Raft server must maintain.

	commitIndex int
	lastApplied int
	nextIndex   []int //next idxs send to peers, for leader
	matchIndex  []int //matched idxs for each peer, for leader
	log         []LogEntry
	applyCh     chan ApplyMsg

	state   State
	term    int
	voteFor int

	applyCond *sync.Cond
	heartBeat *time.Ticker
	electTtl  *time.Ticker
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	d.Decode(&rf.term)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.log)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int //最后一条日志条目index
	LastLogTerm  int //最后一条日志条目term
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.term ||
		(args.Term == rf.term && rf.voteFor != -1 && rf.voteFor != args.CandidateId) {
		reply.Term = rf.term
		reply.VoteGranted = false
		return
	}

	defer rf.persist()
	if args.Term > rf.term {
		rf.term = args.Term
		rf.switchState(Follower)
	}

	reply.Term = rf.term
	reply.VoteGranted = false

	mLastLogIndex := len(rf.log) - 1
	if args.LastLogTerm > rf.log[mLastLogIndex].Term ||
		(args.LastLogTerm == rf.log[mLastLogIndex].Term && args.LastLogIndex >= mLastLogIndex) {
		reply.VoteGranted = true
		rf.switchState(Follower)
		rf.voteFor = args.CandidateId
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) ProcessElection() {
	DPrintf("server: %d start election, cur term: %d, time: %d", rf.me, rf.term, time.Now().UnixMilli())
	voteCnt := 1
	for id := range rf.peers {
		if rf.me == id {
			continue
		}
		go func(id int) {
			args := RequestVoteArgs{}
			reply := RequestVoteReply{}

			rf.mu.Lock()
			args.Term = rf.term
			args.CandidateId = rf.me
			args.LastLogIndex = len(rf.log) - 1
			args.LastLogTerm = rf.log[args.LastLogIndex].Term
			rf.mu.Unlock()

			if !rf.sendRequestVote(id, &args, &reply) {
				//	DPrintf("server %d request vote failed !", id)
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.term != args.Term {
				return
			}
			DPrintf("server: %d, term %d got vote reply from server: %d, term: %d, votegranted: %v", rf.me, rf.term, id, reply.Term, reply.VoteGranted)
			// 如果收到的回复，对方的 term 比你大，那已经不用参选了
			if reply.Term > rf.term {
				rf.term = reply.Term
				rf.switchState(Follower)
				rf.persist()
			} else if reply.VoteGranted {
				if voteCnt++; voteCnt*2 > len(rf.peers) && rf.state == Candidate {
					// 获得的选票已经超过一半了
					// 可能在处理 rpc 请求时，变成了 follower
					rf.switchState(Leader)
					DPrintf("server: %d convert to Leader: %v", rf.me, rf.state == Leader)
				}
			}
		}(id)
	}
}

type AppendEntriesArgs struct {
	Term         int //leader 的 term
	LeaderId     int
	PrevLogIndex int //前次日志条目的index
	PrevLogTerm  int //前次日志条目的term
	Log          []LogEntry
	LeaderCommit int //已知的leader的最新commit
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.term {
		reply.Success = false
		reply.Term = rf.term
		return
	}

	defer rf.persist()
	if args.Term > rf.term {
		rf.term = args.Term
		rf.voteFor = -1
	}
	rf.state = Follower
	rf.electTtl.Reset(randElectTtl())

	if len(rf.log)-1 < args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.term
		reply.ConflictTerm = -1
		reply.ConflictIndex = len(rf.log)
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.term
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		for i := args.PrevLogIndex; i > 0 && rf.log[i-1].Term == reply.ConflictTerm; i-- {
			reply.ConflictIndex = i
		}
		return
	}

	// 4. Append any new entries not already in the log compare from rf.log[args.PrevLogIndex + 1]
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Log...)
	//  for i := range args.Log {
	// 	idx := args.PrevLogIndex + i + 1
	// 	if idx >= len(rf.log) {
	// 		rf.log = append(rf.log, args.Log[i])
	// 	} else {
	// 		if rf.log[idx].Term != args.Log[i].Term {
	// 			rf.log = rf.log[:idx]
	// 			rf.log = append(rf.log, args.Log[i])
	// 		}
	// 	}
	// }

	//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		rf.applyCond.Signal()
	}

	reply.Success = true
	reply.Term = rf.term
	DPrintf("server: %d commit idx: %d, args.commit idx: %d, rf.logs: %v, len(rf.logs): %d, args.term: %d, args.logs: %v", rf.me, rf.commitIndex, args.LeaderCommit, entry(rf.log), len(rf.log), args.Term, entry(args.Log))
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) ProcessAppendEntries() {
	for id := range rf.peers {
		if id == rf.me {
			continue
		}
		go func(id int) {
			args := AppendEntriesArgs{}
			reply := AppendEntriesReply{}

			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			prevLogIndex := max(rf.nextIndex[id]-1, 0)
			entries := make([]LogEntry, len(rf.log[(prevLogIndex+1):]))
			copy(entries, rf.log[(prevLogIndex+1):])

			args.Term = rf.term
			args.PrevLogIndex = prevLogIndex
			args.PrevLogTerm = rf.log[prevLogIndex].Term
			args.Log = entries
			args.LeaderCommit = rf.commitIndex
			args.LeaderId = rf.me
			rf.mu.Unlock()

			if !rf.sendAppendEntries(id, &args, &reply) {
				DPrintf("server: %d, term: %d call server: %d, failed", args.LeaderId, args.Term, id)
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.state != Leader || rf.term != args.Term {
				return
			}

			if reply.Success {
				rf.matchIndex[id] = args.PrevLogIndex + len(entries)
				rf.nextIndex[id] = rf.matchIndex[id] + 1

				// If there exists an N such that N > commitIndex, a majority
				// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
				// set commitIndex = N (§5.3, §5.4).
				for n := len(rf.log) - 1; n > rf.commitIndex && rf.log[n].Term == rf.term; n-- {
					var cnt int
					for _, matched := range rf.matchIndex {
						if matched >= n {
							cnt++
						}
					}
					if 2*cnt > len(rf.peers) {
						rf.commitIndex = n
						rf.applyCond.Signal()
						break
					}
				}
				/* Q: or binary search
				offset := sort.Search(len(rf.log[rf.commitIndex:]), func(i int) bool {
					var cnt int
					for _, matched := range rf.matchIndex {
						if matched >= rf.commitIndex+i {
							cnt++
						}
						if 2*cnt > len(rf.peers) && rf.log[rf.commitIndex+i].Term == rf.term {
							return false
						}
					}
					return true
				})
				if offset > 1 {
					rf.commitIndex += offset - 1
					rf.applyCond.Signal()
				}
				*/
			} else if reply.Term > rf.term { // 如果收到的回复，对方的 term 比你大
				rf.term = reply.Term
				rf.switchState(Follower)
				rf.persist()
				DPrintf("server: %d, term: %d turn to follower, id: %d, term:%d", rf.me, rf.term, id, reply.Term)
			} else {
				rf.nextIndex[id] = reply.ConflictIndex
				if reply.ConflictTerm != -1 {
					for i := args.PrevLogIndex; i > 0 && rf.log[i-1].Term != reply.ConflictTerm; i-- {
						rf.nextIndex[id] = i
					}
				}
			}
		}(id)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.commitIndex
	term := rf.term
	isLeader := rf.state == Leader
	if isLeader {
		rf.log = append(rf.log, LogEntry{Command: command, Term: rf.term})
		rf.persist()
		index = len(rf.log) - 1
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		DPrintf("id: %d, isLeader command: %v, term : %d, logs: %v, len(logs): %d", rf.me, rf.state == Leader, rf.term, entry(rf.log), len(rf.log))
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		select {
		case <-rf.heartBeat.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.ProcessAppendEntries()
			}
			rf.mu.Unlock()
		case <-rf.electTtl.C:
			rf.mu.Lock()
			switch rf.state {
			case Follower, Candidate:
				rf.switchState(Candidate)
				rf.persist()
				rf.ProcessElection()
			}
			rf.mu.Unlock()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = Follower
	rf.term = 0
	rf.voteFor = -1
	rf.heartBeat = time.NewTicker(50 * time.Millisecond)
	rf.electTtl = time.NewTicker(randElectTtl())

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = []LogEntry{{}}
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("server: %d started, term: %d, isLeader: %v", rf.me, rf.term, rf.state == Leader)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyCommit()

	return rf
}

func (rf *Raft) switchState(st State) {
	rf.state = st
	switch st {
	case Follower:
		rf.voteFor = -1
		rf.electTtl.Reset(randElectTtl())
	case Candidate:
		rf.voteFor = rf.me
		rf.term++
		rf.electTtl.Reset(randElectTtl())
	case Leader:
		//		rf.heartBeat.Reset(100 * time.Millisecond)
		rf.electTtl.Stop()
		for i := range rf.nextIndex {
			rf.nextIndex[i] = len(rf.log)
		}
		for i := range rf.matchIndex {
			rf.matchIndex[i] = 0
		}
		//		go rf.ProcessAppendEntries()
	}
}

func (rf *Raft) applyCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.killed() == false {
		if rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}
		if rf.commitIndex > rf.lastApplied {
			entries := rf.log[rf.lastApplied+1 : rf.commitIndex+1]
			DPrintf("server: %d, isLeader: %v, applys: %v", rf.me, rf.state == Leader, entries)

			for _, entry := range entries {
				msg := ApplyMsg{}
				msg.CommandValid = true
				msg.Command = entry.Command
				msg.CommandIndex = rf.lastApplied + 1

				rf.mu.Unlock()
				rf.applyCh <- msg
				rf.mu.Lock()
				rf.lastApplied++
			}
		}
	}
}

func init() {
	rand.Seed(time.Now().Unix())
}
