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

func randElectTtl() time.Duration {
	ms := 250 + (rand.Int63() % 200)
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

	commitIndex      int
	lastApplied      int
	nextIndex        []int //next idxs send to peers, for leader
	matchIndex       []int //matched idxs for each peer, for leader
	log              []LogEntry
	lastIncludeIndex int
	lastIncludedTerm int

	applyCh chan ApplyMsg

	state   State
	term    int
	voteFor int

	applyCond *sync.Cond
	heartBeat *time.Ticker
	electTtl  *time.Ticker
}

func (rf *Raft) BaseLogIndex() int {
	return rf.lastIncludeIndex + 1
}

func (rf *Raft) LastLogIndex() int {
	return rf.lastIncludeIndex + len(rf.log)
}

func (rf *Raft) LastLogTerm() int {
	if len(rf.log) < 1 {
		return rf.lastIncludedTerm
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) LogIndex(idx int) int {
	return idx - rf.BaseLogIndex()
}

func (rf *Raft) Log(idx int) LogEntry {
	if idx < rf.BaseLogIndex() {
		return LogEntry{}
	}
	return rf.log[rf.LogIndex(idx)]
}

func (rf *Raft) LogTerm(idx int) int {
	if idx < rf.lastIncludeIndex {
		return -1
	} else if idx == rf.lastIncludeIndex {
		return rf.lastIncludedTerm
	}
	return rf.log[rf.LogIndex(idx)].Term
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
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	snapshot := rf.persister.ReadSnapshot()
	rf.persister.Save(raftstate, snapshot)
	DPrintf("server: %d saved rf.lastIncludeIndex: %d, rf.lastIncludedTerm: %d", rf.me, rf.lastIncludeIndex, rf.lastIncludedTerm)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	//	DPrintf("server: %d, decoding msg: %s", rf.me, data)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	d.Decode(&rf.term)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.lastIncludeIndex)
	d.Decode(&rf.lastIncludedTerm)
	d.Decode(&rf.log)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludeIndex || index > rf.commitIndex {
		return
	}

	defer rf.persist()

	nLogs := make([]LogEntry, len(rf.log[rf.LogIndex(index)+1:]))
	copy(nLogs, rf.log[rf.LogIndex(index)+1:])

	rf.lastIncludedTerm = rf.LogTerm(index)
	rf.lastIncludeIndex = index
	rf.log = nLogs
	rf.persister.SaveSnapshot(snapshot)

	rf.commitIndex = max(rf.commitIndex, index)
	rf.lastApplied = max(rf.lastApplied, index)

	DPrintf("server: %d reading snapshot, index: %d, rf.lastApplied: %d, rf.commitIndex: %d, rf.lastIncludeIndex: %d, rf.lastIncludedTerm: %d, len(logs): %d", rf.me, index, rf.lastApplied, rf.commitIndex, rf.lastIncludeIndex, rf.lastIncludedTerm, len(rf.log))
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
		index = rf.LastLogIndex()
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		DPrintf("id: %d, isLeader command: %v, term : %d, logs: %v, index: %d", rf.me, rf.state == Leader, rf.term, rf.log, rf.LastLogIndex())
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
			go rf.ProcessAppendEntries()
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
	rf.heartBeat = time.NewTicker(100 * time.Millisecond)
	rf.electTtl = time.NewTicker(randElectTtl())

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastIncludeIndex = -1
	rf.lastIncludedTerm = -1
	rf.log = []LogEntry{{}}
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.lastIncludeIndex > 0 {
		rf.lastApplied = rf.lastIncludeIndex
		rf.commitIndex = rf.lastApplied
	}
	DPrintf("server: %d started, term: %d, isLeader: %v, lastApplied: %d, commited: %d", rf.me, rf.term, rf.state == Leader, rf.lastApplied, rf.commitIndex)
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
	case Candidate:
		rf.voteFor = rf.me
		rf.term++
		rf.electTtl.Reset(randElectTtl())
	case Leader:
		//		rf.heartBeat.Reset(100 * time.Millisecond)
		for i := range rf.nextIndex {
			rf.nextIndex[i] = rf.LastLogIndex() + 1
		}
		for i := range rf.matchIndex {
			rf.matchIndex[i] = rf.lastIncludeIndex
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
			msg := ApplyMsg{}
			rf.lastApplied++
			msg.CommandValid = true
			msg.Command = rf.Log(rf.lastApplied).Command
			msg.CommandIndex = rf.lastApplied

			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		}
	}
}

func init() {
	rand.Seed(time.Now().Unix())
}
