package raft

import "time"

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

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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

	mLastLogIndex := rf.LastLogIndex()
	mLastLogTerm := rf.LastLogTerm()
	if args.LastLogTerm > mLastLogTerm ||
		(args.LastLogTerm == mLastLogTerm && args.LastLogIndex >= mLastLogIndex) {
		reply.VoteGranted = true
		rf.switchState(Follower)
		rf.voteFor = args.CandidateId
	}
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
			args.LastLogIndex = rf.LastLogIndex()
			args.LastLogTerm = rf.LastLogTerm()
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
