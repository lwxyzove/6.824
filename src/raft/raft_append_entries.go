package raft

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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

	mLastLogIndex := rf.LastLogIndex()
	if mLastLogIndex < args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.term
		reply.ConflictTerm = -1
		reply.ConflictIndex = mLastLogIndex + 1
		return
	}

	if rf.LogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.term
		reply.ConflictTerm = rf.LogTerm(args.PrevLogIndex)
		for i := args.PrevLogIndex; i > rf.BaseLogIndex() && rf.LogTerm(i-1) == reply.ConflictTerm; i-- {
			reply.ConflictIndex = i
		}
		return
	}

	// 4. Append any new entries not already in the log compare from rf.log[args.PrevLogIndex + 1]
	rf.log = append(rf.log[:rf.LogIndex(args.PrevLogIndex)+1], args.Log...)

	//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.LastLogIndex())
		rf.applyCond.Signal()
	}

	reply.Success = true
	reply.Term = rf.term
	DPrintf("server: %d commit idx: %d, args.commit idx: %d, rf.logs: %v, len(rf.logs): %d, args.term: %d, args.logs: %v", rf.me, rf.commitIndex, args.LeaderCommit, entry(rf.log), len(rf.log), args.Term, entry(args.Log))
}

func (rf *Raft) AppendEntriesRequest(id int) {
	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}

	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	prevLogIndex := max(rf.nextIndex[id]-1, 0)
	entries := make([]LogEntry, len(rf.log[rf.LogIndex(prevLogIndex)+1:]))
	copy(entries, rf.log[rf.LogIndex(prevLogIndex)+1:])

	args.Term = rf.term
	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = rf.LogTerm(prevLogIndex)
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
		for n := rf.LastLogIndex(); n > rf.commitIndex && rf.LogTerm(n) == rf.term; n-- {
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
			for i := args.PrevLogIndex; i > rf.BaseLogIndex() && rf.LogTerm(i-1) != reply.ConflictTerm; i-- {
				rf.nextIndex[id] = i
			}
		}
	}
}

func (rf *Raft) ProcessAppendEntries() {
	for id := range rf.peers {
		if id == rf.me {
			continue
		}
		go func(id int) {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.nextIndex[id] <= rf.lastIncludeIndex {
				rf.InstallSnapshotRequest(id)
			} else {
				rf.AppendEntriesRequest(id)
			}
		}(id)
	}
}
