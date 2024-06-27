package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.term > args.Term {
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

	reply.Term = rf.term
	if rf.lastIncludeIndex > args.LastIncludedIndex {
		return
	}

	DPrintf("server: %d installed snapshot, index: %d, term: %d", rf.me, args.LastIncludedIndex, args.LastIncludedTerm)

	offset := min(rf.LastLogIndex(), args.LastIncludedIndex)
	nLogs := make([]LogEntry, len(rf.log[rf.LogIndex(offset)+1:]))
	copy(nLogs, rf.log[rf.LogIndex(offset)+1:])
	rf.log = nLogs
	rf.persister.SaveSnapshot(args.Data)

	rf.lastIncludeIndex = max(rf.lastIncludeIndex, args.LastIncludedIndex)
	rf.lastIncludedTerm = max(rf.lastIncludedTerm, args.LastIncludedTerm)

	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)

	applySnapShot := ApplyMsg{}
	applySnapShot.SnapshotValid = true
	applySnapShot.Snapshot = args.Data
	applySnapShot.SnapshotIndex = rf.lastIncludeIndex
	applySnapShot.SnapshotTerm = rf.lastIncludedTerm

	rf.mu.Unlock()
	rf.applyCh <- applySnapShot
	rf.mu.Lock()
}

func (rf *Raft) InstallSnapshotRequest(id int) {
	args := InstallSnapshotArgs{}
	reply := InstallSnapshotReply{}

	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	args.LeaderId = rf.me
	args.Term = rf.term
	args.LastIncludedIndex = rf.lastIncludeIndex
	args.LastIncludedTerm = rf.lastIncludedTerm
	args.Data = rf.persister.ReadSnapshot()
	rf.mu.Unlock()

	if !rf.sendInstallSnapshot(id, &args, &reply) {
		DPrintf("server: %d, term: %d call server: %d, failed", args.LeaderId, args.Term, id)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader || rf.term != args.Term {
		return
	}

	if rf.term < reply.Term {
		rf.term = reply.Term
		rf.switchState(Follower)
		rf.persist()
	} else {
		rf.matchIndex[id] = max(rf.matchIndex[id], args.LastIncludedIndex)
		rf.nextIndex[id] = rf.matchIndex[id] + 1
		//rf.matchIndex[id] = args.LastIncludedIndex
	}
}
