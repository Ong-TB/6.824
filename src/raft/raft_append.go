package raft

import (
	"time"
)

type AppendArgs struct {
	Term         int
	LeaderId     int
	Entries      []Entry
	PrevLogIdx   int
	PrevLogTerm  int
	LeaderCommit int
}

type AppendReply struct {
	Term    int
	Success bool
}

func (rf *Raft) newAppendArgs(term int, entries []Entry, prevLogIdx, cidx int) *AppendArgs {
	return &AppendArgs{
		Term:         term,
		LeaderId:     rf.me,
		Entries:      entries,
		PrevLogIdx:   prevLogIdx,
		PrevLogTerm:  rf.log[prevLogIdx].Term,
		LeaderCommit: cidx,
	}
}

func (rf *Raft) AppendEntries(server int, args *AppendArgs, reply *AppendReply) bool {

	ok := rf.peers[server].Call("Raft.ReceiveEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.updateTerm(reply.Term)
	return ok
}

func (rf *Raft) ReceiveEntries(args *AppendArgs, reply *AppendReply) {
	// rf.electionLock.Lock()
	// defer rf.electionLock.Unlock()
	rf.mu.Lock()

	for !rf.hasRecovered {
		time.Sleep(10 * time.Millisecond)
	}

	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm
	DPrintf("%d Receive entries: t=%d, lid=%d, %s, pli=%d,plt=%d,lcmi=%d\n", rf.me, args.Term, args.LeaderId, printLog(args.Entries), args.PrevLogIdx, args.PrevLogTerm, args.LeaderCommit)

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if args.PrevLogIdx >= len(rf.log) || args.PrevLogTerm != rf.log[args.PrevLogIdx].Term {
		reply.Success = false
		return
	}

	rf.leaderId = args.LeaderId
	// DPrintf("ct=%d, len(rf.log)=%d, lpt = %d\n", rf.currentTerm, len(rf.log), rf.log[args.PrevLogIdx].Term)
	// invalid append
	if args.Term == rf.currentTerm {
		rf.convertToFollwer()

	} else {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		// rf.state = FOLLOWER
		// sendToCh(rf.skipCh)
		rf.convertToFollwer()
	}
	reply.Success = true

	// sendToCh(rf.stepDownCh)
	// rf.convertToFollwer()
	if args.Entries != nil {
		rf.log = copyLog(rf.log[:args.PrevLogIdx+1])
		rf.log = append(rf.log, args.Entries...)
		DPrintf("[%d] reply true, len log is %v\n", rf.me, printLog(rf.log))

	} else {
		// DPrintf("[%d]...received heartbeat from %d\n", rf.me, args.LeaderId)
	}

	if args.LeaderCommit == rf.commitIdx {
		if rf.log[rf.commitIdx].Term != rf.currentTerm {
			return
		}
	}
	if args.LeaderCommit > rf.commitIdx {
		rf.commitIdx = min(args.LeaderCommit, len(rf.log)-1)
		rf.applyCommand()
	}

	// // invalid append
	// DPrintf("[%d] reply FALSe, len log is %v\n", rf.me, rf.log)
	// DPrintf("ct=%d, len(rf.log)=%d, lpt = , rfcmtidx = %d\n", rf.currentTerm, len(rf.log), rf.commitIdx)
	// //

	// reply.Success = false

}
