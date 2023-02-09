package raft

import (
	"time"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateId int
	Term        int
	LastLogIdx  int
	LastLogTerm int
}

func (rf *Raft) newRequestVoteArgs() *RequestVoteArgs {
	return &RequestVoteArgs{
		CandidateId: rf.me,
		Term:        rf.currentTerm,
		LastLogIdx:  len(rf.log) - 1,
		LastLogTerm: rf.log[len(rf.log)-1].Term,
	}
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool // true if

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	for !rf.hasRecovered {
		time.Sleep(10 * time.Millisecond)
	}
	rf.mu.Lock()

	defer rf.mu.Unlock()
	defer rf.persist()
	//kkk
	defer DPrintf("[%v]|%d received vote req from %v", rf.me, rf.currentTerm, *args)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < reply.Term {
		return
	}

	if args.Term > reply.Term {
		// state := rf.state
		// rf.state = FOLLOWER
		rf.currentTerm = args.Term
		reply.Term = args.Term
		rf.convertToFollwer()
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.alMoreUpToDate(args) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
	// rf.state = FOLLOWER
	// sendToCh(rf.skipCh)

	rf.lastFollow = time.Now()

	DPrintf("!!!!!!!!!!![%d] votes %v for cand [%d]|%d, %s\n", rf.me, reply.VoteGranted, args.CandidateId, args.Term, printLog(rf.log))
	/////
}

// args is at least more up-to-date than rf
func (rf *Raft) alMoreUpToDate(args *RequestVoteArgs) bool {
	idx := len(rf.log) - 1
	if args.LastLogTerm > rf.log[idx].Term || args.LastLogTerm == rf.log[idx].Term && args.LastLogIdx >= idx {
		return true
	}
	return false
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("Cand [%d]|%d send request vote to [%d]", args.CandidateId, args.Term, server)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.updateTerm(reply.Term)
	return ok
	// haha
}
