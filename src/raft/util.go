package raft

import (
	"fmt"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		fmt.Printf(format, a...)
	}
	return
}

func DPrintln(a ...interface{}) (n int, err error) {
	if Debug {
		fmt.Println(a...)
	}
	return
}

func printLog(entries []Entry) string {
	s := ""
	for _, e := range entries {
		s += fmt.Sprintf("%v ", e)
	}
	return s
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

func (rf *Raft) updateTerm(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.convertToFollwer()
		rf.persist()
	}
}

func getRandomElectionTimeout() time.Duration {
	t := time.Duration((rand.Int63n(10))*30+electionTimeout) * time.Millisecond
	return t
}

func copyLog(log []Entry) []Entry {
	newLog := make([]Entry, len(log))
	copy(newLog, log)
	return newLog
}

func (rf *Raft) getLastIdx() int {
	return len(rf.log) - 1
}
func (rf *Raft) getLastTerm() int {
	return rf.log[rf.getLastIdx()].Term
}
