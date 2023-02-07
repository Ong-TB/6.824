package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

func (rf *Raft) machineLoop() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		state := rf.state
		rf.applyCommand()
		rf.updateCommitIdx()
		rf.mu.Unlock()
		// log.Println("hahahe")
		switch state {
		case FOLLOWER:
			t := (time.Duration(rand.Int63n(6)*50 + electionTimeout))
			select {
			case <-time.After(t * time.Millisecond):
				DPrintf("*********%d time is %v\n", rf.me, t)
				rf.convertToCandidate()
			case <-rf.skipCh:
			}
		case CANDIDATE:
			select {
			case <-rf.winElectCh:
				rf.convertToLeader()
			case <-rf.stepDownCh:
				rf.mu.Lock()
				rf.convertToFollwer()
				rf.mu.Unlock()
			}
		case LEADER:
			select {
			case <-time.After(100 * time.Millisecond):

				go rf.heartbeat()
				rf.mu.Lock()
				rf.updateCommitIdx()
				rf.mu.Unlock()

			case <-rf.startCh:
				rf.replicate()
			case <-rf.stepDownCh:
				fmt.Println(rf.me, "converto follower")
				rf.mu.Lock()
				rf.convertToFollwer()
				rf.mu.Unlock()
			}
		}
	}
}
func (rf *Raft) resetChannels() {
	rf.winElectCh = make(chan bool)
	rf.stepDownCh = make(chan bool)
	rf.startCh = make(chan bool)
}
func (rf *Raft) convertToCandidate() {
	// DPrintf("%d c cand\n", rf.me)
	// time.Sleep(time.Duration(rand.Int63n(200)) * time.Millisecond)
	rf.mu.Lock()
	rf.resetChannels()
	DPrintf("%d convert to cand\n", rf.me)
	rf.currentTerm++
	rf.state = CANDIDATE
	rf.votedFor = rf.me
	sendToCh(rf.skipCh)

	rf.raiseElection()

	rf.mu.Unlock()
}

func (rf *Raft) raiseElection() {
	args := rf.newRequestVoteArgs()
	votes, denies := 1, 0
	cond := sync.NewCond(&rf.mu)

	for i := 0; i < rf.cnt; i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {

			reply := &RequestVoteReply{}

			// DPrintf("????send to %d\n", server)
			ok := rf.sendRequestVote(server, args, reply)
			// for !ok {
			// 	ok = rf.sendRequestVote(server, args, reply)
			// 	time.Sleep(time.Duration(rand.Int63n(100)) * time.Millisecond)

			// }
			fmt.Println(ok, rf.me, "send to ", server)
			// defer rf.mu.Unlock()
			// rf.mu.Lock()
			if ok && reply.VoteGranted {
				votes++
			} else {
				denies++
			}

			DPrintf("//[%d] voted %v for %d\n", server, reply, rf.me)

			cond.Broadcast()
		}(i)
	}
	// rf.mu.Lock()
	for votes <= rf.cnt/2 && denies <= rf.cnt/2 {
		cond.Wait()
	}
	log.Println("6666666666666666666666666666")
	if votes > rf.cnt/2 {
		sendToCh(rf.winElectCh)
		log.Println("Cand", rf.me, "becomes Leader|term", rf.currentTerm, len(rf.log))
	} else {
		sendToCh(rf.stepDownCh)
		log.Println("Cand", rf.me, "back to f00ollower", rf.currentTerm)
	}
}

func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetChannels()
	rf.state = LEADER
	rf.leaderReinit()
	go rf.heartbeat()
}

func (rf *Raft) convertToFollwer() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	rf.resetChannels()
	rf.votedFor = -1
	rf.state = FOLLOWER
	sendToCh(rf.skipCh)
}

func (rf *Raft) updateCommitIdx() {

	for i := rf.commitIdx + 1; i < len(rf.log); i++ {
		if rf.log[i].Term == rf.currentTerm {
			matchCnt := 0
			for j := 0; j < rf.cnt; j++ {
				if rf.matchIdx[j] >= i {
					matchCnt++
				}
			}
			//
			if matchCnt >= rf.cnt/2 {
				rf.commitIdx = i
				rf.applyCommand()
				DPrintf("[%d] commitidx = %d, lsappliedddddddddddddddddddd.d=%d\n", rf.me, rf.commitIdx, rf.lastApplied)
			}
			// break
		}
	}

}

func (rf *Raft) applyCommand() {

	for rf.commitIdx > rf.lastApplied {
		rf.lastApplied++
		appMsg := ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].Cmd, CommandIndex: rf.lastApplied}
		DPrintf("hahahahahaha[%d] applies with comidx = %v,\n", rf.me, appMsg)

		rf.applyCh <- appMsg
	}

}

func sendToCh(ch chan bool) {
	go func() {
		// select {
		// case ch <- true:
		// default:
		// }
		ch <- true
	}()

}

func (rf *Raft) replicate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// var wg sync.WaitGroup
	// wg.Add(rf.cnt / 2)
	lastLogIdx := len(rf.log) - 1
	for i := 0; i < rf.cnt; i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			if lastLogIdx >= rf.nextIdx[server] {
				reply := &AppendReply{}
				for !reply.Success {
					prevLogIdx := rf.nextIdx[server] - 1
					// DPrintf("send to [%d]..., prevLogIdx is %d\n", server, prevLogIdx)
					args := rf.newAppendArgs(rf.currentTerm, rf.log[prevLogIdx+1:], prevLogIdx)
					reply = &AppendReply{}

					ok := rf.AppendEntries(server, args, reply)

					// for !ok {
					// 	ok = rf.AppendEntries(server, args, reply)
					// }
					if reply.Success {
						// wg.Done()
						rf.matchIdx[server] = args.PrevLogIdx + len(args.Entries)
						break
					} else if args.Term < reply.Term || !ok {
						break
					}
					rf.nextIdx[server]--
					DPrintf("Send AE to [%d] : %v, ok=%v\n", server, *args, ok)
				}
				// update nextIdx
				rf.nextIdx[server] = lastLogIdx + 1
				// commit <- true
			}
			// rf.updateCommitIdx()
		}(i)
	}
	// wg.Wait()
	// rf.updateCommitIdx()
}
