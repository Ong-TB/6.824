package raft

import (
	// "fmt"
	"sync"
	"time"
)

func (rf *Raft) machineLoop() {
	for !rf.killed() {
		// time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		rf.persist()
		state := rf.state
		rf.applyCommand()
		// rf.updateCommitIdx()
		rf.mu.Unlock()
		// DPrintln("hahahe")
		switch state {
		case FOLLOWER:
			select {
			// case <-time.After(t * time.Millisecond):
			case <-rf.timer.C:
				// DPrintf("*********%d time is %v\n", rf.me, t)
				rf.convertToCandidate()
			default:
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
			case <-time.After(50 * time.Millisecond):
				rf.mu.Lock()

				rf.heartbeat()
				rf.updateCommitIdx()
				rf.mu.Unlock()

			case <-rf.startCh:
				rf.replicate()
			case <-rf.stepDownCh:
				DPrintln(rf.me, "converto follower")
				rf.mu.Lock()
				rf.convertToFollwer()
				rf.mu.Unlock()
			}
		}
	}
}
func (rf *Raft) resetChannels() {
	// rf.winElectCh = make(chan bool)
	// rf.stepDownCh = make(chan bool)
	// rf.startCh = make(chan bool)
}
func (rf *Raft) convertToCandidate() {
	// DPrintf("%d c cand\n", rf.me)
	// time.Sleep(time.Duration(rand.Int63n(200)) * time.Millisecond)
	rf.mu.Lock()

	DPrintf("%d convert to cand\n", rf.me)
	rf.resetChannels()

	rf.currentTerm++
	rf.state = CANDIDATE
	rf.votedFor = rf.me

	rf.timer.Stop()
	// sendToCh(rf.skipCh)

	rf.persist()

	rf.mu.Unlock()

	rf.raiseElection()

}

func (rf *Raft) raiseElection() {
	rf.mu.Lock()

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
			// }
			DPrintln(ok, rf.me, "send to ", server)
			// defer rf.mu.Unlock()
			rf.mu.Lock()
			if ok && reply.VoteGranted {
				votes++
			} else {
				denies++
			}
			rf.mu.Unlock()
			DPrintf("//[%d] voted %v for %d\n", server, reply, rf.me)

			cond.Broadcast()
		}(i)
	}
	for votes <= rf.cnt/2 && denies <= rf.cnt/2 {
		cond.Wait()
	}
	DPrintln("6666666666666666666666666666")
	if votes > rf.cnt/2 {
		sendToCh(rf.winElectCh)
		DPrintln("Cand", rf.me, "becomes Leader|term", rf.currentTerm, len(rf.log))
	} else {
		sendToCh(rf.stepDownCh)
		DPrintln("Cand", rf.me, "back to f00ollower", rf.currentTerm)
	}
	rf.mu.Unlock()
}

func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetChannels()
	rf.state = LEADER
	rf.leaderReinit()
	rf.timer.Stop()
	rf.heartbeat()
}

func (rf *Raft) convertToFollwer() {
	// rf.mu.Lock()
	// defer rf.persist()

	rf.resetChannels()
	rf.votedFor = -1
	rf.state = FOLLOWER
	rf.timer.Reset(getRandomElectionTimeout())
	sendToCh(rf.skipCh)
	rf.persist()
}

func (rf *Raft) updateCommitIdx() {

	// for i := rf.commitIdx + 1; i < len(rf.log); i++ {
	// 	if rf.log[i].Term == rf.currentTerm {
	// 		matchCnt := 0
	// 		for j := 0; j < rf.cnt; j++ {
	// 			if rf.matchIdx[j] >= i {
	// 				matchCnt++
	// 			}
	// 		}
	// 		//
	// 		if matchCnt >= rf.cnt/2 {
	// 			rf.commitIdx = i
	// 			rf.applyCommand()
	// 			DPrintf("[%d] commitidx = %d, lsappliedddddddddddddddddddd.d=%d\n", rf.me, rf.commitIdx, rf.lastApplied)
	// 		}
	// 		// break
	// 	}
	// }

	for n := rf.getLastIdx(); n >= rf.commitIdx; n-- {
		count := 1
		if rf.log[n].Term == rf.currentTerm {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIdx[i] >= n {
					count++
				}
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIdx = n
			rf.applyCommand()
			break
		}
	}
}

func (rf *Raft) applyCommand() {

	// for rf.commitIdx > rf.lastApplied {
	// 	rf.lastApplied++
	// 	appMsg := ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].Cmd, CommandIdx: rf.lastApplied}
	// 	rf.applyCh <- appMsg
	// 	DPrintf("hahahahahaha[%d] applies with comidx = %v,\n", rf.me, appMsg)
	// }

	//

	for i := rf.lastApplied + 1; i <= rf.commitIdx; i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Cmd,
			CommandIndex: i,
		}
		rf.lastApplied = i
	}

}

func sendToCh(ch chan bool) {
	go func() {
		ch <- true
	}()

}

func (rf *Raft) replicate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// var wg sync.WaitGroup
	// wg.Add(rf.cnt / 2)
	// cond := sync.NewCond(&rf.mu)
	lastLogIdx := len(rf.log) - 1
	cidx := rf.commitIdx
	for i := 0; i < rf.cnt; i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			nextIdx := rf.nextIdx[server]
			rf.mu.Unlock()
			if lastLogIdx >= nextIdx {
				reply := &AppendReply{}
				for !reply.Success {
					prevLogIdx := max(nextIdx-1, 0)
					rf.mu.Lock()
					log := copyLog(rf.log[prevLogIdx+1:])
					// DPrintf("send to [%d]..., prevLogIdx is %d\n", server, prevLogIdx)
					args := rf.newAppendArgs(rf.currentTerm, log, prevLogIdx, cidx)
					rf.mu.Unlock()
					reply = &AppendReply{}

					ok := rf.AppendEntries(server, args, reply)

					// for !ok {
					// 	ok = rf.AppendEntries(server, args, reply)
					// }
					if reply.Success {
						// wg.Done()
						rf.mu.Lock()
						rf.matchIdx[server] = args.PrevLogIdx + len(args.Entries)
						rf.mu.Unlock()
						break
					} else if args.Term < reply.Term || !ok {
						break
					}
					// } else if reply.ConflictTerm < 0 {
					// 	rf.nextIdx[server] = reply.ConflictIdx
					// 	rf.matchIdx[server] = rf.nextIdx[server] - 1
					// } else {
					// 	newNextIdx := rf.getLastIdx()
					// 	for ; newNextIdx >= 0; newNextIdx-- {
					// 		if rf.log[newNextIdx].Term == reply.ConflictTerm {
					// 			break
					// 		}
					// 	}
					// 	// if not found, set nextIdx to conflictIdx
					// 	if newNextIdx < 0 {
					// 		rf.nextIdx[server] = reply.ConflictIdx
					// 	} else {
					// 		rf.nextIdx[server] = newNextIdx
					// 	}
					// 	rf.matchIdx[server] = rf.nextIdx[server] - 1
					// }
					rf.mu.Lock()
					rf.nextIdx[server]--
					nextIdx = rf.nextIdx[server]
					rf.mu.Unlock()
					DPrintf("Send AE to [%d] : %v, ok=%v\n", server, *args, ok)
				}
				rf.mu.Lock()
				rf.nextIdx[server] = lastLogIdx + 1
				rf.mu.Unlock()
			}
			// cond.Broadcast()
		}(i)
	}

	// for total != rf.cnt-1 {
	// 	cond.Wait()
	// }
	// rf.updateCommitIdx()
}
