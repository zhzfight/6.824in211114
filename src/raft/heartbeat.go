package raft

// surrounded by lock
func (rf *Raft) broadcast() {
	//log.Printf("rf.log:%v",rf.log)

	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}
			preLogIndex := rf.nextIndex[server] - 1
			if rf.getRelativeLogIndex(preLogIndex) < 0 {
				go rf.syncSnapshotWith(server)
				rf.mu.Unlock()
				return
			}
			var entries []entry
			if rf.nextIndex[server] > rf.lastLogIndex {
				entries = nil
			} else {
				entries = make([]entry, len(rf.log[rf.getRelativeLogIndex(preLogIndex+1):]))
				copy(entries, rf.log[rf.getRelativeLogIndex(preLogIndex+1):])
			}
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: preLogIndex,
				PrevLogTerm:  rf.log[rf.getRelativeLogIndex(preLogIndex)].Term,
				LeaderCommit: rf.commitIndex,
				Entries:      entries}
			rf.mu.Unlock()
			//log.Printf("leader%d commitIndex%d send appendRPC to server%d,entries:%v", rf.me, rf.commitIndex, server, args.Entries)
			var reply AppendEntriesReply
			if rf.sendAppendEntries(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.state != LEADER {
					return
				}
				if reply.Success {
					if preLogIndex+len(entries) >= rf.nextIndex[server] {
						rf.nextIndex[server] = preLogIndex + len(entries) + 1
					}
					if preLogIndex+len(entries) > rf.matchIndex[server] {
						rf.matchIndex[server] = preLogIndex + len(entries)
					}
					for N := rf.getAbsoluteLogIndex(len(rf.log) - 1); N > rf.commitIndex; N-- {
						count := 0
						for _, matchIndex := range rf.matchIndex {
							if matchIndex >= N {
								count += 1
							}
						}
						if count > len(rf.peers)/2 && rf.log[rf.getRelativeLogIndex(N)].Term == rf.currentTerm {
							rf.commitIndex = N
							break
						}
					}

				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.convertTo(FOLLOWER)
						rf.persist()
						//log.Printf("leader%d discover higher term from server%d and convertTo follower", rf.me, server)
					} else {
						// snapshot version
						if rf.getRelativeLogIndex(reply.ConflictedIndex) <= 0 {
							go rf.syncSnapshotWith(server)
						} else {
							if reply.ConflictedTerm == -1 {
								rf.nextIndex[server] = reply.ConflictedIndex
							} else {
								if rf.log[rf.getRelativeLogIndex(reply.ConflictedIndex)].Term == reply.ConflictedTerm {
									rf.nextIndex[server] = reply.ConflictedIndex + 1
								} else {
									rf.nextIndex[server] = reply.ConflictedIndex
								}
							}
						}

					}
				}

			}
		}(server)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//log.Printf("server%d state%d term%d commitIndex%d log%v get appendRPC from %d,entries:%v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.log, args.LeaderId, args.Entries)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}
	if rf.state == LEADER {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.convertTo(FOLLOWER)
			rf.votedFor = args.LeaderId
			rf.persist()
		}
	} else if rf.state == CANDIDATE {
		if args.Term >= rf.currentTerm {
			rf.currentTerm = args.Term
			rf.convertTo(FOLLOWER)

			rf.votedFor = args.LeaderId
			rf.persist()
		}
	} else {
		if args.Term >= rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = args.LeaderId
			rf.persist()
			rf.electionTimer.Reset(randTimeDuration(ElectionTime))
		}
	}

	if rf.getRelativeLogIndex(args.PrevLogIndex) >= len(rf.log) {
		// false
		reply.ConflictedIndex = rf.lastLogIndex + 1
		reply.ConflictedTerm = -1
	} else if rf.getRelativeLogIndex(args.PrevLogIndex) >= 0 && rf.log[rf.getRelativeLogIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		// false
		conflictedTerm := rf.log[rf.getRelativeLogIndex(args.PrevLogIndex)].Term
		rf.log = append(rf.log[:rf.getRelativeLogIndex(args.PrevLogIndex)])
		rf.persist()
		rf.lastLogIndex = args.PrevLogIndex - 1

		// the conflictedIndex here is relative
		var conflictedIndex int
		for conflictedIndex = rf.getRelativeLogIndex(args.PrevLogIndex) - 1; conflictedIndex > 0; conflictedIndex-- {
			if rf.log[conflictedIndex].Term != conflictedTerm {
				break
			}
		}
		reply.ConflictedTerm = conflictedTerm
		reply.ConflictedIndex = rf.getAbsoluteLogIndex(conflictedIndex) + 1

	} else {
		var i int
		if rf.getRelativeLogIndex(args.PrevLogIndex) < 0 {
			i = -rf.getRelativeLogIndex(args.PrevLogIndex)
		}
		for ; i < len(args.Entries); i++ {
			if rf.getRelativeLogIndex(args.PrevLogIndex)+i+1 > len(rf.log)-1 || rf.log[rf.getRelativeLogIndex(args.PrevLogIndex)+i+1].Term != args.Entries[i].Term {
				rf.log = append(rf.log[:rf.getRelativeLogIndex(args.PrevLogIndex)+i+1], args.Entries[i:]...)
				rf.persist()
				rf.lastLogIndex = args.PrevLogIndex + len(args.Entries)
				break
			}
		}
		reply.Success = true
	}

	if args.LeaderCommit > rf.commitIndex && reply.Success {
		if rf.lastLogIndex < args.LeaderCommit {
			rf.commitIndex = rf.lastLogIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}
	//log.Printf("server%d's append reply:%v", rf.me, reply)
}
