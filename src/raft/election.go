package raft

import "sync/atomic"

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//
	//log.Printf("server%d term%d log%v get vote request from candidate%d term%d lastLogIndex%d lastLogTerm%d", rf.me, rf.currentTerm, rf.log, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}

	/*
		if rf.state == LEADER {
			if args.Term == rf.currentTerm {
				return
			}
			rf.currentTerm = args.Term
			rf.convertTo(FOLLOWER)
			if args.LastLogTerm < rf.log[rf.lastLogIndex].Term {
				return
			}
			if args.LastLogTerm == rf.log[rf.lastLogIndex].Term && args.LastLogIndex < rf.lastLogIndex {
				return
			}
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else if rf.state == CANDIDATE {
			if args.Term == rf.currentTerm {
				return
			}
			rf.currentTerm = args.Term
			rf.convertTo(FOLLOWER)
			if args.LastLogTerm < rf.log[rf.lastLogIndex].Term {
				return
			}
			if args.LastLogTerm == rf.log[rf.lastLogIndex].Term && args.LastLogIndex < rf.lastLogIndex {
				return
			}
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			if args.Term > rf.currentTerm {
				rf.currentTerm = args.Term
				rf.votedFor = -1
			}
			if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
				if args.LastLogTerm < rf.log[rf.lastLogIndex].Term {
					return
				}
				if args.LastLogTerm == rf.log[rf.lastLogIndex].Term && args.LastLogIndex < rf.lastLogIndex {
					return
				}
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
				rf.electionTimer.Reset(randTimeDuration(ElectionTime))
			}

		}

	*/

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm ||
		(args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertTo(FOLLOWER)
		rf.persist()
	}
	if args.LastLogTerm < rf.log[rf.getRelativeLogIndex(rf.lastLogIndex)].Term {
		return
	}
	if args.LastLogTerm == rf.log[rf.getRelativeLogIndex(rf.lastLogIndex)].Term && args.LastLogIndex < rf.lastLogIndex {
		return
	}
	rf.votedFor = args.CandidateId
	rf.persist()
	reply.VoteGranted = true
	rf.electionTimer.Reset(randTimeDuration(ElectionTime))

}

// surrounded by lock
func (rf *Raft) startElection() {
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.electionTimer.Reset(randTimeDuration(ElectionTime))
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex,
		LastLogTerm:  rf.log[rf.getRelativeLogIndex(rf.lastLogIndex)].Term}
	var voteCount int32
	voteCount += 1
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			var reply RequestVoteReply
			if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				defer rf.persist()
				if reply.VoteGranted && rf.state == CANDIDATE {
					atomic.AddInt32(&voteCount, 1)
					if voteCount > int32(len(rf.peers)/2) {
						rf.convertTo(LEADER)
					}
				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.convertTo(FOLLOWER)
					}
				}

			}
		}(server)
	}
}
