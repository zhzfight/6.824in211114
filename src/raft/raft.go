package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"../labgob"
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type entry struct {
	Term    int
	Command interface{}
}

type NodeState int

const (
	FOLLOWER          NodeState     = 0
	LEADER            NodeState     = 1
	CANDIDATE         NodeState     = 2
	ElectionTime      time.Duration = 350
	HeartbeatInterval time.Duration = time.Duration(100) * time.Millisecond
	AppliedInterval   time.Duration = time.Duration(500) * time.Millisecond
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm    int
	votedFor       int
	state          NodeState
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	appliedTimer   *time.Timer
	log            []entry
	commitIndex    int
	lastLogIndex   int
	nextIndex      []int
	matchIndex     []int
	applyCh        chan ApplyMsg
	lastApplied    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == LEADER

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("fail to decode state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//log.Printf("server%d term%d log%v get vote request from candidate%d term%d lastLogIndex%d lastLogTerm%d", rf.me, rf.currentTerm, rf.log, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer func() {
		if reply.VoteGranted {
			//log.Printf("server%d votefor candidate%d", rf.me, args.CandidateId)
		}

	}()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}

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

	/*
		reply.Term=rf.currentTerm
		reply.VoteGranted=false
		if args.Term<rf.currentTerm||
			(args.Term==rf.currentTerm&&rf.votedFor!=-1&&rf.votedFor!=args.CandidateId){
			return
		}
		if args.Term>rf.currentTerm{
			rf.currentTerm=args.Term
			rf.convertTo(FOLLOWER)
		}
		if args.LastLogTerm < rf.log[rf.lastLogIndex].Term {
			return
		}
		if args.LastLogTerm == rf.log[rf.lastLogIndex].Term && args.LastLogIndex < rf.lastLogIndex {
			return
		}
		rf.votedFor=args.CandidateId
		reply.VoteGranted=true
		rf.electionTimer.Reset(randTimeDuration(ElectionTime))
	*/
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []entry
}

type AppendEntriesReply struct {
	Term            int
	Success         bool
	ConflictedTerm  int
	ConflictedIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//log.Printf("server%d state%d term%d commitIndex%d log%v get appendRPC from %d,entries:%v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.log, args.LeaderId, args.Entries)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
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
		}
	} else if rf.state == CANDIDATE {
		if args.Term >= rf.currentTerm {
			rf.currentTerm = args.Term
			rf.convertTo(FOLLOWER)
			rf.votedFor = args.LeaderId
		}
	} else {
		if args.Term >= rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = args.LeaderId
			rf.electionTimer.Reset(randTimeDuration(ElectionTime))
		}
	}
	if args.PrevLogIndex >= len(rf.log) {
		// false
		reply.ConflictedIndex = rf.lastLogIndex + 1
		reply.ConflictedTerm = -1
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// false
		conflictedTerm := rf.log[args.PrevLogIndex].Term
		rf.log = append(rf.log[:args.PrevLogIndex])
		rf.lastLogIndex = args.PrevLogIndex - 1

		var conflictedIndex int
		for conflictedIndex = args.PrevLogIndex - 1; conflictedIndex > 0; conflictedIndex-- {
			if rf.log[conflictedIndex].Term != conflictedTerm {
				break
			}
		}
		reply.ConflictedTerm = conflictedTerm
		reply.ConflictedIndex = conflictedIndex + 1

	} else {
		for i := 0; i < len(args.Entries); i++ {
			if args.PrevLogIndex+i+1 > rf.lastLogIndex || rf.log[args.PrevLogIndex+i+1].Term != args.Entries[i].Term {
				rf.log = append(rf.log[:args.PrevLogIndex+i+1], args.Entries[i:]...)
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

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.state == LEADER {

		item := entry{Term: rf.currentTerm, Command: command}
		rf.lastLogIndex++
		rf.log = append(rf.log, item)
		rf.matchIndex[rf.me] = rf.lastLogIndex
		//log.Printf("leader%d start item%v", rf.me, item)
	}
	index := rf.lastLogIndex
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = FOLLOWER
	rf.commitIndex = 0
	rf.applyCh = applyCh
	rf.lastApplied = 0
	rf.log = make([]entry, 0)
	rf.log = append(rf.log, entry{Term: 0, Command: 0})

	rf.heartbeatTimer = time.NewTimer(1 * time.Second)
	rf.heartbeatTimer.Stop()
	rf.electionTimer = time.NewTimer(randTimeDuration(ElectionTime))
	rf.appliedTimer = time.NewTimer(AppliedInterval)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.lastLogIndex = len(rf.log) - 1

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	go func() {
		for {
			select {
			case <-rf.electionTimer.C:
				rf.mu.Lock()
				if rf.state == FOLLOWER {
					rf.convertTo(CANDIDATE)
				} else {
					rf.startElection()
				}
				rf.persist()
				rf.mu.Unlock()
			case <-rf.heartbeatTimer.C:
				rf.mu.Lock()
				if rf.state == LEADER {
					rf.broadcast()
					rf.heartbeatTimer.Reset(HeartbeatInterval)
				}
				rf.mu.Unlock()
			case <-rf.appliedTimer.C:
				{
					rf.mu.Lock()
					rf.applyToStateMachine()
					rf.appliedTimer.Reset(AppliedInterval)
					rf.mu.Unlock()
				}
			}
		}
	}()

	return rf
}

func (rf *Raft) applyToStateMachine() {
	//log.Printf("server%d state%d term%d commitIndex%d log%v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.log)
	if rf.lastApplied == rf.commitIndex {
		return
	}
	entriesToApply := append([]entry{}, rf.log[rf.lastApplied+1:rf.commitIndex+1]...)
	go func(startIdx int, entriesToApply []entry) {
		for idx, entry := range entriesToApply {
			applyMsg := ApplyMsg{Command: entry.Command, CommandIndex: startIdx + idx, CommandValid: true}
			rf.applyCh <- applyMsg
		}
	}(rf.lastApplied+1, entriesToApply)
	/*
		go func(from int, to int) {
			for i := from; i <= to; i++ {

				applyMsg := ApplyMsg{Command: rf.log[i].Command, CommandIndex: i, CommandValid: true}
				rf.applyCh <- applyMsg

			}
		}(rf.lastApplied+1, rf.commitIndex)

	*/
	rf.lastApplied = rf.commitIndex
}
func (rf *Raft) convertTo(target NodeState) {
	if target == rf.state {
		return
	}
	if rf.state == FOLLOWER && target == LEADER {
		return
	}
	if rf.state == LEADER && target == CANDIDATE {
		return
	}
	//log.Printf("stateconvert:server%d convert from %d to %d", rf.me, rf.state, target)
	rf.state = target
	switch target {
	case FOLLOWER:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(randTimeDuration(ElectionTime))
		rf.votedFor = -1
	case LEADER:
		for server, _ := range rf.peers {
			rf.nextIndex[server] = rf.lastLogIndex + 1
			rf.matchIndex[server] = 0
		}
		//log.Printf("server%d become leader", rf.me)
		rf.electionTimer.Stop()
		rf.broadcast()
		rf.heartbeatTimer.Reset(HeartbeatInterval)
	case CANDIDATE:
		rf.startElection()
	}

}

// surrounded by lock
func (rf *Raft) broadcast() {
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			preLogIndex := rf.nextIndex[server] - 1
			var entries []entry
			if rf.nextIndex[server] > rf.lastLogIndex {
				entries = nil
			} else {
				entries = rf.log[rf.nextIndex[server] : rf.lastLogIndex+1]

			}
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: preLogIndex,
				PrevLogTerm:  rf.log[preLogIndex].Term,
				LeaderCommit: rf.commitIndex,
				Entries:      entries}
			rf.mu.Unlock()
			//log.Printf("leader%d commitIndex%d send appendRPC to server%d,entries:%v", rf.me, rf.commitIndex, server, args.Entries)
			var reply AppendEntriesReply
			if rf.sendAppendEntries(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				defer rf.persist()
				if rf.state != LEADER {
					return
				}
				if reply.Success {
					if preLogIndex+len(entries) >= rf.nextIndex[server] {
						rf.nextIndex[server] = preLogIndex + len(entries) + 1
						// update commitIndex here?
					}
					if preLogIndex+len(entries) > rf.matchIndex[server] {
						rf.matchIndex[server] = preLogIndex + len(entries)
						// update commitIndex here?
					}
				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.convertTo(FOLLOWER)
						//log.Printf("leader%d discover higher term from server%d and convertTo follower", rf.me, server)
					} else {
						// rf.nextIndex[server] -= 1

						if rf.log[reply.ConflictedIndex].Term == reply.ConflictedTerm {
							rf.nextIndex[server] = reply.ConflictedIndex + 1
						} else {
							rf.nextIndex[server] = reply.ConflictedIndex
						}

						/*
							if reply.ConflictedTerm == -1 {
								rf.nextIndex[server] = reply.ConflictedIndex
								log.Printf("conflictedTerm-1:server%d's nextIndex conflict,the reply conflictedIndex is %d,reset to %d", server, reply.ConflictedIndex, rf.nextIndex[server])
							} else {
								for i := args.PrevLogIndex; i >= 1; i-- {
									if rf.log[i-1].Term == reply.ConflictedTerm {
										rf.nextIndex[server] = i
										break
									}
								}
								log.Printf("conflictedTermelse:server%d's nextIndex conflict,the reply conflictedIndex is %d,reset to %d", server,reply.ConflictedIndex, rf.nextIndex[server])
							}

						*/

					}
				}

			}
		}(server)
	}

	index := make([]int, 0)
	index = append(index, rf.matchIndex...)
	sort.Ints(index)
	if index[len(rf.peers)/2] > rf.commitIndex && rf.log[index[len(rf.peers)/2]].Term == rf.currentTerm {
		rf.commitIndex = index[len(rf.peers)/2]
	}

	/*
		for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
			count := 0
			for _, matchIndex := range rf.matchIndex {
				if matchIndex >= N {
					count += 1
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = N
				break
			}
		}

	*/
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
		LastLogTerm:  rf.log[rf.lastLogIndex].Term}
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

func randTimeDuration(base time.Duration) time.Duration {
	return (time.Duration(rand.Intn(200)) + base) * time.Millisecond
}
