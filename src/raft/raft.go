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
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

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
	noopCount      int

	// snapshot
	snapshotLastIncludedIndex int
}

//
// get real index in snapshot version
// relative: about log
// absolute: others
//
func (rf *Raft) getRelativeLogIndex(index int) int {
	return index - rf.snapshotLastIncludedIndex
}
func (rf *Raft) getAbsoluteLogIndex(index int) int {
	return index + rf.snapshotLastIncludedIndex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.state == LEADER
	// log.Printf("server%d %v",rf.me,isleader)

	return term, isLeader
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
	e.Encode(rf.snapshotLastIncludedIndex)
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
	var snapshotLastIncludedIndex int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&snapshotLastIncludedIndex) != nil {
		panic("fail to decode state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.snapshotLastIncludedIndex = snapshotLastIncludedIndex
	}
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
	// log.Printf("raft%d commitIndex%d start cmd:%v,now the rf.log:%v",rf.me,rf.commitIndex,command,rf.log)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == LEADER {
		if command == "noop" {
			rf.noopCount++
		}
		item := entry{Term: rf.currentTerm, Command: command}
		// log.Printf("item:%v",item)
		rf.lastLogIndex++
		rf.log = append(rf.log, item)
		rf.persist()
		rf.matchIndex[rf.me] = rf.lastLogIndex
		rf.broadcast()
		//log.Printf("leader%d start item%v in index%d,its real index is %d", rf.me, item,rf.lastLogIndex,rf.lastLogIndex-rf.noopCount)
	}
	index := rf.lastLogIndex - rf.noopCount
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
	rf.electionTimer.Stop()
	rf.heartbeatTimer.Stop()
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
	rf.applyCh = applyCh
	rf.noopCount = 0
	rf.snapshotLastIncludedIndex = 0
	rf.log = make([]entry, 0)
	rf.log = append(rf.log, entry{Term: 0, Command: 0})

	rf.heartbeatTimer = time.NewTimer(1 * time.Second)
	rf.heartbeatTimer.Stop()
	rf.electionTimer = time.NewTimer(randTimeDuration(ElectionTime))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.lastLogIndex = len(rf.log) - 1 + rf.snapshotLastIncludedIndex
	rf.commitIndex = rf.snapshotLastIncludedIndex
	rf.lastApplied = rf.snapshotLastIncludedIndex

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

			}
		}
	}()
	go func() {
		noopCount := 0
		for {
			//log.Printf("server%d state%d term%d commitIndex%d lastlogIndex%d the entry in commitIndex is %v,log%v", rf.me, rf.state, rf.currentTerm, rf.commitIndex,rf.lastLogIndex,rf.log[rf.commitIndex] ,rf.log)
			rf.mu.Lock()
			if rf.lastApplied == rf.commitIndex {
				rf.mu.Unlock()
			} else {
				entriesToApply := append([]entry{}, rf.log[rf.getRelativeLogIndex(rf.lastApplied+1):rf.getRelativeLogIndex(rf.commitIndex+1)]...)
				startIdx := rf.lastApplied + 1
				rf.lastApplied = rf.commitIndex
				rf.mu.Unlock()
				for idx, entry := range entriesToApply {
					if entry.Command == "noop" {
						noopCount++
						continue
					}
					// log.Printf("server%d apply {cmd:%v,cmdIndex:%d}",rf.me,entry.Command,startIdx+idx-noopCount)
					applyMsg := ApplyMsg{Command: entry.Command, CommandIndex: startIdx + idx - noopCount, CommandValid: true}
					//log.Printf("rf%d commit%d log%v apply %v",rf.me,rf.commitIndex,rf.log,applyMsg)
					rf.applyCh <- applyMsg

				}
			}

			time.Sleep(AppliedInterval)
		}
	}()
	return rf
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
		rf.noopCount = 0
		for i := 1; i <= rf.getRelativeLogIndex(rf.lastLogIndex); i++ {
			if rf.log[i].Command == "noop" {
				rf.noopCount++
			}
		}
		go func() {
			rf.Start("noop")
		}()

		rf.electionTimer.Stop()
		rf.broadcast()
		rf.heartbeatTimer.Reset(HeartbeatInterval)
	case CANDIDATE:
		rf.startElection()
	}

}

func randTimeDuration(base time.Duration) time.Duration {
	return (time.Duration(rand.Intn(200)) + base) * time.Millisecond
}

// the trimmingIndex here is the absolute index
func (rf *Raft) TrimmingLogWithSnapshot(trimmingIndex int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if trimmingIndex <= rf.snapshotLastIncludedIndex {
		return
	}
	rf.log = rf.log[rf.getRelativeLogIndex(trimmingIndex):]
	rf.snapshotLastIncludedIndex = trimmingIndex
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)

}

func (rf *Raft) syncSnapshotWith(server int) {
	//log.Printf("leader%d sync with server%d",rf.me,server)
	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshotLastIncludedIndex,
		LastIncludedTerm:  rf.log[0].Term,
		Data:              rf.persister.ReadSnapshot()}
	rf.mu.Unlock()
	var reply InstallSnapshotReply
	if rf.sendInstallSnapshot(server, &args, &reply) {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.convertTo(FOLLOWER)
			rf.persist()
		} else {
			if rf.matchIndex[server] < args.LastIncludedIndex {
				rf.matchIndex[server] = args.LastIncludedIndex
			}
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || args.LastIncludedIndex < rf.snapshotLastIncludedIndex {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertTo(FOLLOWER)
		rf.persist()
	}
	lastIncludedRelativeIndex := rf.getRelativeLogIndex(args.LastIncludedIndex)
	if len(rf.log) > lastIncludedRelativeIndex &&
		rf.log[lastIncludedRelativeIndex].Term == args.LastIncludedTerm {
		rf.log = rf.log[lastIncludedRelativeIndex:]
	} else {
		rf.log = []entry{{Term: args.LastIncludedTerm, Command: nil}}
		rf.lastLogIndex = args.LastIncludedIndex
	}
	rf.snapshotLastIncludedIndex = args.LastIncludedIndex
	rf.persist()
	if rf.commitIndex < rf.snapshotLastIncludedIndex {
		rf.commitIndex = rf.snapshotLastIncludedIndex
	}
	if rf.lastApplied < rf.snapshotLastIncludedIndex {
		rf.lastApplied = rf.snapshotLastIncludedIndex
	}
	if rf.lastApplied > rf.snapshotLastIncludedIndex {
		return
	}
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.Data)
	installSnapshotCmd := ApplyMsg{
		Command:      "InstallSnapshot",
		CommandValid: false,
		CommandData:  args.Data}
	go func() {
		rf.applyCh <- installSnapshotCmd
	}()

}
