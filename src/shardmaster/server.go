package shardmaster

import (
	"../raft"
	"log"
	"time"
)
import "../labrpc"
import "sync"
import "../labgob"

type ShardMaster struct {
	serialMu  sync.Mutex
	logMu     sync.Mutex
	configsMu sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg

	// Your data here.

	configs      []Config // indexed by config num
	configNumber int
	serial       map[int64]int
	log          map[int]Op
}

type Op struct {
	// Your data here.
	Operator  string
	Servers   map[int][]string
	GIDs      []int
	Shard     int
	Num       int
	GID       int
	Cid       int64
	Rid       int
	NumConfig Config
}

func (sm *ShardMaster) duplicateCheck(Cid int64, Rid int) bool {
	sm.serialMu.Lock()
	res, ok := sm.serial[Cid]
	sm.serialMu.Unlock()
	if ok {
		if res >= Rid {
			return true
		}
	}
	return false
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	defer log.Printf("join %v reply %v", args, reply)
	if sm.duplicateCheck(args.Cid, args.Rid) {
		reply.Err = OK
		reply.WrongLeader = false
		return
	}
	cmd := Op{Operator: "Join", Servers: args.Servers, Cid: args.Cid, Rid: args.Rid}
	index, _, isLeader := sm.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	t := time.Now()
	for time.Since(t).Milliseconds() < WaitResult {
		sm.logMu.Lock()
		res, ok := sm.log[index]
		sm.logMu.Unlock()
		if ok {
			if res.Cid == cmd.Cid && res.Rid == cmd.Rid {
				reply.Err = OK
				return
			} else {
				reply.Err = ErrWrongLeader
				reply.WrongLeader = true
				return
			}
		} else {
			time.Sleep(200 * time.Millisecond)
		}
	}
	reply.Err = ErrWrongLeader
	reply.WrongLeader = true
	return

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	defer log.Printf("leave %v reply %v", args, reply)
	if sm.duplicateCheck(args.Cid, args.Rid) {
		reply.Err = OK
		reply.WrongLeader = false
		return
	}
	cmd := Op{Operator: "Leave", GIDs: args.GIDs, Cid: args.Cid, Rid: args.Rid}
	index, _, isLeader := sm.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	t := time.Now()
	for time.Since(t).Milliseconds() < WaitResult {
		sm.logMu.Lock()
		res, ok := sm.log[index]
		sm.logMu.Unlock()
		if ok {
			if res.Cid == cmd.Cid && res.Rid == cmd.Rid {
				reply.Err = OK
				return
			} else {
				reply.Err = ErrWrongLeader
				reply.WrongLeader = true
				return
			}
		} else {
			time.Sleep(200 * time.Millisecond)
		}
	}
	reply.Err = ErrWrongLeader
	reply.WrongLeader = true
	return
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	if sm.duplicateCheck(args.Cid, args.Rid) {
		reply.Err = OK
		return
	}
	cmd := Op{Operator: "Move", GID: args.GID, Shard: args.Shard, Cid: args.Cid, Rid: args.Rid}
	index, _, isLeader := sm.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	t := time.Now()
	for time.Since(t).Milliseconds() < WaitResult {
		sm.logMu.Lock()
		res, ok := sm.log[index]
		sm.logMu.Unlock()
		if ok {
			if res.Cid == cmd.Cid && res.Rid == cmd.Rid {
				reply.Err = OK
				return
			} else {
				reply.Err = ErrWrongLeader
				reply.WrongLeader = true
				return
			}
		} else {
			time.Sleep(200 * time.Millisecond)
		}
	}
	reply.Err = ErrWrongLeader
	reply.WrongLeader = true
	return
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	cmd := Op{Operator: "Query", Cid: args.Cid, Num: args.Num}
	index, _, isLeader := sm.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	t := time.Now()
	for time.Since(t).Milliseconds() < WaitResult {
		sm.logMu.Lock()
		res, ok := sm.log[index]
		sm.logMu.Unlock()
		if ok {
			if res.Cid == cmd.Cid && res.Operator == "Query" {
				reply.Err = OK
				reply.Config = res.NumConfig
				reply.WrongLeader = false
				return
			} else {
				reply.Err = ErrWrongLeader
				reply.WrongLeader = true
				return
			}
		} else {
			time.Sleep(200 * time.Millisecond)
		}
	}
	reply.Err = ErrWrongLeader
	reply.WrongLeader = true
	return
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardsm tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.serial = make(map[int64]int)
	sm.log = make(map[int]Op)

	go func() {
		for {

			applyMsg := <-sm.applyCh
			if applyMsg.CommandValid {
				cmd := applyMsg.Command.(Op)
				if cmd.Operator != "Query" {
					sm.serialMu.Lock()
					res, ok := sm.serial[cmd.Cid]
					if ok {
						if res >= cmd.Rid {
							sm.serialMu.Unlock()
							continue
						} else {
							sm.serial[cmd.Cid] = cmd.Rid
						}
					} else {
						sm.serial[cmd.Cid] = cmd.Rid
					}
					sm.serialMu.Unlock()
					if cmd.Operator == "Join" {

						sm.configsMu.Lock()
						// copy and update the new Gid->server map
						newConfig := Config{
							Num:    sm.configNumber + 1,
							Groups: make(map[int][]string),
						}
						log.Printf("newconfig %d join %v", newConfig.Num, cmd)
						newGids := make([]int, 0)
						for k, v := range sm.configs[sm.configNumber].Groups {
							newConfig.Groups[k] = v
						}
						for k, v := range cmd.Servers {
							_, ok := newConfig.Groups[k]
							if ok {
								continue
							}
							newConfig.Groups[k] = v
							newGids = append(newGids, k)
						}
						eachResponsible := make(map[int]int)
						var minResponsible int
						if len(newConfig.Groups) >= NShards {
							minResponsible = 1
						} else {
							minResponsible = NShards / len(newConfig.Groups)
						}
						//log.Printf("minres %d", minResponsible)
						var index int
						for i, gid := range sm.configs[sm.configNumber].Shards {
							if gid == 0 || eachResponsible[gid] >= minResponsible {
								if index >= len(newGids) {
									newConfig.Shards[i] = newGids[index%len(newGids)]
									index++
								} else {
									newConfig.Shards[i] = newGids[index]
									eachResponsible[newGids[index]]++
									if eachResponsible[newGids[index]] >= minResponsible {
										index++
									}
								}
							} else {
								newConfig.Shards[i] = gid
								eachResponsible[gid]++
							}
						}

						sm.configs = append(sm.configs, newConfig)
						sm.configNumber = newConfig.Num
						//log.Printf("join ver %d config %v", sm.configNumber, sm.configs[sm.configNumber])
						sm.configsMu.Unlock()
					} else if cmd.Operator == "Leave" {

						sm.configsMu.Lock()
						newConfig := Config{
							Num:    sm.configNumber + 1,
							Groups: make(map[int][]string),
						}
						log.Printf("newconfig %d leave %v", newConfig.Num, cmd)
						leaveGids := make(map[int]struct{})
						for _, gid := range cmd.GIDs {
							leaveGids[gid] = struct{}{}
						}
						newGids := make([]int, 0)
						for k, v := range sm.configs[sm.configNumber].Groups {
							_, ok := leaveGids[k]
							if ok {
								continue
							}
							newGids = append(newGids, k)
							newConfig.Groups[k] = v
						}
						if len(newGids) == 0 {
							for i, _ := range newConfig.Shards {
								newConfig.Shards[i] = 0
							}

						} else {
							var index int
							eachGidResponsible := make(map[int]int)
							var minResponsible int
							if len(newGids) >= NShards {
								minResponsible = 1
							} else {
								minResponsible = NShards / len(newGids)
							}
							for i, gid := range sm.configs[sm.configNumber].Shards {
								_, ok := leaveGids[gid]
								if !ok {
									newConfig.Shards[i] = gid
									eachGidResponsible[gid]++
								}
							}
							for i, gid := range sm.configs[sm.configNumber].Shards {
								_, ok := leaveGids[gid]
								if ok {
									for eachGidResponsible[newGids[index]] >= minResponsible && index < len(newGids) {
										index++
									}
									if index >= len(newGids) {
										break
									}
									newConfig.Shards[i] = newGids[index]
									eachGidResponsible[newGids[index]]++
								}
							}
						}
						sm.configs = append(sm.configs, newConfig)
						sm.configNumber = newConfig.Num
						//log.Printf("leave ver %d config %v", sm.configNumber, sm.configs[sm.configNumber])
						sm.configsMu.Unlock()
					} else if cmd.Operator == "Move" {
						//log.Printf("move %v", cmd)
						sm.configsMu.Lock()
						newConfig := Config{
							Num:    sm.configNumber + 1,
							Groups: make(map[int][]string),
						}
						for k, v := range sm.configs[sm.configNumber].Groups {
							newConfig.Groups[k] = v
						}
						for i, gid := range sm.configs[sm.configNumber].Shards {
							if i != cmd.Shard {
								newConfig.Shards[i] = gid
							} else {
								newConfig.Shards[i] = cmd.GID
							}
						}
						sm.configs = append(sm.configs, newConfig)
						sm.configNumber = newConfig.Num
						//log.Printf("move ver %d config %v", sm.configNumber, sm.configs[sm.configNumber])
						sm.configsMu.Unlock()
					}
				} else {
					sm.configsMu.Lock()
					if cmd.Num == -1 || cmd.Num > len(sm.configs)-1 {
						cmd.Num = len(sm.configs) - 1
					}

					cmd.NumConfig = sm.configs[cmd.Num]
					sm.configsMu.Unlock()
				}

				sm.logMu.Lock()
				sm.log[applyMsg.CommandIndex] = cmd
				sm.logMu.Unlock()
			}

		}

	}()

	return sm
}
