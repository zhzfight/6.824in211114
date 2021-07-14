package shardkv

import (
	"../shardmaster"
	"bytes"
	"log"
	"time"
)
import "../labrpc"
import "../raft"
import "sync"
import "../labgob"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cid          int64
	Rid          int
	Operator     string
	Key          string
	Value        string
	Config       shardmaster.Config
	ShardDB      map[string]string
	Shard        int
	ConfigNumber int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	shardConfig  shardmaster.Config
	configNumber int
	configLock   sync.Mutex
	smck         *shardmaster.Clerk

	lastAppliedIndex int
	preAppliedIndex  int
	database         map[string]string
	serial           map[int64]int
	log              map[int]Op
	serialMu         sync.Mutex
	databaseMu       sync.Mutex
	logMu            sync.Mutex
	shards           map[int]struct{}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.configLock.Lock()
	if _, ok := kv.shards[key2shard(args.Key)]; !ok {
		reply.Err = ErrWrongGroup
		kv.configLock.Unlock()
		return
	}
	cmd := Op{Key: args.Key, Cid: args.Cid, Rid: args.Rid, Operator: "Get", ConfigNumber: kv.configNumber}
	kv.configLock.Unlock()

	index, _, isLeader := kv.rf.Start(cmd)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	t := time.Now()
	for time.Since(t).Milliseconds() < WaitResult {
		kv.logMu.Lock()
		res, ok := kv.log[index]
		kv.logMu.Unlock()
		if ok {
			if res.Cid == cmd.Cid && res.Rid == cmd.Rid {
				if cmd.ConfigNumber == res.ConfigNumber {
					if len(res.Value) == 0 {
						reply.Err = ErrNoKey
						return
					} else {
						reply.Err = OK
						reply.Value = res.Value
						return
					}
				} else {
					reply.Err = ErrWrongGroup
					return
				}
			} else {
				reply.Err = ErrWrongLeader
				return
			}
		} else {
			time.Sleep(20 * time.Millisecond)
		}
	}
	reply.Err = ErrWrongLeader
	return

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.configLock.Lock()
	if _, ok := kv.shards[key2shard(args.Key)]; !ok {
		reply.Err = ErrWrongGroup
		kv.configLock.Unlock()
		return
	}

	kv.serialMu.Lock()
	res, ok := kv.serial[args.Cid]
	kv.serialMu.Unlock()
	if ok {
		if res >= args.Rid {
			reply.Err = OK
			kv.configLock.Unlock()
			return
		}
	}
	cmd := Op{Operator: args.Op, Key: args.Key, Value: args.Value, Cid: args.Cid, Rid: args.Rid, ConfigNumber: kv.configNumber}
	kv.configLock.Unlock()
	index, _, isLeader := kv.rf.Start(cmd)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	t := time.Now()
	for time.Since(t).Milliseconds() < WaitResult {
		kv.logMu.Lock()
		res, ok := kv.log[index]
		kv.logMu.Unlock()
		if ok {
			if res.Cid == cmd.Cid && res.Rid == cmd.Rid {
				if cmd.ConfigNumber == res.ConfigNumber {
					reply.Err = OK
					return
				} else {
					reply.Err = ErrWrongGroup
					return
				}
			} else {
				reply.Err = ErrWrongLeader
				return
			}
		} else {
			time.Sleep(20 * time.Millisecond)
		}
	}
	reply.Err = ErrWrongLeader
	return
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific Shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.preAppliedIndex = 0
	kv.lastAppliedIndex = 0
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.serial = make(map[int64]int)
	kv.log = make(map[int]Op)
	kv.database = make(map[string]string)
	kv.installSnapshot(persister.ReadSnapshot())

	kv.smck = shardmaster.MakeClerk(masters)
	kv.shards = make(map[int]struct{})

	go kv.periodicallyGetLatestConfig()

	go func() {
		for {
			applyMsg := <-kv.applyCh
			if applyMsg.CommandValid {
				cmd := applyMsg.Command.(Op)

				if cmd.Operator == "Put" || cmd.Operator == "Append" || cmd.Operator == "Get" {
					log.Printf("gid %d server %d cmd %v shard %d", kv.gid, kv.me, cmd, key2shard(cmd.Key))
				} else {
					log.Printf("gid %d server %dcmd %v", kv.gid, kv.me, cmd)
				}

				if cmd.Operator == "Put" || cmd.Operator == "Append" {
					kv.configLock.Lock()
					if cmd.ConfigNumber == kv.configNumber {
						kv.serialMu.Lock()
						kv.databaseMu.Lock()
						res, ok := kv.serial[cmd.Cid]
						if ok {
							if res >= cmd.Rid {
								kv.databaseMu.Unlock()
								kv.serialMu.Unlock()
								continue
							} else {
								kv.serial[cmd.Cid] = cmd.Rid
							}
						} else {
							kv.serial[cmd.Cid] = cmd.Rid
						}

						if cmd.Operator == "Put" {
							kv.database[cmd.Key] = cmd.Value
						} else if cmd.Operator == "Append" {
							res, ok := kv.database[cmd.Key]
							if ok {
								res = res + cmd.Value
								kv.database[cmd.Key] = res
							} else {
								kv.database[cmd.Key] = res
							}
						}
						kv.lastAppliedIndex = applyMsg.CommandIndex
						kv.databaseMu.Unlock()
						kv.serialMu.Unlock()
					}
					kv.configLock.Unlock()
				} else if cmd.Operator == "Get" {
					kv.databaseMu.Lock()
					value, have := kv.database[cmd.Key]
					kv.databaseMu.Unlock()
					if have {
						cmd.Value = value
					}

				} else if cmd.Operator == "ConfigChange" {
					kv.configLock.Lock()
					if cmd.Config.Num > kv.configNumber {
						if _, isLeader := kv.rf.GetState(); isLeader {
							newShards := make(map[int]struct{})
							for shard, gid := range cmd.Config.Shards {
								if gid == kv.gid {
									newShards[shard] = struct{}{}
								}
							}
							newResponsibleShard := make(map[int]struct{})
							for k, _ := range newShards {
								if _, ok := kv.shards[k]; !ok {
									newResponsibleShard[k] = struct{}{}
								}
							}
							if len(newResponsibleShard) > 0 {
								for shard, _ := range newResponsibleShard {
									oldResponsibleGid := kv.shardConfig.Shards[shard]
									if oldResponsibleGid == 0 {
										cmd := Op{Operator: "ShardMigration", Shard: shard}
										kv.rf.Start(cmd)
									} else if servers, ok := kv.shardConfig.Groups[oldResponsibleGid]; ok {
										go func(shard int, servers []string) {
											var args shardArgs
											args.Shard = shard
											for {
												for si := 0; si < len(servers); si++ {
													srv := kv.make_end(servers[si])
													var reply shardReply
													ok := srv.Call("ShardKV.ShardMigration", &args, &reply)
													if ok && (reply.Err == OK) {
														cmd := Op{Operator: "ShardMigration", ShardDB: reply.ShardDb, Shard: shard}
														kv.rf.Start(cmd)
														return
													}
													if ok && (reply.Err == ErrWrongGroup) {
														return
													}
													// ... not ok, or ErrWrongLeader
													if ok && reply.Err == ErrWrongLeader {
														continue
													}
												}
												time.Sleep(poll)
											}
										}(shard, servers)
									}
								}
							}
							for k, _ := range kv.shards {
								if _, ok := newShards[k]; !ok {
									delete(kv.shards, k)
								}
							}
						}
						kv.shardConfig = cmd.Config
						kv.configNumber = cmd.Config.Num
					}
					kv.configLock.Unlock()

				} else if cmd.Operator == "ShardMigration" {
					kv.configLock.Lock()
					kv.databaseMu.Lock()
					for k, v := range cmd.ShardDB {
						kv.database[k] = v
					}
					kv.shards[cmd.Shard] = struct{}{}

					kv.databaseMu.Unlock()
					kv.configLock.Unlock()
				}

				kv.configLock.Lock()
				cmd.ConfigNumber = kv.configNumber
				kv.configLock.Unlock()
				kv.logMu.Lock()
				kv.log[applyMsg.CommandIndex] = cmd
				kv.logMu.Unlock()

			} else {
				switch applyMsg.Command.(string) {
				case "InstallSnapshot":
					kv.installSnapshot(applyMsg.CommandData)
				}
			}
		}
	}()
	if maxraftstate != -1 {
		go kv.takeSnapshot(persister)
	}

	return kv
}
func (kv *ShardKV) takeSnapshot(persister *raft.Persister) {
	for {
		if persister.RaftStateSize() > 4*kv.maxraftstate {
			kv.serialMu.Lock()
			kv.databaseMu.Lock()
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.database)
			e.Encode(kv.serial)
			lastAppliedIndex := kv.lastAppliedIndex
			kv.databaseMu.Unlock()
			kv.serialMu.Unlock()
			data := w.Bytes()
			//log.Printf("server%d:snapshot trimming %d",kv.me,lastAppliedIndex)
			kv.rf.TrimmingLogWithSnapshot(lastAppliedIndex, data)

			// wait for start(cmd) timeout
			time.Sleep(WaitResult)

			// clean up the kv.log
			kv.logMu.Lock()
			for i := kv.preAppliedIndex + 1; i <= lastAppliedIndex; i++ {
				delete(kv.log, i)
			}
			kv.logMu.Unlock()
			kv.preAppliedIndex = lastAppliedIndex

		}
		time.Sleep(WaitResult)

	}
}

func (kv *ShardKV) installSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var database map[string]string
	var serial map[int64]int
	if d.Decode(&database) != nil ||
		d.Decode(&serial) != nil {
		panic("fail to decode snapshot")
	} else {

		kv.database = database
		kv.serial = serial

	}
}

func (kv *ShardKV) ShardMigration(args *shardArgs, reply *shardReply) {
	kv.configLock.Lock()
	kv.databaseMu.Lock()
	reply.ShardDb = make(map[string]string)
	for k, v := range kv.database {
		if key2shard(k) == args.Shard {
			reply.ShardDb[k] = v
			delete(kv.database, k)
		}
	}
	if len(reply.ShardDb) == 0 {
		reply.Err = ErrWrongGroup
	} else {
		reply.Err = OK
	}
	kv.databaseMu.Unlock()
	kv.configLock.Unlock()
	return
}

func (kv *ShardKV) periodicallyGetLatestConfig() {
	for {
		if _, isLeader := kv.rf.GetState(); isLeader {
			latestConfig := kv.smck.Query(-1)
			kv.configLock.Lock()
			if latestConfig.Num > kv.configNumber {
				kv.configLock.Unlock()
				cmd := Op{Config: latestConfig, Operator: "ConfigChange"}
				kv.rf.Start(cmd)
				log.Printf("gid %d server %d get latestConfig %v", kv.gid, kv.me, latestConfig)
				time.Sleep(4 * poll)
			} else {
				kv.configLock.Unlock()
				time.Sleep(poll)
			}
		}
	}
}
