package shardkv

import (
	"../shardmaster"
	"bytes"
	"math/rand"
	"time"
)
import "../labrpc"
import "../raft"
import "sync"
import "../labgob"

var count int32

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cid                 int64
	Rid                 int
	Operator            string
	Key                 string
	Value               string
	Config              shardmaster.Config
	ShardDB             map[string]string
	Shard               int
	GroupConfigNumber   int
	RequestConfigNumber int
	DeleteConfigNumber  int

	SerialDB map[int64]int
	Err      Err
	FromGid  int
	ToGid    int
	Tag      int32
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
	shardConfig  []shardmaster.Config
	configNumber int
	smck         *shardmaster.Clerk

	lastAppliedIndex int
	preAppliedIndex  int
	database         map[string]string
	serial           map[int64]int
	log              map[int]Op
	databaseMu       sync.Mutex
	logMu            sync.Mutex
	rfMu             sync.Mutex
	shards           []int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	shard := key2shard(args.Key)
	thisShardConfigNumber := kv.shards[shard]
	if args.ConfigNumber != kv.configNumber {
		//log.Printf("cid %d get shard %d in gid %d args.conf %d shardconf %d kv.conf %d and request", args.Cid, key2shard(args.Key), kv.gid, args.ConfigNumber, thisShardConfigNumber, kv.configNumber)

		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	if thisShardConfigNumber != args.ConfigNumber {

		//因为当前shard的Conf没有达到最新的conf，所以要请求
		//log.Printf("cid %d put shard %d in gid %d args.conf %d shardconf %d kv.conf %d and request", args.Cid, key2shard(args.Key), kv.gid, args.ConfigNumber, thisShardConfigNumber, kv.configNumber)
		go kv.requestShard1(shard, args.ConfigNumber)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	cmd := Op{Key: args.Key, Cid: args.Cid, Rid: args.Rid, Operator: "Get", GroupConfigNumber: args.ConfigNumber, Tag: rand.Int31()}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(cmd)
	//log.Printf("cid %d gid %d start in index %d server %d cmd:%v", args.Cid, kv.gid,  index,kv.me, cmd)
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
			if res.Tag == cmd.Tag {
				reply.Err = res.Err
				if res.Err == OK {
					reply.Value = res.Value
				}
				return
			} else {
				//log.Printf("cid %d gid %d server %d fail in index %d cmd:%v", args.Cid, kv.gid, kv.me, index, cmd)
				reply.Err = ErrWrongLeader
				return
			}
		} else {
			time.Sleep(100 * time.Millisecond)
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

	kv.mu.Lock()

	shard := key2shard(args.Key)
	thisShardConfigNumber := kv.shards[shard]
	if args.ConfigNumber != kv.configNumber {
		//log.Printf("cid %d put shard %d in gid %d args.conf %d shardconf %d kv.conf %d", args.Cid, key2shard(args.Key), kv.gid, args.ConfigNumber, thisShardConfigNumber, kv.configNumber)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	if thisShardConfigNumber != args.ConfigNumber {

		//因为当前shard的Conf没有达到最新的conf，所以要请求

		//log.Printf("cid %d put shard %d in gid %d args.conf %d shardconf %d kv.conf %d and request", args.Cid, key2shard(args.Key), kv.gid, args.ConfigNumber, thisShardConfigNumber, kv.configNumber)
		go kv.requestShard1(shard, args.ConfigNumber)

		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	res, ok := kv.serial[args.Cid]
	if ok {
		if res >= args.Rid {
			reply.Err = OK

			kv.mu.Unlock()
			return
		}
	}
	cmd := Op{Operator: args.Op, Key: args.Key, Value: args.Value, Cid: args.Cid, Rid: args.Rid, GroupConfigNumber: args.ConfigNumber, Tag: rand.Int31()}
	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(cmd)
	//log.Printf("cid %d gid %d start in index %d server %d cmd:%v", args.Cid, kv.gid,  index,kv.me, cmd)

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
			if res.Tag == cmd.Tag {
				reply.Err = res.Err
				return
			} else {
				//log.Printf("cid %d gid %d server %d fail in index %d cmd:%v", args.Cid, kv.gid, kv.me, index, cmd)
				reply.Err = ErrWrongLeader
				return
			}
		} else {
			time.Sleep(100 * time.Millisecond)
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

	rand.Seed(time.Now().UnixNano())
	kv.preAppliedIndex = 0
	kv.lastAppliedIndex = 0
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.serial = make(map[int64]int)
	kv.log = make(map[int]Op)
	kv.database = make(map[string]string)
	kv.shardConfig = make([]shardmaster.Config, 0)
	kv.shards = make([]int, shardmaster.NShards)
	kv.installSnapshot(persister.ReadSnapshot())

	kv.smck = shardmaster.MakeClerk(masters)
	if len(kv.shardConfig) == 0 {
		config0 := shardmaster.Config{Num: 0, Groups: make(map[int][]string)}
		kv.shardConfig = append(kv.shardConfig, config0)
	}
	kv.configNumber = kv.shardConfig[len(kv.shardConfig)-1].Num
	go kv.periodicallyGetLatestConfig()

	go func() {
		for {
			applyMsg := <-kv.applyCh
			if applyMsg.CommandValid {
				cmd := applyMsg.Command.(Op)

				kv.mu.Lock()
				if _, isLeader := kv.rf.GetState(); isLeader {
					if cmd.Operator == "Put" || cmd.Operator == "Append" || cmd.Operator == "Get" {
						//log.Printf("gid %d now shards %v index %d server %d cmd %v shard %d cmd.num %d kv.num %d shardnum %d", kv.gid, kv.shards, applyMsg.CommandIndex, kv.me, cmd, key2shard(cmd.Key), cmd.GroupConfigNumber, kv.configNumber, kv.shards[key2shard(cmd.Key)])
					} else {
						//log.Printf("gid %d now shards %v index %d server %d cmd %v cmd.num %d kv.num %d shardnum %d", kv.gid, kv.shards, applyMsg.CommandIndex, kv.me, cmd, cmd.GroupConfigNumber, kv.configNumber, kv.shards[key2shard(cmd.Key)])
					}
				}
				if cmd.Operator == "Put" || cmd.Operator == "Append" {
					if cmd.GroupConfigNumber == kv.configNumber && kv.shards[key2shard(cmd.Key)] == kv.configNumber {
						res, ok := kv.serial[cmd.Cid]
						if ok {
							if res >= cmd.Rid {
								kv.mu.Unlock()
								continue
							} else {
								kv.serial[cmd.Cid] = cmd.Rid
							}
						} else {
							kv.serial[cmd.Cid] = cmd.Rid
						}

						kv.databaseMu.Lock()
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
						kv.databaseMu.Unlock()
						cmd.Err = OK
					} else {
						//log.Printf("cid %d put shard %d groupconf %d kvconf %d shardconf %d", cmd.Cid, cmd.Shard, cmd.GroupConfigNumber, kv.configNumber, kv.shards[key2shard(cmd.Key)])
						cmd.Err = ErrWrongGroup
					}

				} else if cmd.Operator == "Get" {
					if cmd.GroupConfigNumber == kv.configNumber && kv.shards[key2shard(cmd.Key)] == kv.configNumber {
						kv.databaseMu.Lock()
						value, have := kv.database[cmd.Key]
						kv.databaseMu.Unlock()
						if have {
							cmd.Value = value
							cmd.Err = OK
						} else {
							cmd.Err = ErrNoKey
						}

					} else {
						//log.Printf("cid %d get shard %d groupconf %d kvconf %d shardconf %d", cmd.Cid, cmd.Shard, cmd.GroupConfigNumber, kv.configNumber, kv.shards[key2shard(cmd.Key)])
						cmd.Err = ErrWrongGroup
					}

				} else if cmd.Operator == "ConfigChange" {

					if cmd.Config.Num > kv.configNumber {
						if _, isLeader := kv.rf.GetState(); isLeader {
							newShards := make(map[int]struct{})
							for shard, gid := range cmd.Config.Shards {
								if gid == kv.gid {
									newShards[shard] = struct{}{}
								}
							}
							//log.Printf("gid %d newshards %v", kv.gid, newShards)
							//log.Printf("gid %d oldshards %v", kv.gid, kv.shards)
							newResponsibleShard := make(map[int]struct{})
							for k, _ := range newShards {
								if kv.shards[k] == kv.configNumber {
									//改成cmd更新如何？
									cmd := Op{Operator: "InstallShardMigration", Shard: k, GroupConfigNumber: cmd.Config.Num, FromGid: kv.gid}
									kv.rf.Start(cmd)
									//log.Printf("installmigration gid %d start in index %d cmd:%v here1", kv.gid, index, cmd)
									//kv.shards[k] = cmd.Config.Num
								} else {
									newResponsibleShard[k] = struct{}{}
								}
							}

							if len(newResponsibleShard) > 0 {
								for shard, _ := range newResponsibleShard {
									go kv.requestShard1(shard, cmd.Config.Num)
								}
							}
						}
						kv.shardConfig = append(kv.shardConfig, cmd.Config)
						kv.configNumber = cmd.Config.Num
						//log.Printf("gid %d nowconfigs %v", kv.me, kv.shardConfig)
					}
				} else if cmd.Operator == "ShardMigration" {
					if kv.shards[cmd.Shard] == cmd.RequestConfigNumber {
						kv.databaseMu.Lock()
						cmd.ShardDB = make(map[string]string)
						for k, v := range kv.database {
							if key2shard(k) == cmd.Shard {
								cmd.ShardDB[k] = v
							}
						}
						cmd.SerialDB = make(map[int64]int)
						for k, v := range kv.serial {
							cmd.SerialDB[k] = v
						}
						if _, isLeader := kv.rf.GetState(); isLeader {
							//log.Printf("success gid %d migrate shard %d to gid %d confignumber %d nowshards %v", kv.gid, cmd.Shard, cmd.ToGid, cmd.RequestConfigNumber, kv.shards)
						}
						kv.databaseMu.Unlock()
						cmd.Err = OK
					} else {
						if _, isLeader := kv.rf.GetState(); isLeader {
							//log.Printf("fail gid %d migrate shard %d to gid %d request %d shardconf %d nowshards %v", kv.gid, cmd.Shard, cmd.ToGid, cmd.RequestConfigNumber, kv.shards[cmd.Shard], kv.shards)
						}
						cmd.Err = ErrWrongGroup
					}

				} else if cmd.Operator == "InstallShardMigration" {
					if cmd.FromGid == kv.gid {
						if cmd.GroupConfigNumber > kv.shards[cmd.Shard] {
							kv.shards[cmd.Shard] = cmd.GroupConfigNumber
						}
					} else if kv.shards[cmd.Shard] < cmd.RequestConfigNumber {
						if cmd.ShardDB != nil {
							kv.databaseMu.Lock()
							for k, v := range cmd.ShardDB {
								kv.database[k] = v
							}
							kv.databaseMu.Unlock()
						}
						kv.shards[cmd.Shard] = cmd.GroupConfigNumber
						for k, v := range cmd.SerialDB {
							sequenceNum, ok := kv.serial[k]
							if ok {
								if sequenceNum < v {
									kv.serial[k] = v
								}
							} else {
								kv.serial[k] = v
							}
						}
					}
					kv.databaseMu.Lock()
					if _, isLeader := kv.rf.GetState(); isLeader {
						//log.Printf("gid %d install shard %d:in config %d now shards %v serial %v db %v", kv.gid, cmd.Shard, cmd.GroupConfigNumber, kv.shards, kv.serial, kv.database)
						if cmd.FromGid != kv.gid {
							//log.Printf("gid %d start delete gid %d shard %d in conf %d nowshard %v", kv.gid, cmd.FromGid, cmd.Shard, cmd.RequestConfigNumber, kv.shards)
							if servers, ok := kv.shardConfig[cmd.RequestConfigNumber].Groups[cmd.FromGid]; ok {
								go kv.deleteShard(cmd.Shard, cmd.RequestConfigNumber, servers)
							}
						}
					}
					kv.databaseMu.Unlock()

				} else if cmd.Operator == "DeleteShard" {
					if cmd.DeleteConfigNumber == kv.shards[cmd.Shard] {
						kv.databaseMu.Lock()
						for k, _ := range kv.database {
							if key2shard(k) == cmd.Shard {
								delete(kv.database, k)
							}
						}
						kv.databaseMu.Unlock()
						kv.shards[cmd.Shard] = -kv.shards[cmd.Shard]
						//log.Printf("success delete gid %d shard %d nowshards %v", kv.gid, cmd.Shard, kv.shards)
						cmd.Err = OK
					} else if kv.shards[cmd.Shard] == 0 {
						cmd.Err = OK
					} else {
						//log.Printf("fail to delete gid %d shard %d deleteConfigNumber %d kvshardconf %d", kv.gid, cmd.Shard, cmd.DeleteConfigNumber, kv.shards[cmd.Shard])
					}
				}

				kv.mu.Unlock()

				kv.logMu.Lock()
				kv.lastAppliedIndex = applyMsg.CommandIndex
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

		if persister.RaftStateSize() > 8*kv.maxraftstate {
			kv.mu.Lock()
			kv.databaseMu.Lock()
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.database)
			e.Encode(kv.serial)
			e.Encode(kv.shards)
			e.Encode(kv.shardConfig)
			lastAppliedIndex := kv.lastAppliedIndex
			//log.Printf("takesnapshot gid %d server %d: preindex %d lastindex %d", kv.gid, kv.me, kv.preAppliedIndex, kv.lastAppliedIndex)

			kv.databaseMu.Unlock()
			kv.mu.Unlock()
			data := w.Bytes()

			kv.rf.TrimmingLogWithSnapshot(lastAppliedIndex, data)

			// wait for start(cmd) timeout
			time.Sleep(4000 * time.Millisecond)
			// clean up the kv.log
			kv.logMu.Lock()
			for i := kv.preAppliedIndex + 1; i <= lastAppliedIndex; i++ {
				//log.Printf("gid %d server %d index %d cmd %v", kv.gid, kv.me, i, kv.log[i])
				delete(kv.log, i)
			}
			kv.logMu.Unlock()
			kv.preAppliedIndex = lastAppliedIndex

		}
		time.Sleep(200 * time.Millisecond)

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
	shards := make([]int, shardmaster.NShards)
	var configs []shardmaster.Config
	if d.Decode(&database) != nil ||
		d.Decode(&serial) != nil ||
		d.Decode(&shards) != nil ||
		d.Decode(&configs) != nil {
		panic("fail to decode snapshot")
	} else {

		kv.database = database
		kv.serial = serial
		kv.shards = shards
		//log.Printf("gid %d installsnapshotshard shard %v db %v serial %v", kv.gid, shards, kv.database, kv.serial)
		kv.shardConfig = configs

		//log.Printf("gid %d server %d install snapshot %v", kv.gid, kv.me, kv.shardConfig)
	}
}

func (kv *ShardKV) ShardMigration(args *shardArgs, reply *shardReply) {

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()

	if args.RequestConfigNumber > kv.configNumber {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if args.RequestConfigNumber != kv.shards[args.Shard] {
		if kv.shards[args.Shard] == -args.RequestConfigNumber {

		} else if args.RequestConfigNumber > kv.shards[args.Shard] {
			go kv.requestShard1(args.Shard, args.RequestConfigNumber)
		}

		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	cmd := Op{Shard: args.Shard, GroupConfigNumber: args.GroupConfigNumber, RequestConfigNumber: args.RequestConfigNumber, Operator: "ShardMigration", ToGid: args.Gid, Tag: rand.Int31()}
	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(cmd)
	//log.Printf("migration gid %d start in index %d server %d cmd:%v", kv.gid,  index,kv.me, cmd)
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
			//log.Printf("shardmigrate res %v", res)
			if res.Operator == "ShardMigration" && res.Tag == cmd.Tag {
				if res.Err == OK {
					reply.ShardDb = make(map[string]string)
					reply.SerialDb = make(map[int64]int)
					for k, v := range res.ShardDB {
						reply.ShardDb[k] = v
					}
					for k, v := range res.SerialDB {
						reply.SerialDb[k] = v
					}
					reply.Err = res.Err
				} else {
					reply.Err = res.Err
				}

				return
			} else {
				//log.Printf("gid %d from gid %d server %d failly get shardmigration %d because errorwrongleader,index %d res:%v,cmd:%v", cmd.ToGid, kv.gid, kv.me, args.Shard, index, res, cmd)
				reply.Err = ErrWrongLeader
				return
			}
		} else {
			time.Sleep(100 * time.Millisecond)
		}
	}

	reply.Err = ErrWrongLeader
	return

}

func (kv *ShardKV) periodicallyGetLatestConfig() {
	for {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			latestConfig := kv.smck.Query(kv.configNumber + 1)

			if latestConfig.Num > kv.configNumber {
				//log.Printf("gid %d query %d and get latestConfig %v nowshard %v", kv.gid, kv.configNumber+1, latestConfig, kv.shards)
				cmd := Op{Config: latestConfig, Operator: "ConfigChange"}
				kv.mu.Unlock()
				kv.rf.Start(cmd)
				time.Sleep(3 * poll * time.Millisecond)
			} else {
				kv.mu.Unlock()
			}
		}
		time.Sleep(poll * time.Millisecond)
	}
}

/*
func (kv *ShardKV) requestShard(shard int, servers []string, oldgid int, requestConfigNumber int, currentConfigNumber int) {
	log.Printf("gid %d request shard %d from gid %d requestconfignumber %d currentnumber %d", kv.gid, shard, oldgid, requestConfigNumber, currentConfigNumber)
	atomic.AddInt32(&count, 1)
	log.Printf("gid %d requestcount %d", kv.gid, count)
	var args shardArgs
	args.Shard = shard
	args.RequestConfigNumber = requestConfigNumber
	args.Gid = kv.gid
	args.Server = kv.me
	args.GroupConfigNumber = currentConfigNumber
	for {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var Err shardReply
			ok := srv.Call("ShardKV.ShardMigration", &args, &Err)
			if ok && (Err.Err == OK) {
				cmd := Op{Operator: "InstallShardMigration", ShardDB: Err.ShardDb, Shard: shard, GroupConfigNumber: currentConfigNumber, SerialDB: Err.SerialDb, Gid: oldgid}
				log.Printf("installshardmigration gid %d start cmd %v request", kv.gid, cmd)
				kv.rf.Start(cmd)
				return
			}
			if ok && (Err.Err == ErrWrongGroup) {
				break
			}
			// ... not ok, or ErrWrongLeader
			if ok && Err.Err == ErrWrongLeader {
				continue
			}
		}
		kv.mu.Lock()
		//如果kv.shards[shard] >= requestConfigNumber,说明已经通过其他的线程请求到了，这时候就可以退出当前请求了
		if kv.shards[shard] >= requestConfigNumber {
			if kv.shards[shard] < currentConfigNumber {
				cmd := Op{Operator: "InstallShardMigration", GroupConfigNumber: currentConfigNumber, Gid: kv.gid}
				kv.rf.Start(cmd)
			}
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		time.Sleep(poll * time.Millisecond)

	}
}

*/
func (kv *ShardKV) ShardDeletion(args *deleteArgs, reply *deleteReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	//log.Printf("gid %d server %d confignumber %d get shardmigration request from gid %d server %d shard %d confignumber %d",
	//	kv.gid, kv.me, kv.configNumber, args.Gid, args.Server, args.Shard, args.RequestConfigNumber)

	//如果指定要删除的那个confignumber已经变化了，说明已经被删除或者更新了？
	if args.DeleteConfigNumber != kv.shards[args.Shard] {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	cmd := Op{Shard: args.Shard, DeleteConfigNumber: args.DeleteConfigNumber, Operator: "DeleteShard", Tag: rand.Int31(), FromGid: args.Gid}
	//log.Printf("gid %d server %d confignumber %d start cmd %v",
	//	kv.gid, kv.me, kv.configNumber, cmd)
	index, _, isLeader := kv.rf.Start(cmd)
	kv.mu.Unlock()
	//log.Printf("deletion gid %d start in index %d server %d cmd:%v", kv.gid, index, kv.me, cmd)
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
			//log.Printf("shardmigrate res %v", res)
			if res.Operator == "DeleteShard" && res.Tag == cmd.Tag {

				reply.Err = OK

				return
			} else {

				reply.Err = ErrWrongLeader
				return
			}
		} else {
			time.Sleep(100 * time.Millisecond)
		}
	}

	reply.Err = ErrWrongLeader
	return

}

func (kv *ShardKV) deleteShard(shard int, deleteConfigNumber int, servers []string) {
	args := deleteArgs{Shard: shard, DeleteConfigNumber: deleteConfigNumber, Gid: kv.gid}
	for {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply deleteReply
			ok := srv.Call("ShardKV.ShardDeletion", &args, &reply)
			if ok && (reply.Err == OK) {
				return
			}
			// ... not ok, or ErrWrongLeader
			if ok && reply.Err == ErrWrongLeader {
				continue
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func (kv *ShardKV) requestShard1(shard int, currentConfigNumber int) {
	//找出最近的有这个shard的group并向该group请求转移这个shard
	kv.mu.Lock()
	var i int
	var servers []string
	var ok bool
	var oldResponsibleGid int
	for i = currentConfigNumber - 1; i >= 0; i-- {
		oldResponsibleGid = kv.shardConfig[i].Shards[shard]
		//如果上一个负责此shard就是本组，并且shard的configNumber与i相同，那么只需要自更新shard的configNumber
		if oldResponsibleGid == kv.gid {
			if kv.shards[shard] == i {
				cmd := Op{Operator: "InstallShardMigration", Shard: shard, GroupConfigNumber: currentConfigNumber, FromGid: kv.gid}
				kv.rf.Start(cmd)
				//log.Printf("installmigration gid %d start in index %d cmd:%v here2", kv.gid, index, cmd)
				break
			} else {
				continue
			}
		}
		//如果发现以前的config中没有负责这个shard的group，那么也只需要自更新shard的configNumber
		if oldResponsibleGid == 0 {
			cmd := Op{Operator: "InstallShardMigration", Shard: shard, GroupConfigNumber: currentConfigNumber, FromGid: kv.gid}
			kv.rf.Start(cmd)
			//log.Printf("installmigration gid %d start in index %d cmd:%v here3", kv.gid, index, cmd)
			break
		}
		if servers, ok = kv.shardConfig[i].Groups[oldResponsibleGid]; ok {
			break
		}
	}
	kv.mu.Unlock()
	if !ok {
		return
	}
	//log.Printf("gid %d start request shard %d from gid %d requestconf %d currconf %d", kv.gid, shard, oldResponsibleGid, i, currentConfigNumber)
	var args shardArgs
	args.Shard = shard
	args.RequestConfigNumber = i
	args.GroupConfigNumber = currentConfigNumber
	args.Gid = kv.gid
	for {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply shardReply
			ok := srv.Call("ShardKV.ShardMigration", &args, &reply)
			if ok && (reply.Err == OK) {
				cmd := Op{Operator: "InstallShardMigration", ShardDB: reply.ShardDb, Shard: shard, RequestConfigNumber: args.RequestConfigNumber, GroupConfigNumber: currentConfigNumber, SerialDB: reply.SerialDb, FromGid: oldResponsibleGid}
				kv.rf.Start(cmd)

				return
			}
			if ok && (reply.Err == ErrWrongGroup) {
				break
			}
			// ... not ok, or ErrWrongLeader
			if ok && reply.Err == ErrWrongLeader {
				continue
			}
		}
		if _, isLeader := kv.rf.GetState(); !isLeader {
			return
		}
		kv.mu.Lock()
		//如果发现本机的kv.configNumber更新了，那么停止当前的请求进程？
		if args.GroupConfigNumber < kv.configNumber {
			kv.mu.Unlock()
			return
		}

		//如果kv.shards[shard] >= requestConfigNumber,说明已经通过其他的线程请求到了，这时候就可以退出当前请求了
		if kv.shards[shard] >= args.RequestConfigNumber {

			if kv.shards[shard] < currentConfigNumber {
				cmd := Op{Operator: "InstallShardMigration", Shard: shard, GroupConfigNumber: currentConfigNumber, FromGid: kv.gid}
				kv.rf.Start(cmd)
				//log.Printf("installmigration gid %d start in index %d cmd:%v here5,kvshard %d argsreqconf %d currconf %d,oldres %d", kv.gid, index, cmd, kv.shards, args.RequestConfigNumber, currentConfigNumber, oldResponsibleGid)
			}
			kv.mu.Unlock()
			return
		} else if kv.shards[shard] == -args.RequestConfigNumber {
			kv.mu.Unlock()
			return
		}

		kv.mu.Unlock()
		time.Sleep(300 * time.Millisecond)

	}
}
