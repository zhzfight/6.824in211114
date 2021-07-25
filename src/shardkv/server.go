package shardkv

import (
	"../shardmaster"
	"bytes"
	"log"
	"sync/atomic"
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
	Cid          int64
	Rid          int
	Operator     string
	Key          string
	Value        string
	Config       shardmaster.Config
	ShardDB      map[string]string
	Shard        int
	ConfigNumber int
	Gid          int
	SerialDB     map[int64]int
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
	shards           []int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	thisShardConfigNumber := kv.shards[key2shard(args.Key)]
	if thisShardConfigNumber != args.ConfigNumber || args.ConfigNumber != kv.configNumber {
		log.Printf("here shard error")
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	cmd := Op{Key: args.Key, Cid: args.Cid, Rid: args.Rid, Operator: "Get", ConfigNumber: kv.configNumber}
	kv.mu.Unlock()

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
					log.Printf("here config error")
					reply.Err = ErrWrongGroup
					return
				}
			} else {
				reply.Err = ErrWrongLeader
				return
			}
		} else {
			time.Sleep(200 * time.Millisecond)
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

	thisShardConfigNumber := kv.shards[key2shard(args.Key)]

	if thisShardConfigNumber != args.ConfigNumber || args.ConfigNumber != kv.configNumber {
		log.Printf("here shard error")
		reply.Err = ErrWrongGroup
		log.Printf("client %d rid %d gid %d put shard %d value %s thisshard %d args.conf %d reply %v", args.Cid, args.Rid, kv.gid, key2shard(args.Key), args.Value, thisShardConfigNumber, args.ConfigNumber, reply)
		kv.mu.Unlock()
		return
	}
	res, ok := kv.serial[args.Cid]
	if ok {
		if res >= args.Rid {
			reply.Err = OK
			log.Printf("client %d rid %d higerserial gid %d put shard %d value %s thisshard %d args.conf %d reply %v", args.Cid, args.Rid, kv.gid, key2shard(args.Key), args.Value, thisShardConfigNumber, args.ConfigNumber, reply)
			kv.mu.Unlock()
			return
		}
	}
	cmd := Op{Operator: args.Op, Key: args.Key, Value: args.Value, Cid: args.Cid, Rid: args.Rid, ConfigNumber: kv.configNumber}
	kv.mu.Unlock()
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
					log.Printf("client %d rid %d gid %d put shard %d value %s thisshard %d args.conf %d reply %v", args.Cid, args.Rid, kv.gid, key2shard(args.Key), args.Value, thisShardConfigNumber, args.ConfigNumber, reply)
					return
				} else {
					reply.Err = ErrWrongGroup
					log.Printf("client %d rid %d gid %d put shard %d value %s thisshard %d args.conf %d reply %v", args.Cid, args.Rid, kv.gid, key2shard(args.Key), args.Value, thisShardConfigNumber, args.ConfigNumber, reply)
					return
				}
			} else {
				reply.Err = ErrWrongLeader
				log.Printf("client %d rid %d gid %d put shard %d value %s thisshard %d args.conf %d reply %v", args.Cid, args.Rid, kv.gid, key2shard(args.Key), args.Value, thisShardConfigNumber, args.ConfigNumber, reply)
				return
			}
		} else {
			time.Sleep(200 * time.Millisecond)
		}
	}
	reply.Err = ErrWrongLeader
	log.Printf("client %d rid %d gid %d put shard %d value %s thisshard %d args.conf %d reply %v", args.Cid, args.Rid, kv.gid, key2shard(args.Key), args.Value, thisShardConfigNumber, args.ConfigNumber, reply)
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
						log.Printf("gid %d cmd %v shard %d cmd.num %d kv.num %d shardnum %d", kv.gid, cmd, key2shard(cmd.Key), cmd.ConfigNumber, kv.configNumber, kv.shards[key2shard(cmd.Key)])
					} else {
						log.Printf("gid %d cmd %v cmd.num %d kv.num %d shardnum %d", kv.gid, cmd, cmd.ConfigNumber, kv.configNumber, kv.shards[key2shard(cmd.Key)])
					}
				}
				if cmd.Operator == "Put" || cmd.Operator == "Append" {
					if cmd.ConfigNumber == kv.configNumber {
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
					}

				} else if cmd.Operator == "Get" {
					if cmd.ConfigNumber == kv.configNumber {
						kv.databaseMu.Lock()
						value, have := kv.database[cmd.Key]
						kv.databaseMu.Unlock()
						if have {
							cmd.Value = value
						}
					}

				} else if cmd.Operator == "ConfigChange" {

					if cmd.Config.Num > kv.configNumber {

						newShards := make(map[int]struct{})
						for shard, gid := range cmd.Config.Shards {
							if gid == kv.gid {
								newShards[shard] = struct{}{}
							}
						}
						log.Printf("gid %d newshards %v", kv.gid, newShards)
						log.Printf("gid %d oldshards %v", kv.gid, kv.shards)
						newResponsibleShard := make(map[int]struct{})
						for k, _ := range newShards {
							if kv.shards[k] == kv.configNumber {
								//改成cmd更新如何？
								cmd := Op{Operator: "InstallShardMigration", Shard: k, ConfigNumber: cmd.Config.Num, Gid: kv.gid}
								kv.rf.Start(cmd)
								//kv.shards[k] = cmd.Config.Num
							} else {
								newResponsibleShard[k] = struct{}{}
							}
						}
						log.Printf("gid %d update shard %v", kv.gid, kv.shards)
						log.Printf("gid %d newresponsible %v in confignumber %d", kv.gid, newResponsibleShard, cmd.Config.Num)
						if _, isLeader := kv.rf.GetState(); isLeader {
							if len(newResponsibleShard) > 0 {
								for shard, _ := range newResponsibleShard {
									for i := cmd.Config.Num - 1; i >= 0; i-- {
										log.Printf("gid %d %v", kv.gid, kv.shardConfig)
										oldResponsibleGid := kv.shardConfig[i].Shards[shard]
										log.Printf("gid %d shard %d oldresponsible %d in confignumber %d", kv.gid, shard, oldResponsibleGid, cmd.Config.Num)
										if oldResponsibleGid == kv.gid {
											if kv.shards[shard] == i {
												cmd := Op{Operator: "InstallShardMigration", Shard: shard, ConfigNumber: cmd.Config.Num, Gid: kv.gid}
												kv.rf.Start(cmd)
												break
											} else {
												continue
											}
										}
										if oldResponsibleGid == 0 {
											cmd := Op{Operator: "InstallShardMigration", Shard: shard, ConfigNumber: cmd.Config.Num, Gid: kv.gid}
											kv.rf.Start(cmd)
											break
										}
										if servers, ok := kv.shardConfig[i].Groups[oldResponsibleGid]; ok {
											go kv.requestShard(shard, servers, oldResponsibleGid, i, cmd.Config.Num)
											break
										}
									}

								}
							}

						}

						if _, isLeader := kv.rf.GetState(); isLeader {
							log.Printf("gid %d shards %v", kv.gid, kv.shards)
						}
						kv.shardConfig = append(kv.shardConfig, cmd.Config)
						kv.configNumber = cmd.Config.Num

					}
				} else if cmd.Operator == "ShardMigration" {

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
						log.Printf("gid %d migrate shard %d to gid %d confignumber %d", kv.gid, cmd.Shard, cmd.Gid, cmd.ConfigNumber)
					}
					kv.databaseMu.Unlock()

				} else if cmd.Operator == "InstallShardMigration" {

					if kv.shards[cmd.Shard] < cmd.ConfigNumber {
						if cmd.ShardDB != nil {
							kv.databaseMu.Lock()
							for k, v := range cmd.ShardDB {
								kv.database[k] = v
							}
							kv.databaseMu.Unlock()
						}
						kv.shards[cmd.Shard] = cmd.ConfigNumber
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
						log.Printf("gid %d install shard %d:in config %d now shards %v serial %v db %v", kv.gid, cmd.Shard, cmd.ConfigNumber, kv.shards, kv.serial, kv.database)
						if cmd.Gid != 0 && cmd.Gid != kv.gid {
							if servers, ok := kv.shardConfig[cmd.ConfigNumber].Groups[cmd.Gid]; ok {
								log.Printf("gid %d start delete gid %d shard %d in conf %d nowshard %v", kv.gid, cmd.Gid, cmd.Shard, cmd.ConfigNumber, kv.shards)
								go kv.deleteShard(cmd.Shard, cmd.ConfigNumber, cmd.Gid, servers)
							}
						}
					}
					kv.databaseMu.Unlock()

				} else if cmd.Operator == "DeleteShard" {
					kv.databaseMu.Lock()
					for k, _ := range kv.database {
						if key2shard(k) == cmd.Shard {
							delete(kv.database, k)
						}
					}
					kv.databaseMu.Unlock()
					kv.shards[cmd.Shard] = cmd.ConfigNumber
					log.Printf("gid %d nowshard %v", kv.gid, kv.shards)
				}

				cmd.ConfigNumber = kv.configNumber
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
			//log.Printf("takesnapshot")
			kv.mu.Lock()
			kv.databaseMu.Lock()
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.database)
			e.Encode(kv.serial)
			e.Encode(kv.shards)
			e.Encode(kv.shardConfig)
			lastAppliedIndex := kv.lastAppliedIndex
			kv.databaseMu.Unlock()
			kv.mu.Unlock()
			data := w.Bytes()
			//log.Printf("server%d:snapshot trimming %d",kv.me,lastAppliedIndex)
			kv.rf.TrimmingLogWithSnapshot(lastAppliedIndex, data)

			// wait for start(cmd) timeout
			time.Sleep(200 * time.Millisecond)

			// clean up the kv.log
			kv.logMu.Lock()
			for i := kv.preAppliedIndex + 1; i <= lastAppliedIndex; i++ {
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
		kv.mu.Lock()
		kv.databaseMu.Lock()
		kv.database = database
		kv.serial = serial
		kv.shards = shards
		log.Printf("gid %d installsnapshotshard shard %v db %v serial %v", kv.gid, shards, kv.database, kv.serial)
		kv.shardConfig = configs
		if len(configs) != 0 && len(configs) != 1 {
			shouldShard := make([]int, shardmaster.NShards)
			for i := 0; i < shardmaster.NShards; i++ {
				for j := len(configs) - 1; j >= 0; j-- {
					if configs[j].Shards[i] == kv.gid {
						shouldShard[i] = j
						break
					}
				}
			}
			if _, isLeader := kv.rf.GetState(); isLeader {
				log.Printf("gid %d nowshard %v shouldshard %v", kv.gid, shards, shouldShard)
			}
			for shard := 0; shard < shardmaster.NShards; shard++ {
				if shouldShard[shard] > shards[shard] {
					for j := len(configs) - 1; j >= 0; j-- {
						oldResponsibleGid := configs[j].Shards[shard]
						if oldResponsibleGid == kv.gid {
							if shards[shard] == j {
								cmd := Op{Operator: "InstallShardMigration", Shard: shard, ConfigNumber: configs[len(configs)-1].Num, Gid: kv.gid}
								kv.rf.Start(cmd)
								break
							} else {
								continue
							}
						}
						if oldResponsibleGid == 0 {
							cmd := Op{Operator: "InstallShardMigration", Shard: shard, ConfigNumber: configs[len(configs)-1].Num, Gid: kv.gid}
							kv.rf.Start(cmd)
							break
						}
						if servers, ok := kv.shardConfig[j].Groups[oldResponsibleGid]; ok {
							go kv.requestShard(shard, servers, oldResponsibleGid, j, configs[len(configs)-1].Num)
							break
						}
					}
				}
			}
		}
		kv.databaseMu.Unlock()
		kv.mu.Unlock()
		log.Printf("gid %d server %d install snapshot %v", kv.gid, kv.me, kv.shardConfig)
	}
}

func (kv *ShardKV) ShardMigration(args *shardArgs, reply *shardReply) {

	defer log.Printf("gid %d shardmigrate shard %d to gid %d reply %v", kv.gid, args.Shard, args.Gid, reply)
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	//log.Printf("gid %d server %d confignumber %d get shardmigration request from gid %d server %d shard %d confignumber %d",
	//	kv.gid, kv.me, kv.configNumber, args.Gid, args.Server, args.Shard, args.RequestConfigNumber)
	if args.RequestConfigNumber != kv.shards[args.Shard] || args.GroupConfigNumber != kv.configNumber {

		reply.Err = ErrWrongGroup
		log.Printf("gid %d reply to gid %d reply %v args.requestnumber %d kv.theshardnumber %d args.cnumber %d kv.connum %d", kv.gid, args.Gid, reply, args.RequestConfigNumber, kv.shards[args.Shard], args.GroupConfigNumber, kv.configNumber)
		kv.mu.Unlock()
		return
	}
	cmd := Op{Shard: args.Shard, ConfigNumber: args.RequestConfigNumber, Operator: "ShardMigration", Gid: args.Gid}
	//log.Printf("gid %d server %d confignumber %d start cmd %v",
	//	kv.gid, kv.me, kv.configNumber, cmd)
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	t := time.Now()
	for time.Since(t).Milliseconds() < WaitResult {
		kv.logMu.Lock()
		res, ok := kv.log[index]
		kv.logMu.Unlock()
		if ok {
			//log.Printf("shardmigrate res %v", res)
			if res.Operator == "ShardMigration" && res.Shard == cmd.Shard && res.Gid == cmd.Gid {
				reply.ShardDb = make(map[string]string)
				reply.SerialDb = make(map[int64]int)
				for k, v := range res.ShardDB {
					reply.ShardDb[k] = v
				}
				for k, v := range res.SerialDB {
					reply.SerialDb[k] = v
				}
				reply.Err = OK
				log.Printf("gid %d from gid %d successfully get shardmigration %d", cmd.Gid, kv.gid, args.Shard)
				return
			} else {
				log.Printf("gid %d from gid %d failly get shardmigration %d because res.shard %d cmd.shard %d res.gid %d cmd.gid %d", cmd.Gid, kv.gid, args.Shard, res.Shard, cmd.Shard, res.Gid, cmd.Gid)
				reply.Err = ErrWrongLeader
				return
			}
		} else {
			time.Sleep(200 * time.Millisecond)
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
				log.Printf("gid %d query %d and get latestConfig %v nowshard %v", kv.gid, kv.configNumber+1, latestConfig, kv.shards)
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
			var reply shardReply
			ok := srv.Call("ShardKV.ShardMigration", &args, &reply)
			if ok && (reply.Err == OK) {
				cmd := Op{Operator: "InstallShardMigration", ShardDB: reply.ShardDb, Shard: shard, ConfigNumber: currentConfigNumber, SerialDB: reply.SerialDb, Gid: oldgid}
				log.Printf("installshardmigration gid %d start cmd %v request", kv.gid, cmd)
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
		kv.mu.Lock()
		//如果kv.shards[shard] >= requestConfigNumber,说明已经通过其他的线程请求到了，这时候就可以退出当前请求了
		if kv.shards[shard] >= requestConfigNumber {
			if kv.shards[shard] < currentConfigNumber {
				cmd := Op{Operator: "InstallShardMigration", ConfigNumber: currentConfigNumber, Gid: kv.gid}
				kv.rf.Start(cmd)
			}
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		time.Sleep(poll * time.Millisecond)

	}
}
func (kv *ShardKV) ShardDeletion(args *deleteArgs, reply *deleteReply) {
	defer log.Printf("gid %d delete shard %d by gid %d reply %v", kv.gid, args.Shard, args.Gid, reply)
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
	cmd := Op{Shard: args.Shard, ConfigNumber: args.DeleteConfigNumber, Operator: "DeleteShard", Gid: args.Gid}
	//log.Printf("gid %d server %d confignumber %d start cmd %v",
	//	kv.gid, kv.me, kv.configNumber, cmd)
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	t := time.Now()
	for time.Since(t).Milliseconds() < WaitResult {
		kv.logMu.Lock()
		res, ok := kv.log[index]
		kv.logMu.Unlock()
		if ok {
			//log.Printf("shardmigrate res %v", res)
			if res.Operator == "DeleteShard" && res.Shard == cmd.Shard && res.Gid == cmd.Gid {

				reply.Err = OK
				log.Printf("gid %d delete gid %d successfully shard %d", cmd.Gid, kv.gid, args.Shard)
				return
			} else {
				log.Printf("gid %d delete gid %d failly shard %d because res.shard %d cmd.shard %d res.gid %d cmd.gid %d", cmd.Gid, kv.gid, args.Shard, res.Shard, cmd.Shard, res.Gid, cmd.Gid)
				reply.Err = ErrWrongLeader
				return
			}
		} else {
			time.Sleep(200 * time.Millisecond)
		}
	}

	reply.Err = ErrWrongLeader
	return

}

func (kv *ShardKV) deleteShard(shard int, configNumber int, gid int, servers []string) {
	args := deleteArgs{Shard: shard, DeleteConfigNumber: configNumber, Gid: gid}
	for {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply deleteReply
			ok := srv.Call("ShardKV.ShardDeletion", &args, &reply)
			if ok && (reply.Err == OK) {
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

		time.Sleep(poll * time.Millisecond)
	}
}
