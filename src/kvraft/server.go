package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operator string
	Key      string
	Value    string
	Cid      int64
	Rid      int
}

type KVServer struct {
	serialMu   sync.Mutex
	logMu      sync.Mutex
	databaseMu sync.Mutex
	me         int
	rf         *raft.Raft
	applyCh    chan raft.ApplyMsg
	dead       int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	database map[string]string
	serial   map[int64]int
	log      map[int]Op

	// for snapshot
	lastAppliedIndex int
	preAppliedIndex  int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	cmd := Op{Operator: "Get", Key: args.Key, Cid: args.Cid}
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
			if res == cmd {
				kv.databaseMu.Lock()
				value, have := kv.database[args.Key]
				kv.databaseMu.Unlock()
				if have {
					reply.Value = value
					reply.Err = OK
					return
				} else {
					reply.Err = ErrNoKey
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

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.serialMu.Lock()
	res, ok := kv.serial[args.Cid]
	kv.serialMu.Unlock()
	if ok {
		if res >= args.Rid {
			reply.Err = OK
			return
		}
	}

	cmd := Op{Operator: args.Op, Key: args.Key, Value: args.Value, Cid: args.Cid, Rid: args.Rid}

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
			if res == cmd {
				reply.Err = OK
				return
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
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	//log.Printf("kvserver start")
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.database = make(map[string]string)
	kv.serial = make(map[int64]int)
	kv.installSnapshot(persister.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.log = make(map[int]Op)
	kv.lastAppliedIndex = 0
	kv.preAppliedIndex = 0

	go func() {
		for {

			applyMsg := <-kv.applyCh
			// log.Printf("applyMsg:%v", applyMsg)
			if applyMsg.CommandValid {
				cmd := applyMsg.Command.(Op)
				if cmd.Operator != "Get" {
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

						//log.Printf("update put:key:%v, value:%v", cmd.Key, cmd.Value)
					} else if cmd.Operator == "Append" {

						res, ok := kv.database[cmd.Key]
						if ok {
							res = res + cmd.Value
							kv.database[cmd.Key] = res
						} else {
							kv.database[cmd.Key] = cmd.Value
						}

						//log.Printf("update append:key:%v, value:%v", cmd.Key, res)
					}
					kv.lastAppliedIndex = applyMsg.CommandIndex
					kv.databaseMu.Unlock()
					kv.serialMu.Unlock()
				}

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
func (kv *KVServer) takeSnapshot(persister *raft.Persister) {
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

func (kv *KVServer) installSnapshot(snapshot []byte) {
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
