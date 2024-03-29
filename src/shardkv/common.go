package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each Shard.
// Shardmaster may change Shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	poll           = 100
	WaitResult     = 2000
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cid          int64
	Rid          int
	ConfigNumber int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Cid          int64
	Rid          int
	ConfigNumber int
}

type GetReply struct {
	Err   Err
	Value string
}

type shardArgs struct {
	Gid                 int
	Server              int
	Shard               int
	RequestConfigNumber int
	GroupConfigNumber   int
}

type shardReply struct {
	ConfigNumber int
	ShardDb      map[string]string
	SerialDb     map[int64]int
	Err          Err
}

type deleteArgs struct {
	Gid                int
	Shard              int
	DeleteConfigNumber int
}
type deleteReply struct {
	Err Err
}
