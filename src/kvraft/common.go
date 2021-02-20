package kvraft

import "time"

const (
	OK                  = "OK"
	ErrNoKey            = "ErrNoKey"
	ErrWrongLeader      = "ErrWrongLeader"
	Retry               = "Retry"
	PollingTimeInterval = 200 * time.Millisecond
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Rid int
	Cid int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Rid int
	Cid int64
}

type GetReply struct {
	Err   Err
	Value string
}
