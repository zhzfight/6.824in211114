package kvraft

import (
	"../labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	Rid    int
	Cid    int64
	leader int
	mu     sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.Rid = 0
	ck.Cid = nrand()
	ck.leader = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{Key: key, Cid: ck.Cid}
	reply := GetReply{}
	for {
		for i := 0; i < len(ck.servers); i++ {
			if ck.sendGet((i+ck.leader)%len(ck.servers), &args, &reply) {
				if reply.Err == OK {
					ck.leader = i
					return reply.Value
				} else if reply.Err == ErrWrongLeader {
					continue
				} else {
					return ""
				}
			}
		}
		time.Sleep(PollingTimeInterval)
	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	ck.mu.Lock()
	ck.Rid++
	args := PutAppendArgs{Key: key, Value: value, Op: op, Rid: ck.Rid, Cid: ck.Cid}
	ck.mu.Unlock()
	reply := PutAppendReply{}
	for {
		for i := 0; i < len(ck.servers); i++ {
			if ck.sendPutAppend((i+ck.leader)%len(ck.servers), &args, &reply) {
				if reply.Err == OK {
					ck.leader = i
					return
				} else {
					continue
				}
			}
		}
		time.Sleep(PollingTimeInterval)
	}
}
func (ck *Clerk) sendGet(server int, args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[server].Call("KVServer.Get", args, reply)
	return ok
}

func (ck *Clerk) sendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
	return ok
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
