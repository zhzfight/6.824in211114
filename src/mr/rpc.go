package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type Err string

const (
	NoJob = "NoJob"
	Ok    = "Ok"
)

type TaskArgs struct {
	ToWorker   chan Command
	FromWorker chan Res
}

type TaskReply struct {
	TP      TaskType
	MN      int
	RN      int
	FN      string
	FNs     []string
	NReduce int
	Err     Err
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
