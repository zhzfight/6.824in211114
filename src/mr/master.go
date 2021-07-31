package mr

import (
	"io/ioutil"
	"log"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	M             int
	R             int
	MTs           Tasks
	RTs           Tasks
	MD            bool
	RD            bool
	MChannelsTo   []chan Command
	MChannelsFrom []chan Res
	RChannelsTo   []chan Command
	RChannelsFrom []chan Res
	mu            sync.Mutex
}

const (
	Map       = "Map"
	Reduce    = " Reduce"
	Idle      = "Idle"
	Positive  = "Positive"
	Completed = "Completed"
	Query     = "Query"
	Stop      = "Stop"
)

type State string
type TaskType string
type Command string
type Res string
type Task struct {
	TP TaskType
	MN int
	RN int
	FN string
	S  State
}
type Tasks []Task

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) AssignJob(args *TaskArgs, reply *TaskReply) error {
	m.mu.Lock()
	if !m.MD {
		for i := 0; i < m.M; i++ {
			if m.MTs[i].S == Idle {
				m.MChannelsTo[i] = args.toWorker
				m.MChannelsFrom[i] = args.fromWorker
				reply.TP = Map
				reply.MN = m.MTs[i].MN
				reply.FN = m.MTs[i].FN
				reply.Err = Ok
				go m.monitor(Map, i)
				break
			}
		}
	} else if !m.RD {
		for i := 0; i < m.R; i++ {
			if m.MTs[i].S == Idle {
				m.RChannelsTo[i] = args.toWorker
				m.MChannelsFrom[i] = args.fromWorker
				reply.TP = Reduce
				reply.RN = m.MTs[i].RN
				reply.FN = m.MTs[i].FN
				reply.Err = Ok
				go m.monitor(Reduce, i)
				break
			}
		}
	} else {
		reply.Err = NoJob
	}
	m.mu.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	ret := m.MD && m.RD
	m.mu.Unlock()
	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	m.M = 8
	m.R = nReduce
	m.MTs = make([]Task, 0)
	m.RTs = make([]Task, 0)
	m.MChannelsTo = make([]chan Command, m.M)
	m.RChannelsTo = make([]chan Command, m.R)

	for i := 0; i < m.M; i++ {
		fp, err := os.Create("./Input-" + string(i))
		if err != nil {
			log.Fatalf("cannot create file %v", fp)
		}
		fp.Close()
	}
	var count int
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		lines := strings.Split(string(content), "\n")
		for _, line := range lines {
			err := ioutil.WriteFile("./Input"+string(count%m.M), []byte(line), 0644)
			if err != nil {
				log.Fatalf("cannot write %v", "./Input-"+string(count%m.M))
			}
			count++
		}
	}

	for i := 0; i < m.M; i++ {
		task := Task{TP: Map, MN: i, FN: "Input-" + string(i), S: Idle}
		m.MTs = append(m.MTs, task)
	}

	for i := 0; i < m.R; i++ {
		task := Task{TP: Reduce, RN: i, S: Idle}
		m.RTs = append(m.RTs, task)
	}

	m.server()
	return &m
}

func (m *Master) monitor(taskType TaskType, taskNum int) {
	t := time.Now()
	timer := new(time.Timer)
	var channelTo chan Command
	var channelFrom chan Res
	if taskType == Map {
		channelTo = m.MChannelsTo[taskNum]
		channelFrom = m.MChannelsFrom[taskNum]
	} else {
		channelTo = m.RChannelsTo[taskNum]
		channelFrom = m.RChannelsFrom[taskNum]
	}
	for time.Since(t).Seconds() < 10 {
		//在这10s内，以200毫秒为周期ping worker进程
		select {
		case <-timer.C:
		//检查有无回复
		case res := <-channelFrom:

		}
	}
	//如果10s内完不成这个任务，则将这个任务回收
	channelTo = nil
	if taskType == Map {
		m.MChannelsTo[taskNum] = nil
		m.MChannelsFrom[taskNum] = nil
		m.MTs[taskNum].S = Idle
	} else {
		m.RChannelsTo[taskNum] = nil
		m.RChannelsFrom[taskNum] = nil
		m.RTs[taskNum].S = Idle
	}
}
