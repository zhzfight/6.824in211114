package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"sync"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	taskState := Idle
	var receiver chan Command
	var sender chan Res
	receiver = make(chan Command)
	sender = make(chan Res)
	var mu sync.Mutex

	for {
		mu.Lock()
		taskState = Idle
		mu.Unlock()
		task := askForTask(receiver, sender)
		if task.TP == None {
			return
		} else if task.TP == Wait {

		} else {
			go func() {
				for {
					select {
					case cmd := <-receiver:
						if cmd == Query {
							sender <- Res(taskState)
						} else if cmd == Stop {
							return
						}
					}
				}
			}()
			mu.Lock()
			taskState = Positive
			mu.Unlock()
			if task.TP == Map {
				intermediate := []KeyValue{}
				file, err := os.Open(task.FNs[0])
				if err != nil {
					log.Fatalf("cannot open %v", task.FNs)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", task.FNs)
				}
				file.Close()
				kva := mapf(task.FNs[0], string(content))
				intermediate = append(intermediate, kva...)

				sort.Sort(ByKey(intermediate))

				encs := make([]*json.Encoder, task.NReduce)
				for i := 0; i < task.NReduce; i++ {
					oname := "mr-" + string(task.MN) + "-" + string(i)
					intermediateFile, err := os.Create(oname)
					if err != nil {
						log.Fatalf("cannot create intermediate file %v", oname)
					}
					encs[i] = json.NewEncoder(intermediateFile)
					defer intermediateFile.Close()
				}

				for _, kv := range intermediate {
					reducePart := ihash(kv.Key)
					err := encs[reducePart].Encode(&kv)
					if err != nil {
						log.Fatalf("cannot encode intermediate file")
					}
				}

				mu.Lock()
				taskState = Completed
				mu.Unlock()
			} else {
				mu.Lock()
				taskState = Positive
				mu.Unlock()
				oname := "mr-out-" + string(task.RN)
				ofile, err := os.Create(oname)
				if err != nil {
					log.Fatalf("cannot create outputfile %v", oname)
				}
				var kva []KeyValue
				kva = make([]KeyValue, 0)

				for i := 0; i < 8; i++ {
					iFileName := "mr-" + string(i) + "-" + string(task.RN)
					file, err := os.Open(iFileName)
					if err != nil {
						log.Fatalf("cannot open intermediate file %v", iFileName)
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv)
					}
				}
				i := 0
				for i < len(kva) {
					j := i + 1
					for j < len(kva) && kva[j].Key == kva[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, kva[k].Value)
					}
					output := reducef(kva[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

					i = j
				}
				mu.Lock()
				taskState = Completed
				mu.Unlock()
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func askForTask(receiver chan Command, sender chan Res) Task {
	args := TaskArgs{toWorker: receiver, fromWorker: sender}
	reply := TaskReply{}
	// send the RPC request, wait for the reply.
	call("Master.AssignJob", &args, &reply)
	if reply.Err == NoJob {
		task := Task{TP: None}
		return task
	} else {
		if reply.TP == Map {
			task := Task{TP: Map, MN: reply.MN, FN: reply.FN, NReduce: reply.NReduce}
			return task
		} else {
			task := Task{TP: Reduce, RN: reply.RN, FNs: reply.FNs, NReduce: reply.NReduce}
			return task
		}
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
