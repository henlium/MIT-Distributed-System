package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	w := newWorker(mapf, reducef)
	w.main()
}

type worker struct {
	r int // size of reduce tasks

	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func newWorker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) *worker {
	w := worker{}
	w.r = getR()
	w.mapf = mapf
	w.reducef = reducef
	return &w
}

func (w *worker) main() {
	println("worker started")
	for {
		t := getTask()
		if t.Type == NoTask {
			break
		}
		if t.Type == MapTask {
			w.doMap(t)
		}
		if t.Type == ReduceTask {
			w.doReduce(t)
		}
		finishTask(t)
	}
}

func getR() int {
	reply := SingleInt{}
	if !call("Coordinator.GetR", &Empty{}, &reply) {
		log.Fatal("GetR failed")
	}
	return reply.Value
}

func getTask() Task {
	t := Task{}
	if !call("Coordinator.GetTask", &Empty{}, &t) {
		log.Fatal("GetTask failed")
	}
	return t
}

func (w *worker) doMap(task Task) {
	filename := task.Input
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatal(err)
	}
	kvs := (w.mapf)(filename, string(content))
	var iks [][]KeyValue // a slice of intermediate keys
	for i := 0; i < w.r; i++ {
		iks = append(iks, []KeyValue{})
	}
	for _, kv := range kvs {
		r := ihash(kv.Key) % w.r
		iks[r] = append(iks[r], kv)
	}
	// TODO: Extract writing to a func
	for i, ik := range iks {
		sort.Slice(ik, func(i, j int) bool { return ik[i].Key < ik[j].Key })
		file, _ := ioutil.TempFile("", "mr")
		enc := json.NewEncoder(file)
		for _, kv := range ik {
			enc.Encode(kv)
		}
		file.Close()
		os.Rename(file.Name(), fmt.Sprintf("mr-%v-%v", task.Number, i))
	}
	finishTask(task)
}

func (w *worker) doReduce(task Task) {
	kvs := map[string][]string{}
	for i := 0; i < w.r; i++ {
		filename := fmt.Sprintf("mr-%v-%v", i, task.Number)
		file, _ := os.Open(filename)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
	}
	file, _ := ioutil.TempFile("", "mr-tmp")
	for key, values := range kvs {
		result := w.reducef(key, values)
		fmt.Fprintf(file, "%v %v\n", key, result)
	}
	file.Close()
	os.Rename(file.Name(), fmt.Sprintf("mr-out-%v", task.Number))
}

func finishTask(t Task) {
	call("Coordinator.FinishTask", &t, &Empty{})
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
