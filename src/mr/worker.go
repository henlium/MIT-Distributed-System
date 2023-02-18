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
	"time"
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
		if t.Type == WaitForTask {
			time.Sleep(time.Second)
			continue
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
		writeJsons(ik, fmt.Sprintf("mr-%v-%v", task.Number, i))
	}
	finishTask(task)
}

func writeJsons(kvs []KeyValue, filename string) {
	file, _ := ioutil.TempFile("", "")
	enc := json.NewEncoder(file)
	for _, kv := range kvs {
		enc.Encode(kv)
	}
	file.Close()
	os.Rename(file.Name(), filename)
}

func (w *worker) doReduce(task Task) {
	kvs := map[string][]string{}
	for i := 0; i < w.r; i++ {
		filename := fmt.Sprintf("mr-%v-%v", i, task.Number)
		loadIK(filename, kvs)
	}
	var key2result []KeyValue
	for key, values := range kvs {
		result := w.reducef(key, values)
		key2result = append(key2result, KeyValue{key, result})
	}
	filename := fmt.Sprintf("mr-out-%v", task.Number)
	writeTSV(key2result, filename)
}

// Load a file of intermediate key to a "key to values" slice
func loadIK(filename string, kvs map[string][]string) {
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

func writeTSV(keyValues []KeyValue, filename string) {
	file, _ := ioutil.TempFile("", "mr-tmp")
	for _, kv := range keyValues {
		fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
	}
	file.Close()
	os.Rename(file.Name(), filename)
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
