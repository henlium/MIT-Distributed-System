package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
)

type TaskState int

const (
	Unassigned TaskState = iota
	Assigned
	Finished
)

type Coordinator struct {
	m int // size of map tasks
	r int // size of reduce tasks

	mapState    sync.Map
	reduceState sync.Map

	finishedM int64
	finishedR int64

	mTasks []Task
}

// get the size of reduce tasks for workers
func (c *Coordinator) GetR(_ *Empty, r *SingleInt) error {
	r.Value = c.r
	return nil
}

// get a task for workers
func (c *Coordinator) GetTask(_ *Empty, task *Task) error {
	if int(atomic.LoadInt64(&c.finishedM)) < c.m {
		*task = *c.getUnassignedM()
		return nil
	} else if int(atomic.LoadInt64(&c.finishedR)) < c.r {
		*task = c.getUnassignedR()
		return nil
	}
	task.Type = NoTask
	return nil
}

func (c *Coordinator) getUnassignedM() (t *Task) {
	c.mapState.Range(func(num, state interface{}) bool {
		if state != Unassigned {
			return true
		}
		i, _ := num.(int)
		if i >= len(c.mTasks) {
			panic(fmt.Sprintf("Files index out of range: %v, %v", i, len(c.mTasks)))
		}
		t = &c.mTasks[i]
		return false
	})
	return
}

func (c *Coordinator) getUnassignedR() (t Task) {
	c.reduceState.Range(func(num, state interface{}) bool {
		if state != Unassigned {
			return true
		}
		t.Number, _ = num.(int)
		t.Type = ReduceTask
		return false
	})
	return
}

func (c *Coordinator) FinishTask(task *Task, _ *Empty) error {
	if task.Type == MapTask {
		curState, _ := c.mapState.Load(task.Number)
		if curState == Finished {
			return nil
		}
		println("M done", task.Number, "Remaining", atomic.LoadInt64(&c.finishedM))
		c.mapState.Store(task.Number, Finished)
		atomic.AddInt64(&c.finishedM, 1)
	}
	if task.Type == ReduceTask {
		curState, _ := c.reduceState.Load(task.Number)
		if curState == Finished {
			return nil
		}
		println("R done", task.Number)
		c.reduceState.Store(task.Number, Finished)
		atomic.AddInt64(&c.finishedR, 1)
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// println("finished R:", atomic.LoadInt64(&c.finishedR))
	return int(atomic.LoadInt64(&c.finishedR)) == c.r
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{m: len(files), r: nReduce}
	c.mTasks = make([]Task, 0, len(files))
	for i, f := range files {
		c.mTasks = append(c.mTasks, Task{Type: MapTask, Number: i, Input: f})
	}

	for i := 0; i < c.m; i++ {
		c.mapState.Store(i, Unassigned)
	}
	for i := 0; i < c.r; i++ {
		c.reduceState.Store(i, Unassigned)
	}

	c.server()
	return &c
}
