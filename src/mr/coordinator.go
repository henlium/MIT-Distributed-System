package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type TaskType int

const (
	WaitForTask TaskType = iota
	NoTask
	MapTask
	ReduceTask
)

type Task struct {
	Type   TaskType
	Number int    // the id of a m or r task
	Input  string // the filename/path of the input file
}

type TaskState int

const (
	Unassigned TaskState = iota
	Assigned
	Finished
)

type taskRecord struct {
	state TaskState
	start time.Time
}

type Coordinator struct {
	m int // size of map tasks
	r int // size of reduce tasks

	assignLock sync.Mutex

	mapState    sync.Map
	reduceState sync.Map

	// TODO: The following atomic counters somehow introduce socket connection errors

	finishedM atomic.Int64
	finishedR atomic.Int64

	mTasks []Task
}

// get the size of reduce tasks for workers
func (c *Coordinator) GetR(_ *Empty, r *SingleInt) error {
	r.Value = c.r
	return nil
}

// get a task for workers
func (c *Coordinator) GetTask(_ *Empty, task *Task) error {
	if int(c.finishedM.Load()) < c.m {
		*task = c.getUnassignedM()
		return nil
	} else if int(c.finishedM.Load()) < c.r {
		*task = c.getUnassignedR()
		return nil
	}
	task.Type = NoTask
	return nil
}

func (c *Coordinator) getUnassignedM() (t Task) {
	c.assignLock.Lock()
	defer c.assignLock.Unlock()
	c.mapState.Range(func(num, r interface{}) bool {
		record := r.(taskRecord)
		if record.state != Unassigned {
			return true
		}
		i, _ := num.(int)
		t = c.mTasks[i]
		return false
	})
	if t.Type != MapTask {
		return
	}
	c.mapState.Store(t.Number, taskRecord{Assigned, time.Now()})
	return
}

func (c *Coordinator) getUnassignedR() (t Task) {
	c.assignLock.Lock()
	defer c.assignLock.Unlock()
	c.reduceState.Range(func(num, r interface{}) bool {
		record := r.(taskRecord)
		if record.state != Unassigned {
			return true
		}
		t.Number, _ = num.(int)
		t.Type = ReduceTask
		return false
	})
	if t.Type != ReduceTask {
		return
	}
	c.reduceState.Store(t.Number, taskRecord{Assigned, time.Now()})
	return
}

func (c *Coordinator) FinishTask(task *Task, _ *Empty) error {
	if task.Type == MapTask {
		val, _ := c.mapState.Load(task.Number)
		tr := val.(taskRecord)
		if tr.state == Finished {
			return nil
		}
		c.mapState.Store(task.Number, taskRecord{state: Finished})
		c.finishedM.Add(1)
	}
	if task.Type == ReduceTask {
		val, _ := c.reduceState.Load(task.Number)
		tr := val.(taskRecord)
		if tr.state == Finished {
			return nil
		}
		c.reduceState.Store(task.Number, taskRecord{state: Finished})
		c.finishedR.Add(1)
	}
	return nil
}

func (c *Coordinator) checkStragglers() {
	for {
		time.Sleep(5 * time.Second)
		c.mapState.Range(func(num, r interface{}) bool {
			record := r.(taskRecord)
			if record.state != Assigned {
				return true
			}
			if time.Since(record.start) > 10*time.Second {
				c.mapState.Store(num, taskRecord{})
			}
			return true
		})
		c.reduceState.Range(func(num, r interface{}) bool {
			record := r.(taskRecord)
			if record.state != Assigned {
				return true
			}
			if time.Since(record.start) > 10*time.Second {
				c.reduceState.Store(num, taskRecord{})
			}
			return true
		})
	}
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
	return int(c.finishedR.Load()) == c.r
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
		c.mapState.Store(i, taskRecord{})
	}
	for i := 0; i < c.r; i++ {
		c.reduceState.Store(i, taskRecord{})
	}

	go c.checkStragglers()

	c.server()
	return &c
}
