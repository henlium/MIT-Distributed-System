package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type Empty struct{}

type SingleInt struct {
	Value int
}

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

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
