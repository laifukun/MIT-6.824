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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y string
}

type TaskType string

const (
	MAPTASK    = "map"
	REDUCETASK = "reduce"
	WAIT       = "wait"
	NOTASK     = "notask"
)

type TaskStatus int

const (
	NOTREADY = iota
	UNALLOCATED
	ALLOCATED
	INTERUPTED
	COMPLETED
)

type MSGType int

const (
	REQTASK = iota
	SENDINFO
)

type MRArgs struct {
	Message     MSGType //message type: 0: request task; 1, send info
	Filename    string
	STaskType   TaskType //map or reduce
	TaskID      int
	STaskStatus TaskStatus
}

type MRReply struct {
	RTaskType       TaskType
	Filename        string
	TotalReduceTask int
	TotalMapTask    int
	TaskID          int
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
