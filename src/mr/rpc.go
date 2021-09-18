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
	Y int
}

type GetTaskArgs struct {
}

type TaskType int

var (
	TaskTypeOff    TaskType = 0
	TaskTypeMap    TaskType = 1
	TaskTypeReduce TaskType = 2
	TaskTypeWait   TaskType = 3
)

type GetTaskReply struct {
	TaskType TaskType
	TaskId   string
	Content  []string
	NReduce  int64
}

type MapTaskDoneArgs struct {
	MapTaskId             string
	intermediateFileNames []string
}

type MapTaskDoneReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
