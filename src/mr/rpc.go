package mr

import (
	"os"
	"strconv"
)

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
	JobId           string
	TaskType        TaskType
	TaskId          string
	Content         []string
	NReduce         int64
	ReduceTaskIndex int64
}

type MapTaskDoneArgs struct {
	MapTaskId             string
	IntermediateFileNames []string
}

type MapTaskDoneReply struct {
}

type ReduceTaskDoneArgs struct {
	ReduceTaskId   string
	OutputFileName string
}

type ReduceTaskDoneReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
