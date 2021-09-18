package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"

	"github.com/google/uuid"
)

type CoordinatorState int64

var (
	CoordinatorStateMapping  CoordinatorState = 1
	CoordinatorStateReducing CoordinatorState = 2
)

type MapTaskState int64

var (
	MapTaskStateWaiting  MapTaskState = 1
	MapTaskStateRunning  MapTaskState = 2
	MapTaskStateFinished MapTaskState = 3
)

type MapTask struct {
	MapTaskIds   []string
	Split        string
	MapTaskState MapTaskState
}

type ReduceTaskState int64

var (
	ReduceTaskStateWaiting  ReduceTaskState = 1
	ReduceTaskStateRunning  ReduceTaskState = 2
	ReduceTaskStateFinished ReduceTaskState = 3
)

type ReduceTask struct {
	ReduceTaskIds     []string
	ReduceTaskIndex   int
	IntermediateFiles []string
	OutputFile        string
	ReduceTaskState   ReduceTaskState
}

// TODO: 加锁
type Coordinator struct {
	// Your definitions here.
	CoordinatorState CoordinatorState
	MapTasks         []MapTask
	ReduceTasks      []ReduceTask
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	if c.CoordinatorState == CoordinatorStateMapping {
		// TODO: 加锁
		for _, mapTask := range c.MapTasks {
			if mapTask.MapTaskState == MapTaskStateWaiting {
				reply.TaskId = uuid.NewString()
				reply.TaskType = TaskTypeMap
				reply.NReduce = int64(len(c.ReduceTasks))
				reply.Content = []string{mapTask.Split}
			}
		}
	}
	return nil
}

func (c *Coordinator) MapTaskDone(args *MapTaskDoneArgs, reply *MapTaskDoneReply) error {
	// 加锁
	for _, mapTask := range c.MapTasks {
		for _, id := range mapTask.MapTaskIds {
			if args.MapTaskId == id {
				mapTask.MapTaskState = MapTaskStateFinished
				for i, n := range args.intermediateFileNames {
					// RPC 不改变顺序的话就这样写
					c.ReduceTasks[i].IntermediateFiles = append(c.ReduceTasks[i].IntermediateFiles, n)
				}
				return nil
			}
		}
	}
	return fmt.Errorf("Who the fuck are you?")
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	mapTasks := make([]MapTask, len(files))
	for i, file := range files {
		mapTask := MapTask{
			Split:        file,
			MapTaskState: MapTaskStateWaiting,
		}
		mapTasks[i] = mapTask
	}
	c.MapTasks = mapTasks

	reduceTasks := make([]ReduceTask, nReduce)
	for i := 0; i < nReduce; i++ {
		reduceTask := ReduceTask{
			ReduceTaskIndex: i,
			ReduceTaskState: ReduceTaskStateWaiting,
		}
		reduceTasks[i] = reduceTask
	}
	c.ReduceTasks = reduceTasks

	c.server()
	c.mapCoordinaton()

	return &c
}

func (c *Coordinator) mapCoordinaton() {
	for {
		for _, mapTask := range c.MapTasks {
			if mapTask.MapTaskState != MapTaskStateFinished {
				time.Sleep(3 * time.Second)
				continue
			}
		}
		break
	}
}
