package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

type CoordinatorState int64

var (
	CoordinatorStateMapping  CoordinatorState = 1
	CoordinatorStateReducing CoordinatorState = 2
	CoordinatorStateFinished CoordinatorState = 3
)

type MapTaskState int64

var (
	MapTaskStateWaiting  MapTaskState = 1
	MapTaskStateRunning  MapTaskState = 2
	MapTaskStateExpire   MapTaskState = 3
	MapTaskStateFinished MapTaskState = 4
)

type MapTask struct {
	MapTaskIds   []string
	Split        string
	MapTaskState MapTaskState
	StartTime    int64
}

type ReduceTaskState int64

var (
	ReduceTaskStateWaiting  ReduceTaskState = 1
	ReduceTaskStateRunning  ReduceTaskState = 2
	ReduceTaskStateExpire   ReduceTaskState = 3
	ReduceTaskStateFinished ReduceTaskState = 4
)

type ReduceTask struct {
	ReduceTaskIds     []string
	ReduceTaskIndex   int64
	IntermediateFiles []string
	OutputFile        string
	ReduceTaskState   ReduceTaskState
	StartTime         int64
}

// TODO: 加锁
type Coordinator struct {
	// Your definitions here.
	JobId            string
	CoordinatorState CoordinatorState
	MapTasks         []*MapTask
	ReduceTasks      []*ReduceTask
	OutputFileNames  []string
	L                *sync.Mutex
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
	c.L.Lock()
	defer c.L.Unlock()
	if c.CoordinatorState == CoordinatorStateMapping {
		for _, mapTask := range c.MapTasks {
			if mapTask.MapTaskState == MapTaskStateWaiting ||
				mapTask.MapTaskState == MapTaskStateExpire {
				reply.JobId = c.JobId
				reply.TaskId = uuid.NewString()
				reply.TaskType = TaskTypeMap
				reply.NReduce = int64(len(c.ReduceTasks))
				reply.Content = []string{mapTask.Split}
				mapTask.MapTaskIds = append(mapTask.MapTaskIds, reply.TaskId)
				mapTask.MapTaskState = MapTaskStateRunning
				mapTask.StartTime = time.Now().Unix()
				return nil
			}
		}
		reply.TaskType = TaskTypeWait
		return nil
	} else if c.CoordinatorState == CoordinatorStateReducing {
		for _, reduceTask := range c.ReduceTasks {
			if reduceTask.ReduceTaskState == ReduceTaskStateWaiting ||
				reduceTask.ReduceTaskState == ReduceTaskStateExpire {
				reply.JobId = c.JobId
				reply.TaskId = uuid.NewString()
				reply.TaskType = TaskTypeReduce
				reply.NReduce = int64(len(c.ReduceTasks))
				reply.Content = reduceTask.IntermediateFiles
				reply.ReduceTaskIndex = reduceTask.ReduceTaskIndex
				reduceTask.ReduceTaskIds = append(reduceTask.ReduceTaskIds, reply.TaskId)
				reduceTask.ReduceTaskState = ReduceTaskStateRunning
				reduceTask.StartTime = time.Now().Unix()
				return nil
			}
		}
		reply.TaskType = TaskTypeWait
		return nil
	} else if c.CoordinatorState == CoordinatorStateFinished {
		reply.TaskType = TaskTypeOff
		return nil
	}
	return nil
}

func (c *Coordinator) MapTaskDone(args *MapTaskDoneArgs, reply *MapTaskDoneReply) error {
	c.L.Lock()
	defer c.L.Unlock()
	for _, mapTask := range c.MapTasks {
		for _, id := range mapTask.MapTaskIds {
			if args.MapTaskId == id {
				println("meimnaobin")
				mapTask.MapTaskState = MapTaskStateFinished
				for i, n := range args.IntermediateFileNames {
					// RPC 不改变顺序的话就这样写
					c.ReduceTasks[i].IntermediateFiles = append(c.ReduceTasks[i].IntermediateFiles, n)
				}
				return nil
			}
		}
	}
	return fmt.Errorf("who the fuck are you?")
}

func (c *Coordinator) ReduceTaskDone(args *ReduceTaskDoneArgs, reply *ReduceTaskDoneReply) error {
	c.L.Lock()
	defer c.L.Unlock()
	for _, reduceTask := range c.ReduceTasks {
		for _, id := range reduceTask.ReduceTaskIds {
			if args.ReduceTaskId == id {
				reduceTask.ReduceTaskState = ReduceTaskStateFinished
				c.OutputFileNames = append(c.OutputFileNames, args.OutputFileName)
				return nil
			}
		}
	}
	return fmt.Errorf("who the fuck are you?")
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
	return c.CoordinatorState == CoordinatorStateFinished
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.JobId = uuid.NewString()
	c.L = new(sync.Mutex)
	// Your code here.
	mapTasks := make([]*MapTask, len(files))
	for i, file := range files {
		mapTask := &MapTask{
			Split:        file,
			MapTaskState: MapTaskStateWaiting,
			StartTime:    time.Now().Unix(),
		}
		mapTasks[i] = mapTask
	}
	c.MapTasks = mapTasks

	reduceTasks := make([]*ReduceTask, nReduce)
	for i := 0; i < nReduce; i++ {
		reduceTask := &ReduceTask{
			ReduceTaskIndex: int64(i),
			ReduceTaskState: ReduceTaskStateWaiting,
		}
		reduceTasks[i] = reduceTask
	}
	c.ReduceTasks = reduceTasks
	c.CoordinatorState = CoordinatorStateMapping
	c.server()
	go c.coordinatorMoniter()
	return &c
}

func (c *Coordinator) coordinatorMoniter() {
	c.mapMoniter()
	c.reduceMoniter()
}

var ExpireTime int64 = 10

func (c *Coordinator) mapMoniter() {
	for {
		rolling := false
		c.L.Lock()
		for _, mapTask := range c.MapTasks {
			if mapTask.MapTaskState != MapTaskStateFinished {
				rolling = true
			}
			if mapTask.MapTaskState == MapTaskStateRunning &&
				time.Now().Unix()-mapTask.StartTime > ExpireTime {
				mapTask.MapTaskState = MapTaskStateExpire
			}
		}
		c.L.Unlock()
		if !rolling {
			fmt.Printf("mapMonitor: map finish.\n")
			c.L.Lock()
			c.CoordinatorState = CoordinatorStateReducing
			c.L.Unlock()
			break
		}
		fmt.Printf("mapMonitor: map still running.\n")
		time.Sleep(10 * time.Second)
	}
}

func (c *Coordinator) reduceMoniter() {
	for {
		rolling := false
		c.L.Lock()
		for _, reduceTask := range c.ReduceTasks {
			if reduceTask.ReduceTaskState != ReduceTaskStateFinished {
				rolling = true
			}
			if reduceTask.ReduceTaskState == ReduceTaskStateRunning &&
				time.Now().Unix()-reduceTask.StartTime > ExpireTime {
				reduceTask.ReduceTaskState = ReduceTaskStateExpire
			}
		}
		c.L.Unlock()
		if !rolling {
			c.L.Lock()
			c.CoordinatorState = CoordinatorStateFinished
			c.L.Unlock()
			break
		}
		time.Sleep(10 * time.Second)
	}
}
