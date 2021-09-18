package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		reply, ok := CallGetTask()
		if !ok {
			fmt.Printf("CallGetWork fucked. Worker gone.")
			return
		}
		switch reply.TaskType {
		case TaskTypeOff:
			fmt.Printf("Work done, Worker gone.")
			return
		case TaskTypeMap:
			intermediateFileNames, ok := doMap(reply.TaskId, reply.Content[0], reply.NReduce, mapf)
			if !ok {
				fmt.Printf("mapf fucked. Worker gone.")
				return
			}
			_, ok = CallMapTaskDone(intermediateFileNames)
			if !ok {
				fmt.Printf("CallMapTaskDone fucked. Worker gone.")
				return
			}
		case TaskTypeReduce:
			// TODO
			// 遍历所有的intermediateFile，reduce属于自己的key
		case TaskTypeWait:
			fmt.Printf("No work to do. Worker waits.")
			time.Sleep(10 * time.Second)
		default:
			fmt.Printf("WorkType wrong. Worker gone.")
			return
		}
	}
}

func doMap(mapTaskId string, splitName string, nReduce int64,
	mapf func(string, string) []KeyValue) ([]string, bool) {
	intermediateFileNames := make([]string, nReduce)
	for i := 0; i < int(nReduce); i++ {
		intermediateFileNames = append(intermediateFileNames,
			fmt.Sprintf("tmp/intermediate-%s-%v", mapTaskId, i))
	}
	split, err := os.Open(splitName)
	if err != nil {
		log.Fatalf("cannot open %v", splitName)
	}
	content, err := ioutil.ReadAll(split)
	if err != nil {
		log.Fatalf("cannot read %v", splitName)
	}
	split.Close()
	kva := mapf(splitName, string(content))

	intermediateFiles := make([]*os.File, 0)
	for _, fileName := range intermediateFileNames {
		file, err := os.Create(fileName)
		if err != nil {
			fmt.Printf("%+v", err)
			return nil, false
		}
		intermediateFiles = append(intermediateFiles, file)
	}

	for _, kv := range kva {
		fmt.Fprintf(intermediateFiles[ihash(kv.Key)], "%v %v\n", kv.Key, kv.Value)
	}

	for _, file := range intermediateFiles {
		file.Close()
	}

	// TODO: map的同时shuffle
	return intermediateFileNames, true
}

func CallGetTask() (*GetTaskReply, bool) {
	args := &GetTaskArgs{}
	reply := &GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		return nil, false
	}
	return reply, true
}

func CallMapTaskDone(intermediateFileNames []string) (*MapTaskDoneReply, bool) {
	args := &MapTaskDoneArgs{}
	args.intermediateFileNames = intermediateFileNames
	reply := &MapTaskDoneReply{}
	ok := call("Coordinator.MapTaskDone", &args, &reply)
	if !ok {
		return nil, false
	}
	return reply, true
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
