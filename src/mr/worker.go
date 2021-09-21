package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
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
			fmt.Printf("CallGetWork fucked.")
			return
		}
		switch reply.TaskType {
		case TaskTypeOff:
			fmt.Printf("Work done.")
			return
		case TaskTypeMap:
			intermediateFileNames, ok := doMap(reply.JobId, reply.TaskId, reply.Content[0], reply.NReduce, mapf)
			if !ok {
				fmt.Printf("mapf fucked.")
			}
			_, ok = CallMapTaskDone(intermediateFileNames)
			if !ok {
				fmt.Printf("CallMapTaskDone fucked.")
			}
		case TaskTypeReduce:
			outputFileName, ok := doReduce(reply.JobId, reply.ReduceTaskIndex, reply.Content, reducef)
			if !ok {
				fmt.Printf("reducef fucked.")
			}
			_, ok = CallReduceTaskDone(outputFileName)
			if !ok {
				fmt.Printf("CallReduceTaskDone fucked.")
			}
		case TaskTypeWait:
			fmt.Printf("No work to do. Worker waits.")
			time.Sleep(10 * time.Second)
		default:
			fmt.Printf("WorkType wrong.")
		}
	}
}

func doMap(jobId string, mapTaskId string, splitName string, nReduce int64,
	mapf func(string, string) []KeyValue) ([]string, bool) {
	intermediateFileNames := make([]string, nReduce)
	for i := 0; i < int(nReduce); i++ {
		intermediateFileNames = append(intermediateFileNames,
			fmt.Sprintf("tmp-%s/intermediate-%v-%s", jobId, i, mapTaskId))
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
		fmt.Fprintf(intermediateFiles[ihash(kv.Key)], "%v,%v\n", kv.Key, kv.Value)
	}

	for _, file := range intermediateFiles {
		file.Close()
	}

	return intermediateFileNames, true
}

func doReduce(jobId string, index int64, intermidiateFileNames []string,
	reducef func(string, []string) string) (string, bool) {
	kvs := make(map[string][]string, 0)
	for _, inintermidiateFileName := range intermidiateFileNames {
		inintermidiateFile, err := os.Open(inintermidiateFileName)
		if err != nil {
			log.Fatalf("cannot open %v", inintermidiateFileName)
		}
		content, err := ioutil.ReadAll(inintermidiateFile)
		if err != nil {
			log.Fatalf("cannot read %v", inintermidiateFileName)
		}
		inintermidiateFile.Close()
		lines := strings.Split(string(content), "\n")
		for _, l := range lines {
			kv := strings.Split(l, ",")
			if vs, ok := kvs[kv[0]]; ok {
				vs = append(vs, kv[1])
			} else {
				kvs[kv[0]] = make([]string, 0)
				kvs[kv[0]] = append(kvs[kv[0]], kv[1])
			}
		}
	}
	outputFileName := fmt.Sprintf("mr-out-%v", index)
	if outputExists(outputFileName) {
		fmt.Printf("Output already exist.")
		return "", false
	}
	outputFile, err := os.Create(outputFileName)
	if err != nil {
		fmt.Printf("%+v", err)
		return "", false
	}
	for k, vs := range kvs {
		o := reducef(k, vs)
		fmt.Fprintf(outputFile, "%v,%v\n", k, o)
	}
	outputFile.Close()
	return outputFileName, true
}

func outputExists(path string) bool {
	_, err := os.Stat(path) //os.Stat获取文件信息
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
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
	args.IntermediateFileNames = intermediateFileNames
	reply := &MapTaskDoneReply{}
	ok := call("Coordinator.MapTaskDone", &args, &reply)
	if !ok {
		return nil, false
	}
	return reply, true
}

func CallReduceTaskDone(outputFileName string) (*ReduceTaskDoneReply, bool) {
	args := &ReduceTaskDoneArgs{}
	args.OutputFileName = outputFileName
	reply := &ReduceTaskDoneReply{}
	ok := call("Coordinator.ReduceTaskDone", &args, &reply)
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
