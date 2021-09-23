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
				break
			}
			fmt.Printf("doMap done. Calling MapTaskDone.\n")
			_, ok = CallMapTaskDone(reply.TaskId, intermediateFileNames)
			if !ok {
				fmt.Printf("CallMapTaskDone fucked.")
			}
		case TaskTypeReduce:
			outputFileName, ok := doReduce(reply.JobId, reply.TaskId, reply.ReduceTaskIndex, reply.Content, reducef)
			if !ok {
				fmt.Printf("reducef fucked.\n")
				break
			}
			fmt.Printf("doReduce done. Calling ReduceTaskDone.\n")
			_, ok = CallReduceTaskDone(reply.TaskId, outputFileName)
			if !ok {
				fmt.Printf("CallReduceTaskDone fucked.")
			}
		case TaskTypeWait:
			fmt.Printf("No work to do. Worker waits.\n")
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
		intermediateFileNames[i] = fmt.Sprintf(".intermediate-%s-%v-%s.txt", jobId, i, mapTaskId)
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

	fmt.Println("mapf done.")

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
		fmt.Fprintf(intermediateFiles[ihash(kv.Key)%int(nReduce)], "%v %v\n", kv.Key, kv.Value)
	}

	for _, file := range intermediateFiles {
		file.Close()
	}

	return intermediateFileNames, true
}

func doReduce(jobId string, reduceTaskId string, index int64, intermidiateFileNames []string,
	reducef func(string, []string) string) (string, bool) {
	kvs := make(map[string][]string)
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
		lines = lines[:len(lines)-1]
		for _, l := range lines {
			kv := strings.Split(l, " ")
			if _, ok := kvs[kv[0]]; ok {
				kvs[kv[0]] = append(kvs[kv[0]], kv[1])
			} else {
				kvs[kv[0]] = make([]string, 0)
				kvs[kv[0]] = append(kvs[kv[0]], kv[1])
			}
		}
	}

	output := ""
	for k, vs := range kvs {
		o := reducef(k, vs)
		output = fmt.Sprintf("%s%v %v\n", output, k, o)
	}

	outputFileName := fmt.Sprintf("mr-out-%v.%s.tmp", index, reduceTaskId)
	outputFile, err := os.Create(outputFileName)
	if err != nil {
		fmt.Printf("%+v", err)
		return "", false
	}
	fmt.Fprintf(outputFile, "%s", output)
	outputFile.Close()
	os.Rename(outputFileName, fmt.Sprintf("mr-out-%v", index))
	return outputFileName, true
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

func CallMapTaskDone(mapTaskId string, intermediateFileNames []string) (*MapTaskDoneReply, bool) {
	args := &MapTaskDoneArgs{}
	args.MapTaskId = mapTaskId
	args.IntermediateFileNames = intermediateFileNames
	reply := &MapTaskDoneReply{}
	ok := call("Coordinator.MapTaskDone", &args, &reply)
	if !ok {
		return nil, false
	}
	return reply, true
}

func CallReduceTaskDone(reduceTaskId string, outputFileName string) (*ReduceTaskDoneReply, bool) {
	args := &ReduceTaskDoneArgs{}
	args.ReduceTaskId = reduceTaskId
	args.OutputFileName = outputFileName
	reply := &ReduceTaskDoneReply{}
	ok := call("Coordinator.ReduceTaskDone", &args, &reply)
	if !ok {
		return nil, false
	}
	return reply, true
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
