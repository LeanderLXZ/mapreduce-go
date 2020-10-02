package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
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

// for sorting by key
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.

	// Register workers for map
	nReduce, workerId := Register()

	for {
		// Request map task
		fileName, taskMode, taskId := Request(workerId)

		if taskMode == "wait" {
			// No task assigned, waiting for master
			time.Sleep(time.Second)
		} else if taskMode == "done" {
			// All task done, exit worker
			log.Fatal("All task done, worker exited.")
			break
		} else if taskMode == "map" {
			// Map
			// Report to master that the work has started
			Report(workerId, taskId, taskMode, "working")

			intermediate := []KeyValue{}
			file, err := os.Open(fileName)
			if err != nil {
				Report(workerId, taskId, taskMode, "failed")
				break
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				Report(workerId, taskId, taskMode, "failed")
				file.Close()
				break
			}
			file.Close()
			kva := mapf(fileName, string(content))
			intermediate = append(intermediate, kva...)

			// Split keys to nReduce files
			imFiles := make([][]KeyValue, nReduce)
			for i := 0; i < len(intermediate); i++ {
				kv := intermediate[i]
				r := ihash(kv.Key) % nReduce
				imFiles[r] = append(imFiles[r], kv)
			}

			// Generate intermediate files for nReduce workers
			for r := 0; r < nReduce; r++ {
				imFileName := fmt.Sprintf("mr-%v-%v", taskId, r)
				imFile, _ := os.Create(imFileName)
				enc := json.NewEncoder(imFile)
				for _, kv := range imFiles[r] {
					enc.Encode(&kv)
				}
				imFile.Close()
			}
			// Report to master that the work has finished
			Report(workerId, taskId, taskMode, "done")
		} else if taskMode == "reduce" {
			//Reduce
			// Report to master that the work has started
			Report(workerId, taskId, taskMode, "working")

			intermediate := []KeyValue{}
			files := strings.Split(fileName, " ")
			reduceFailed := false
			for _, f := range files {
				// Get intermediate key value pairs from file
				file, err := os.Open(f)
				if err != nil {
					reduceFailed = true
					break
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						reduceFailed = true
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			if reduceFailed == true {
				Report(workerId, taskId, taskMode, "failed")
				break
			}

			// Sort intermediate keys
			sort.Sort(ByKey(intermediate))

			// Reduce
			outputName := fmt.Sprintf("mr-out-%v", taskId)
			outputFile, _ := os.Create(outputName)
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// Out the reduced files
				fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			// Report to master that the work has finished
			Report(workerId, taskId, taskMode, "done")
		} else {
			// Wrong mode, work failed
			Report(workerId, taskId, taskMode, "failed")
		}

		// uncomment to send the Example RPC to the master.
		// CallExample()
	}
}

func Register() (int, int) {
	args := RegisterWorkerArgs{}
	reply := RegisterWorkerReply{}
	for call("Master.RegisterWorker", &args, &reply) == false {
		time.Sleep(time.Second)
	}
	nReduce := reply.nReduce
	workerId := reply.workerId
	// DPrintf("Got workerId %v\n", workerId)
	return nReduce, workerId
}

func Request(workerId int) (string, string, int) {
	args := RequestTaskArgs{
		workerId: workerId,
	}
	reply := RequestTaskReply{}
	for call("Master.RequestTask", &args, &reply) == false {
		time.Sleep(time.Second)
	}
	fileName := reply.fileName
	taskMode := reply.taskMode
	taskId := reply.taskId
	return fileName, taskMode, taskId
}

func Report(workerId int, taskId int, taskMode string, msg string) {
	args := ReportTaskArgs{
		workerId: workerId,
		taskId:   taskId,
		taskMode: taskMode,
		msg:      msg,
	}
	reply := ReportTaskReply{}
	for call("Master.ReportTask", &args, &reply) == false {
		time.Sleep(time.Second)
	}
}

//
// example function to show how to make an RPC call to the master.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", "mr-socket")
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
