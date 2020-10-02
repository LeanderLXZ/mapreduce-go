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
	nReduce, workerID := Register()

	DPrintf("Registered\n")

	for {
		// Request map task
		fileName, taskMode, taskID := Request(workerID)

		if taskMode == "wait" {
			// No task assigned, waiting for master
			DPrintf("%v %v %v\n", taskMode, workerID, taskID)
			time.Sleep(time.Second)
		} else if taskMode == "done" {
			// All task done, exit worker
			DPrintf("%v %v %v\n", taskMode, workerID, taskID)
			log.Fatal("All task done, worker exited.")
			break
		} else if taskMode == "map" {
			// Map
			// Report to master that the work has started
			Report(workerID, taskID, taskMode, "working")
			DPrintf("%v %v %v\n", taskMode, workerID, taskID)

			intermediate := []KeyValue{}
			file, err := os.Open(fileName)
			if err != nil {
				Report(workerID, taskID, taskMode, "failed")
				break
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				Report(workerID, taskID, taskMode, "failed")
				file.Close()
				break
			}
			file.Close()
			kva := mapf(fileName, string(content))
			intermediate = append(intermediate, kva...)

			// Split keys to nReduce files
			imfiles := make([][]KeyValue, nReduce)
			for i := 0; i < len(intermediate); i++ {
				kv := intermediate[i]
				r := ihash(kv.Key) % nReduce
				imfiles[r] = append(imfiles[r], kv)
			}

			// Generate intermediate files for nReduce workers
			for r := 0; r < nReduce; r++ {
				imfileName := fmt.Sprintf("mr-%v-%v", taskID, r)
				imfile, _ := os.Create(imfileName)
				enc := json.NewEncoder(imfile)
				for _, kv := range imfiles[r] {
					enc.Encode(&kv)
				}
				imfile.Close()
			}
			// Report to master that the work has finished
			Report(workerID, taskID, taskMode, "done")
		} else if taskMode == "reduce" {
			//Reduce
			// Report to master that the work has started
			Report(workerID, taskID, taskMode, "working")
			DPrintf("%v %v %v\n", taskMode, workerID, taskID)

			intermediate := []KeyValue{}
			files := strings.Split(fileName, " ")
			reduceFailed := false
			for _, f := range files {
				if f != "" {
					// Get intermediate key value pairs from file
					file, err := os.Open(f)
					if err != nil {
						DPrintf("Reduce open failed %v\n", f)
						reduceFailed = true
						break
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						intermediate = append(intermediate, kv)
					}
				}
			}
			if reduceFailed == true {
				Report(workerID, taskID, taskMode, "failed")
				DPrintf("Reduce failed %v %v %v\n", workerID, taskID, taskMode)
				break
			}

			// Sort intermediate keys
			sort.Sort(ByKey(intermediate))

			// Reduce
			outputName := fmt.Sprintf("mr-out-%v", taskID)
			outputfile, _ := os.Create(outputName)
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
				fmt.Fprintf(outputfile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			// Report to master that the work has finished
			Report(workerID, taskID, taskMode, "done")
		} else {
			// Wrong mode, work failed
			Report(workerID, taskID, taskMode, "failed")
			DPrintf("%v %v %v\n", taskMode, workerID, taskID)
		}

		// uncomment to send the Example RPC to the master.
		// CallExample()
	}
}

func Register() (int, int) {
	args := RegisterWorkerArgs{}
	reply := RegisterWorkerReply{}
	call("Master.RegisterWorker", &args, &reply)
	nReduce := reply.NReduce
	workerID := reply.WorkerID
	// DPrintf("Cannot read %v ===", nReduce)
	return nReduce, workerID
}

func Request(workerID int) (string, string, int) {
	args := RequestTaskArgs{
		WorkerID: workerID,
	}
	reply := RequestTaskReply{}
	for call("Master.RequestTask", &args, &reply) == false {
		DPrintf("Cannot read")
		time.Sleep(time.Second)
	}
	fileName := reply.FileName
	taskMode := reply.TaskMode
	taskID := reply.TaskID
	return fileName, taskMode, taskID
}

func Report(workerID int, taskID int, taskMode string, msg string) {
	args := ReportTaskArgs{
		WorkerID: workerID,
		TaskID:   taskID,
		TaskMode: taskMode,
		Msg:      msg,
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
