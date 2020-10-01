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

	// TODO: register workers for map
	nReduce, workerID := Register()

	// TODO: Request map task
	fileName, taskMode, taskID := Request(workerID)

	if taskMode == "map" {
		// Map
		intermediate := []KeyValue{}
		file, err := os.Open(fileName)
		if err != nil {
			Report(workerID, taskID, "failed")
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", fileName)
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
		for i := 0; i < nReduce; i++ {
			imFileName := fmt.Sprintf("mr-%v-%v", taskID, i)
			imFile, _ := os.Create(imFileName)
			enc := json.NewEncoder(imFile)
			for _, kv := range imFiles[i] {
				err := enc.Encode(&kv)
			}
			imFile.Close()
		}
	} else if taskMode == "reduce" {
		// Get intermediate key value pairs from file
		intermediate := []KeyValue{}
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}

		// Sort intermediate keys
		sort.Sort(ByKey(intermediate))

		// Reduce
		outputName := fmt.Sprintf("mr-out-%v", taskID)
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

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}

	} else {
		log.Fatalf("Wrong mode for worker")
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func Register() (int, int) {
	args := RegisterWorkerArgs{}
	reply := RegisterWorkerReply{}

	// send the RPC request, wait for the reply.
	for call("Master.RegisterWorker", &args, &reply) == false {
		time.Sleep(time.Second)
	}

	// workerID := reply.workerID
	DPrintf("Got workerID %v\n", reply.workerID)
	// DPrintf("Got a list of files %v\n", reply.InputFiles)

	nReduce := reply.nReduce
	workerID := reply.workerID
	return nReduce, workerID
}

func Request(workerID int) (string, string, int) {
	args := RequestTaskArgs{
		workerID: workerID,
	}
	reply := RequestTaskReply{}
	for call("Master.RequestTask", &args, &reply) == false {
		time.Sleep(time.Second)
	}
	fileName := reply.fileName
	taskMode := reply.taskMode
	taskID := reply.taskID
	return fileName, taskMode, taskID
}

func Report(workerID int, taskID int, msg string) {
	args := ReportTaskArgs{
		workerID: workerID,
		taskID:   taskID,
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
