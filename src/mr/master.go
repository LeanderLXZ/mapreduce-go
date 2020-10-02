package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	FileList    []string       //rest files list
	TaskList    map[int]string // takes status list
	WorkingList map[int]Task   // working files--WorkerID list

	NReduce   int
	WorkerNum int
	TaskID    int
	MapDone   bool
	AllDone   bool

	RWMutex  sync.Mutex
	RQTMutex sync.Mutex
	RPTMutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	DPrintf("Worker has called the Example RPC\n")
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.AllDone
}

//RegisterWorker is an RPC method that is called by workers after they have started
// up to report that they are ready to receive tasks.
func (m *Master) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	m.RWMutex.Lock()
	reply.WorkerID = m.WorkerNum
	reply.NReduce = m.NReduce
	m.WorkerNum++
	m.RWMutex.Unlock()
	return nil
}

//RequestTask is an RPC method that is called by workers to request a map or reduce task
func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	m.RQTMutex.Lock()
	if m.AllDone == false {
		if len(m.FileList) != 0 {
			task := Task{m.TaskID, m.FileList[0], args.WorkerID}
			reply.FileName = task.FileName
			reply.TaskID = task.TaskID
			if m.MapDone == false { //map task
				reply.TaskMode = "map"
			} else {
				reply.TaskMode = "reduce"
			}
			m.FileList = m.FileList[1:]
			m.WorkingList[task.TaskID] = task
			m.TaskList[task.TaskID] = ""
			m.TaskID++

			// Run a ticker for task
			go m.taskTicker(task.TaskID)
		} else {
			// tell worker to wait new task
			reply.TaskMode = "wait"
		}
	} else { // AllDone
		reply.TaskMode = "done"
	}
	m.RQTMutex.Unlock()
	return nil
}

//ReportTask is an RPC method that is called by workers to report a task's status
//whenever a task is finished or failed
//HINT: when a task is failed, master should reschedule it.
func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.RPTMutex.Lock()
	if args.Msg == "failed" {
		m.resetTask(args.TaskID)
		m.TaskList[args.TaskID] = "failed"
	} else if args.Msg == "done" {
		m.TaskList[args.TaskID] = "done"
		m.finishTask(args.TaskID, args.TaskMode)
	} else if args.Msg == "working" {
		m.TaskList[args.TaskID] = "working"
	}
	m.RPTMutex.Unlock()
	return nil
}

//
// create a Master.
//
func MakeMaster(files []string, NReduce int) *Master {
	m := Master{}
	// Your code here.
	m.FileList = files
	m.TaskList = make(map[int]string)
	m.WorkingList = make(map[int]Task)
	m.NReduce = NReduce
	m.WorkerNum = 0
	m.TaskID = 0
	m.MapDone = false
	m.AllDone = false
	clearAll(m.NReduce)
	go m.server()
	return &m
}

func (m *Master) taskTicker(taskID int) {
	ticker := time.NewTicker(10 * time.Second)
	for {
		// Get rid of read and write a same map
		time.Sleep(time.Second)
		select {
		case <-ticker.C:
			m.RQTMutex.Lock()
			m.resetTask(taskID)
			m.RQTMutex.Unlock()
			return
		default:
			taskStatus := m.TaskList[taskID]
			if taskStatus == "done" || taskStatus == "failed" {
				return
			}
		}
	}
}

func (m *Master) updateTaskMode(taskMode string) error {
	if len(m.FileList) == 0 && len(m.WorkingList) == 0 {
		if taskMode == "map" {
			m.MapDone = true
			files, _ := ioutil.ReadDir("./")

			// Update the filelist to reduce files
			rFileList := make([]string, m.NReduce)
			for r := 0; r < m.NReduce; r++ {
				for _, f := range files {
					pattern := fmt.Sprintf("mr-\\d*-%v", r)
					matched, _ := regexp.MatchString(pattern, f.Name())
					if matched == true {
						rFileList[r] = strings.Join([]string{rFileList[r], f.Name()}, " ")
					}
				}
			}
			m.FileList = rFileList
			m.TaskID = 0
		} else if taskMode == "reduce" {
			m.AllDone = true
			clearIntermediate(m.NReduce)
		}
	}
	return nil
}

func (m *Master) resetTask(taskID int) error {
	fileName := m.WorkingList[taskID].FileName
	m.FileList = append(m.FileList, fileName)
	delete(m.WorkingList, taskID)
	return nil
}

func (m *Master) finishTask(taskID int, taskMode string) error {
	delete(m.WorkingList, taskID)
	m.updateTaskMode(taskMode)
	return nil
}

func clearAll(nReduce int) error {
	// Update the filelist to reduce files
	for r := 0; r < nReduce; r++ {
		files, _ := ioutil.ReadDir("./")
		for _, f := range files {
			patternIM := fmt.Sprintf("mr-\\d*-%v", r)
			matchedIM, _ := regexp.MatchString(patternIM, f.Name())
			patternOUT := fmt.Sprintf("mr-out-\\d*")
			matchedOUT, _ := regexp.MatchString(patternOUT, f.Name())
			if matchedIM == true || matchedOUT == true {
				os.Remove(f.Name())
			}
		}
	}
	return nil
}

func clearIntermediate(nNReduce int) error {
	// Update the filelist to reduce files
	for r := 0; r < nNReduce; r++ {
		files, _ := ioutil.ReadDir("./")
		for _, f := range files {
			patternIM := fmt.Sprintf("mr-\\d*-%v", r)
			matchedIM, _ := regexp.MatchString(patternIM, f.Name())
			if matchedIM == true {
				os.Remove(f.Name())
			}
		}
	}
	return nil
}
