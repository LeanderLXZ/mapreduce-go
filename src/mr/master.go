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
	FileList   []string //rest files list
	TaskList   []Task   //working files--WorkerID list
	WorkerList []int

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
	// DPrintf("Sending file list: %v\n", reply.InputFiles)
	return nil
}

//RequestTask is an RPC method that is called by workers to request a map or reduce task
func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	m.RQTMutex.Lock()
	if m.AllDone == false {
		if len(m.FileList) != 0 {
			task := Task{m.TaskID, m.FileList[0], args.WorkerID, "working"}
			reply.FileName = task.Files
			reply.TaskID = task.TaskID
			if m.MapDone == false { //map task
				reply.TaskMode = "map"
			} else {
				reply.TaskMode = "reduce"
			}
			m.FileList = m.FileList[1:]
			m.TaskList = append(m.TaskList, task)
			m.TaskID++

			// Run a ticker for task
			go m.taskTicker(task)
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
		m.setTaskStatus(args.TaskID, "failed")
	} else if args.Msg == "done" {
		m.finishTask(args.TaskID, args.TaskMode)
		m.setTaskStatus(args.TaskID, "done")
	} else if args.Msg == "working" {
		m.setTaskStatus(args.TaskID, "working")
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
	m.NReduce = NReduce
	m.WorkerNum = 0
	m.TaskID = 0
	m.MapDone = false
	m.AllDone = false
	clearAll(m.NReduce)
	go m.server()
	return &m
}

func (m *Master) taskTicker(task Task) {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-ticker.C:
			m.RQTMutex.Lock()
			m.resetTask(task.TaskID)
			m.RQTMutex.Unlock()
		default:
			if task.Status == "done" || task.Status == "failed" {
				return
			}
		}
	}
}

func (m *Master) updateTaskMode(taskMode string) error {
	if len(m.FileList) == 0 && len(m.TaskList) == 0 {
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

func (m *Master) popTaskList(taskID int) {
	for i := 0; i < len(m.TaskList); i++ { //update TaskList
		if m.TaskList[i].TaskID == taskID {
			m.TaskList = append(m.TaskList[:i], m.TaskList[i+1:]...)
		}
	}
}

func (m *Master) resetTask(taskID int) error {
	m.popTaskList(taskID)
	var fileName string
	for i := 0; i < len(m.TaskList); i++ { //update TaskList
		if m.TaskList[i].TaskID == taskID {
			fileName = m.TaskList[i].Files
		}
	}
	m.FileList = append(m.FileList, fileName)
	return nil
}

func (m *Master) finishTask(taskID int, taskMode string) error {
	m.popTaskList(taskID)
	m.updateTaskMode(taskMode)
	return nil
}

func (m *Master) setTaskStatus(taskID int, status string) error {
	for i := 0; i < len(m.TaskList); i++ { //update TaskList
		if m.TaskList[i].TaskID == taskID {
			m.TaskList[i].Status = status
		}
	}
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
