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
			time := time.Now().Unix()
			task := Task{m.TaskID, m.FileList[0], args.WorkerID, time}

			reply.FileName = task.Files
			if m.MapDone == false { //map task
				reply.TaskMode = "map"
			} else {
				reply.TaskMode = "reduce"
			}
			reply.TaskID = task.TaskID

			m.FileList = m.FileList[1:]
			//workerlist update, do I need workerlist?
			m.TaskList = append(m.TaskList, task)
			m.TaskID++
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

func UpdateTaskList(TaskList []Task, TaskID int) []Task {
	for i := 0; i < len(TaskList); i++ { //update TaskList
		if TaskList[i].TaskID == TaskID {
			TaskList = append(TaskList[:i], TaskList[i+1:]...)
		}
	}
	return TaskList
}

func CheckTaskList(TaskList []Task, TaskID int) (string, int, int64) {
	var FileName string
	var WorkerID int
	var time int64

	for i := 0; i < len(TaskList); i++ { //update TaskList
		if TaskList[i].TaskID == TaskID {
			FileName = TaskList[i].Files
			WorkerID = TaskList[i].WorkerID
			time = TaskList[i].Time
		}
	}
	return FileName, WorkerID, time
}

func UpdateTaskMode(m *Master, taskMode string) error {
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
			ClearIntermediate(m.NReduce)
		}
	}
	return nil
}

func Clear(nReduce int) error {
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

func ClearIntermediate(nNReduce int) error {
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

//ReportTask is an RPC method that is called by workers to report a task's status
//whenever a task is finished or failed
//HINT: when a task is failed, master should reschedule it.
func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.RPTMutex.Lock()
	msg := args.Msg

	if msg == "failed" {
		var FileName string
		FileName, _, _ = CheckTaskList(m.TaskList, args.TaskID)
		m.TaskList = UpdateTaskList(m.TaskList, args.TaskID)
		m.FileList = append(m.FileList, FileName)
	} else if msg == "done" {
		m.TaskList = UpdateTaskList(m.TaskList, args.TaskID)
		UpdateTaskMode(m, args.TaskMode)
	} else if msg == "working" {
		time1 := time.Now().Unix()
		_, _, time0 := CheckTaskList(m.TaskList, args.TaskID)
		if time0-time1 > 10 {
			m.TaskList = UpdateTaskList(m.TaskList, args.TaskID)
		}
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

	Clear(m.NReduce)

	go m.server()

	return &m
}
