package mr

//
// RPC definitions.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type RegisterWorkerArgs struct {
}

type RegisterWorkerReply struct {
	nReduce  int
	workerID int
	// InputFiles []string
}

type RequestTaskArgs struct {
	workerID int
}

type RequestTaskReply struct {
	fileName string
	taskMode string
	taskID   int
}

type ReportTaskArgs struct {
	workerID int
	taskID   int
	taskMode string
	msg      string
}

type ReportTaskReply struct {
}

// Add your RPC definitions here.
