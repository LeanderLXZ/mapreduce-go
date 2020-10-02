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
	workerId int
}

type RequestTaskArgs struct {
	workerId int
}

type RequestTaskReply struct {
	fileName string
	taskMode string
	taskId   int
}

type ReportTaskArgs struct {
	workerId int
	taskId   int
	taskMode string
	msg      string
}

type ReportTaskReply struct {
	taskMode string
}

// Add your RPC definitions here.
