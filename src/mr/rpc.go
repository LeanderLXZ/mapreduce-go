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
	// InputFiles []string
}

type RequestTaskArgs struct {
	workerId int
}

type RequestTaskReply struct {
	fileName string
	taskId   int
}

type ReportTaskArgs struct {
}

type ReportTaskReply struct {
}

// Add your RPC definitions here.
