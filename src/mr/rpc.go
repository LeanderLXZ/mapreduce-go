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
	NReduce  int
	WorkerID int
}

type RequestTaskArgs struct {
	WorkerID int
}

type RequestTaskReply struct {
	FileName string
	TaskMode string
	TaskID   int
}

type ReportTaskArgs struct {
	WorkerID int
	TaskID   int
	TaskMode string
	Msg      string
}

type ReportTaskReply struct {
}

// Add your RPC definitions here.
