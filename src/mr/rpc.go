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
	InputFiles []string
}

type RequestTaskArgs struct {
}

type RequestTaskReply struct {
}

type ReportTaskArgs struct {
}

type ReportTaskReply struct {
}

// Add your RPC definitions here.
