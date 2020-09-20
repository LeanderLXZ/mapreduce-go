/**
* @Author: huadong.hu@outlook.com
* @Date: 7/18/20 11:10
* @Desc:
 */
package mr

import (
	"fmt"
	"strconv"
)
type Task struct {
	//@TODO Your definitions here.
}

// jobPhase indicates whether a task is scheduled as a map or reduce task.
type JobPhase int
const (
	MapPhase    JobPhase = 0
	ReducePhase JobPhase = 1
)

// Debugging enabled?
// !!!!@TODO turn debugEnabled to false for grading
const debugEnabled = true

// DPrintf() will only print if debugEnabled is true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}


// reduceName constructs the name of the intermediate file which map task
// <mapTask> produces for reduce task <reduceTask>.
func reduceName(mapTask int, reduceTask int) string {
	return "mr-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

// mergeName constructs the name of the output file of reduce task <reduceTask>
func mergeName(reduceTask int) string {
	return "mr-out-" + strconv.Itoa(reduceTask)
}