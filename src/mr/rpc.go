package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}
type Request struct {


	Id int
	STATUS_CODE int
	work_index int

}

const STATUS_REQUEST_WORK=1
const STATUS_MAP_FINISH =2
const STATUS_REDUCE_FINISH =3
const STATUS_DO_REDUCE =5
const STATUS_DO_MAP =4
const STATUS_END =6


type Reply struct {
	Id int
	FileName  string
	ReduceNum int
	STATUS_CODE int
	Work_index int
}
type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
