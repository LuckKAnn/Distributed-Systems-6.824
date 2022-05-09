package mr

import (
	"fmt"
	"log"
	"time"
)
import "net"
import "net/rpc"
import "net/http"
import "sync"


type Master struct {
	// Your definitions here.
	files []string
	reduceNum int
    mutex  sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) Process(args Request, reply *Reply) error {
	fmt.Println("经过process处理")
	fmt.Println("请求id:",args.Id)
	m.mutex.Lock()
	reply.FileName = ""
	for index,files := range m.files{
		if files!="" {
			reply.FileName = files
			m.files[index] = ""
		}
	}
	m.mutex.Unlock()
	//如果返回的filename为空，说明map任务分配完了
	reply.Id = 200
	reply.ReduceNum = m.reduceNum
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	//sockname := masterSock()
	//os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
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
	ret := false

	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		files: files,
		reduceNum: nReduce,
	}

	//为了完成这个任务应该做什么: client向服务端注册，拿到任务，服务端给任务给worker，worker完成之后回送
	//如果没有在规定的时间完成,那么应该取消分配给worker的任务,然后分发给其他worker
	//如何给worker分配任务呢?如果worker不够用，是不是还需要重复利用同一个worker》？
	//worker可能接收到的是map，也可能是reduce，如果是map，完成之后会生成中间文件，需要返回给master，master再去分发


	/**
	既然要分配任务，那么应该怎么分配呢
	 */
	// Your code here.


	m.server()
	time.Sleep(100000000)
	return &m
}
