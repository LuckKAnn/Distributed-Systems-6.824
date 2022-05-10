package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"time"
)
import "net"
import "net/rpc"
import "net/http"
import "sync"


type Master struct {
	// Your definitions here.
	files_deal []bool
	files []string
	reduceNum int
    mutex  sync.Mutex
	cached map[int]bool
	countMap int
	waitend sync.WaitGroup
	countReduce int
}
func init()  {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

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
/**
如何去判断是否超时呢？
这个任务应该是在服务端来做的，要具备得有超时重试的功能
 */
func (m *Master) Process(args Request, reply *Reply) error {

	/**
	 表示请求任务
	 */
	if args.STATUS_CODE==STATUS_REQUEST_WORK {
		//countMap初始的数量就是文件的数量
		m.mutex.Lock()

		if m.countMap!=0 {
			//log.Printf("接收到Map请求，当前的map任务数量:%d \n",m.countMap)
			reply.FileName = ""
			for index,files := range m.files{
				if !m.files_deal[index] {
					//选择这个文件给worker进行处理
					reply.FileName = files
					m.files_deal[index] = true
					//根据请求的ID，缓存未完成的任务
					m.cached[args.Id] = false
					reply.Work_index = index
					//校验超时
					go m.checkIfWorkerFail(args.Id,index,1)
					break
				}
			}
			//如果返回的filename为空，说明map任务分配完了
			reply.Id = args.Id
			reply.ReduceNum = m.reduceNum
			reply.STATUS_CODE=STATUS_DO_MAP
			//如何判断map任务都执行完成了，应该执行reduce任务了
		}else if m.countMap==0 {
			//如果map任务完成了
			//log.Printf("接收到reduce请求，当前的reduce任务数量:%d \n",m.reduceNum)
			if m.reduceNum!=0 {
				//应该执行reduce任务
				m.waitend.Add(1)
				reply.FileName = ""
				for index,files := range m.files{
					if !m.files_deal[index] {
						reply.FileName = files
						m.files_deal[index] = true
						m.cached[args.Id] = false
						reply.Work_index = index
						go m.checkIfWorkerFail(args.Id,index,2)
						break
					}
				}
				reply.Id=args.Id
				reply.STATUS_CODE=STATUS_DO_REDUCE
			}else {
				//表示结束
				reply.STATUS_CODE=STATUS_END
			}

		}
		m.mutex.Unlock()

	} else  if args.STATUS_CODE==STATUS_MAP_FINISH {
		//log.Println("接收到MAP_FINIS报文")
		//表示某个map任务完成
		//某个map任务执行完成
		m.mutex.Lock()
		//先判断这是不是过期的任务的消息
		_,ok := m.cached[args.Id]
		if ok {
			m.cached[args.Id] = true
			m.countMap--
			//获取创建的reduce任务
			if m.countMap==0{
				//	更新阶段，也就是更新文件
				var s[]string
				files, err := m.GetAllFile("./datas/", s)
				if err != nil {
					panic("wrong")
				}
				m.files = files
				m.files_deal = make([]bool, len(files))
				m.cached = make(map[int]bool)
				m.reduceNum= len(files)
				m.waitend.Add(len(files))
			}
		}
		m.mutex.Unlock()
	} else if args.STATUS_CODE==STATUS_REDUCE_FINISH{
		//log.Println("接收到REDUCE_FINIS报文")
		m.mutex.Lock()
		_,ok := m.cached[args.Id]
		if ok{
			m.reduceNum--
			m.cached[args.Id] = true
			m.waitend.Done()
			if m.reduceNum==0{
				reply.STATUS_CODE=STATUS_END
			}
		} else {
			log.Println("接收到超时的消息")
		}
		m.mutex.Unlock()

	}



	return nil
}
func (m *Master) checkIfWorkerFail(id int ,index int,flag int){

	/*睡眠十秒*/
	time.Sleep(time.Second*10)
	//如果还没有完成任务，那么任务应该重新分配
	b,ok := m.cached[id]
	if ok {
	//	存在，不做任何处理
		if !b {

			if flag==1 {
				log.Printf("位于%d的消息超时,是map超时",index)
			}else{
				log.Printf("位于%d的消息超时,是reduce超时",index)
			}
		//	还没有完成，那么删除这个key
			delete(m.cached,id)
			//	说明这个任务当前还没有完成，应该把它设置为允许再分配
			m.mutex.Lock()
			m.files_deal[index]=false
			m.mutex.Unlock()
		}
	}

}
func (m *Master)GetAllFile(pathname string, s []string) ([]string, error) {
	rd, err := ioutil.ReadDir(pathname)
	if err != nil {
		fmt.Println("read dir fail:", err)
		return s, err
	}

	for _, fi := range rd {
		if !fi.IsDir() {
			fullName := pathname + "/" + fi.Name()
			s = append(s, fullName)
		}
	}
	return s, nil
}
func (m *Master)deleteJsonFiles(pathname string) ( error) {
	dir, err := ioutil.ReadDir(pathname)
	for _, d := range dir {
		os.RemoveAll(path.Join([]string{pathname, d.Name()}...))
	}
	return err
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":8088")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
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

	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.reduceNum==0 {
		err := m.deleteJsonFiles("./datas")
		if err!=nil {
			log.Println("delete json file fail")
		}
		return true;
	}else {
		return false;
	}

}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	var deal []bool = make([]bool, len(files))

	m := Master{
		files_deal: deal,
		files: files,
		reduceNum: nReduce,
	}
	m.countMap = len(files)
	m.cached =make(map[int]bool)

	//为了完成这个任务应该做什么: client向服务端注册，拿到任务，服务端给任务给worker，worker完成之后回送
	//如果没有在规定的时间完成,那么应该取消分配给worker的任务,然后分发给其他worker
	//如何给worker分配任务呢?如果worker不够用，是不是还需要重复利用同一个worker》？
	//worker可能接收到的是map，也可能是reduce，如果是map，完成之后会生成中间文件，需要返回给master，master再去分发


	/**
	既然要分配任务，那么应该怎么分配呢
	 */
	// Your code here.

	m.server()
	return &m
}
