package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"
type ByKey []KeyValue
// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {

	Key   string `json:"key"`
	Value string  `json:"val"`
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	//启动RPC向master进行注册
	//sockname := masterSock()
	//client, err := rpc.DialHTTP("tcp", "localhost:8088")
	//client, err := rpc.DialHTTP("unix", sockname)
	//if err!=nil{
	//	panic(err.Error())
	//}
	rand.Seed(time.Now().UnixNano())
	for  {

		requst := Request{
			Id: rand.Intn(100000000),
			STATUS_CODE: STATUS_REQUEST_WORK,
		}
		var response *Reply
		//response :=&Reply{id: 0,fileName: ""}
		response = new(Reply)

		call("Master.Process", requst, &response)
		//err = client.Call("Master.Process", requst, &response)
		log.Println(response)

		if response.STATUS_CODE==STATUS_DO_MAP {
			//	执行map任务
			doMapWork(response,mapf)
		} else if response.STATUS_CODE==STATUS_DO_REDUCE {
			//	执行reduce任务
			doReduceWork(response,reducef)
		} else if response.STATUS_CODE==STATUS_END{
			//结束
			break
		}
	}

}

func doReduceWork(response *Reply,reducef func(string, []string) string){
	if response.FileName=="" {
		return
	}
	file, err := os.Open(response.FileName)
	if err!=nil {
		panic("wrong")
	}
	intermediate := []KeyValue{}
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		intermediate = append(intermediate, kv)
	}
	sort.Sort(ByKey(intermediate))
	work_index := response.Work_index
	ofile, _ := os.Create("./tmp-mr-out-"+strconv.Itoa(work_index))
	//ofile, _ := os.OpenFile("tmp-mr-out-"+string(work_index), os.O_APPEND|os.O_CREATE|os.O_RDWR, os.ModeAppend|os.ModePerm)

	//ofile, _ := ioutil.TempFile("","824-mr"+string(work_index))

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
	errs := os.Rename("./tmp-mr-out-"+strconv.Itoa(work_index), "./mr-out-"+strconv.Itoa(work_index))
	if errs !=nil{
		fmt.Println(err.Error())
		log.Fatalln("rename wrong")
	}
	//回送，表示完成reduce
	reduceFinishRequest  :=Request{STATUS_CODE: STATUS_REDUCE_FINISH,Id: response.Id}
	useLessResponse := Reply{}
	call("Master.Process", reduceFinishRequest, &useLessResponse)

}
func doMapWork(response *Reply,mapf func(string, string) []KeyValue){

	//接下来应该是读取文件，然后调用map函数
	//这是什么数据结构，空数组？
	//intermediate := []KeyValue{}
	file, err := os.Open(response.FileName)
	if err != nil {
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", response.FileName)
	}
	file.Close()
	kva := mapf(response.FileName, string(content))
	//work_index := response.Work_index
	for _, kv := range kva {
		Y := strconv.Itoa(ihash(kv.Key)%response.ReduceNum)
		//jsonName := "./datas/mr-" +strconv.Itoa(work_index)+"-"+Y+".json"
		jsonName := "./datas/mr-x-"+Y+".json"
		jsonFile, _ := os.OpenFile(jsonName, os.O_APPEND|os.O_CREATE|os.O_RDWR, os.ModeAppend|os.ModePerm)
		//jsonFile, _ := os.Create(jsonName)
		enc := json.NewEncoder(jsonFile)
		err = enc.Encode(&kv)
		if err != nil {
			log.Fatalf("json encode fail:",err.Error())
		}
		jsonFile.Close()
	}

	//回送，表示完成
	mapfinishRequest  :=Request{STATUS_CODE: STATUS_MAP_FINISH,Id: response.Id}
	useLessResponse := Reply{}
	call("Master.Process", mapfinishRequest, &useLessResponse)

}


//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
