package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"


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
	client, err := rpc.DialHTTP("tcp", "localhost:1234")
	//client, err := rpc.DialHTTP("unix", sockname)
	if err!=nil{
		panic(err.Error())
	}
	requst := Request{
		Id: 1,
	}
	var response *Reply
	//response :=&Reply{id: 0,fileName: ""}
	response = new(Reply)

	err = client.Call("Master.Process", requst, &response)
	if err !=nil {
		panic(err.Error())
	}

	fmt.Println(response.Id)
	fmt.Println(response.FileName)
	fmt.Println("connected")

	if response.FileName=="" {
		return
	}

	//接下来应该是读取文件，然后调用map函数
	//这是什么数据结构，空数组？
	//intermediate := []KeyValue{}
	file, err := os.Open(response.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", response.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", response.FileName)
	}
	file.Close()
	kva := mapf(response.FileName, string(content))
	//现在有了数据了
	//intermediate = append(intermediate, kva...)
	//现在要把中间文件写入到json当中
	//jsonFile, err := os.Create("./test.json")
	//os.OpenFile("./test.json",os.O_APPEND|os.O_CREATE,os.ModeAppend|os.ModePerm)
	//enc := json.NewEncoder(jsonFile)
	//for _, kv := range kva {
	//	err := enc.Encode(&kv)
	//	if err!=nil {
	//		panic(err.Error())
	//	}
	//}
	//
	//jsonFile.Close()
	//
	for _, kv := range kva {
		fmt.Println(kv.Key)
		fmt.Println(kv.Value)
		Y := strconv.Itoa(ihash(kv.Key)%response.ReduceNum)
		jsonName := "./datas/mr-X-" +Y+".json"
		jsonFile, _ := os.OpenFile(jsonName, os.O_APPEND|os.O_CREATE|os.O_RDWR, os.ModeAppend|os.ModePerm)
		enc := json.NewEncoder(jsonFile)
		err = enc.Encode(&kv)
		if err != nil {
			log.Fatalf("json encode fail:",err.Error())
		}
		jsonFile.Close()
	}



	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
