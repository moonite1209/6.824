package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
)

type WorkerInfo struct {
	id       int
	status   string // idle working
	is_alive bool
}
type MapTask struct {
	filename string
}
type ReduceTask struct {
	filename string
}

type Master struct {
	// Your definitions here.
	workers      map[int]WorkerInfo
	map_tasks    []MapTask
	reduce_tasks []ReduceTask
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) KeepAlive(args *KeepAliveArgs, reply *KeepAliveReply) error {
	pid := args.pid
	status := args.status
	m.workers[pid] = WorkerInfo{id: pid, status: status, is_alive: true}
	reply.ack = true
	return nil
}

func (m *Master) ReqireTask(args *ReqireTaskArgs, reply *ReqireTaskReply) error {

}

func (m *Master) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {

}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false
	// Your code here.
	if len(m.map_tasks) == 0 && len(m.reduce_tasks) == 0 {
		ret = true
	}
	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	inputs := []string{}
	for i := range files {
		f, e := filepath.Glob(files[i])
		if e != nil {
			log.Fatal("file error:", e)
		}
		inputs = append(inputs, f...)
	}
	map_tasks := []MapTask{}
	for i := range inputs {
		map_tasks = append(map_tasks, MapTask{filename: inputs[i]})
	}
	m := Master{map_tasks: map_tasks}

	// Your code here.

	m.server()
	return &m
}
