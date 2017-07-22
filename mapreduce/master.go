package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {

	jobChan := make(chan *DoJobArgs)
	workerChan := make(chan string)
	okChan := make(chan struct{})

	go func() {
		for i := 0; i < mr.nMap; i++ {
			job := &DoJobArgs{mr.file, Map, i, mr.nReduce}
			jobChan <- job
		}
	}()

	go func() {
		for job := range jobChan {
			worker := mr.selectWorker(workerChan)
			go mr.invokeWorker(worker, job, jobChan, workerChan, okChan)
		}
	}()

	mr.waitOkN(okChan, mr.nMap)

	go func() {
		for i := 0; i < mr.nReduce; i++ {
			job := &DoJobArgs{mr.file, Reduce, i, mr.nMap}
			jobChan <- job
		}
	}()

	mr.waitOkN(okChan, mr.nReduce)

	return mr.KillWorkers()
}
func (mr *MapReduce) selectWorker(workerChan chan string) string {
	var worker string
	select {
	case worker = <-mr.registerChannel:
		mr.Workers[worker] = &WorkerInfo{worker}
	case worker = <-workerChan:
	}
	return worker
}

func (mr *MapReduce) waitOkN(okChan chan struct{}, n int) {
	for i := 0; i < n; i++ {
		<-okChan
	}
}

func (mr *MapReduce) invokeWorker(worker string, job *DoJobArgs, jobChan chan *DoJobArgs, workerChan chan string, okChan chan struct{}) {
	defer func() {
		workerChan <- worker
	}()
	var replay DoJobReply;
	ok := call(worker, "Worker.DoJob", job, &replay)
	if !ok {
		DPrintf("call worker %s failured", worker)
		jobChan <- job
		return
	}
	okChan <- struct{}{}
}
