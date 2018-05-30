package mapreduce

import (
	"fmt"
	"sync/atomic"
)

type scheduleJobs struct {
	dargs  *DoTaskArgs
	worker string
}

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var nother int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		nother = nReduce
	case reducePhase:
		ntasks = nReduce
		nother = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nother)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	// only one task for one worker

	// waiting for worker
	availChan := make(chan string, 2)
	jobQueue := make(chan *DoTaskArgs, 50)
	finish := make(chan struct{})

	var jobTracker int32
	// worker producer
	go func() {
		for {
			select {
			case worker := <-registerChan:
				availChan <- worker
			}
		}
	}()

	// start consumer
	atomic.StoreInt32(&jobTracker, 1)
	go func() {
		for atomic.LoadInt32(&jobTracker) > 0 {
			select {
			case args := <-availChan:
				go func(args string) {
					job := <-jobQueue
					ok := call(args, "Worker.DoTask", job, nil)
					if ok {
						atomic.AddInt32(&jobTracker, -1)
						availChan <- args
					} else {
						jobQueue <- job
					}
				}(args)
			}
		}
		finish <- struct{}{}
	}()

	// start producer
	atomic.StoreInt32(&jobTracker, 0)
	for i := 0; i < ntasks; i++ {
		go func(i int) {
			dta := &DoTaskArgs{
				JobName:       jobName,
				File:          mapFiles[i],
				Phase:         phase,
				TaskNumber:    i,
				NumOtherPhase: nother,
			}

			atomic.AddInt32(&jobTracker, 1)
			jobQueue <- dta
		}(i)
	}
	<-finish
	fmt.Printf("Schedule: %v done\n", phase)
}
