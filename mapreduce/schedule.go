package mapreduce

import (
	"fmt"
	"sync"
	"time"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}
	fmt.Println("inside Schedule::::")
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	var wg sync.WaitGroup
	for i := 0; i < ntasks; i++ {

		time.Sleep(2 * time.Second)
		wg.Add(1)
		args := DoTaskArgs{jobName, mapFiles[i], phase, i, n_other}
		//fmt.Println("worker id:", worker)
		go func(args DoTaskArgs, registerChan chan string) {
			defer wg.Done()
			for {
				worker := <-registerChan
				ok := call(worker, "Worker.DoTask", args, nil)
				if ok {
					go func() {
						fmt.Println("inside go:", worker)
						registerChan <- worker
					}()
					break
				}
			}
		}(args, registerChan)
		fmt.Println("Running:", i)
	}

	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
