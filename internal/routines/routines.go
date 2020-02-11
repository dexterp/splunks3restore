package routines

import (
	"fmt"
	"sync"
)

type ActionFuncBatch func(*Id, []interface{})
type ActionFunc func(*Id, interface{})

// Identifies go routine number
type Id struct {
	Name string // Type of routine
	Num  uint   // Go routine number
}

// Routines manages worker pools. It provides a framework to start go routine pools, push objects unto a queue,
// wait for channels to drain, wait for routines to exit and flush any objects that have been batched.
type Routines struct {
	name      string             // Add a Type to routine
	addJobMU  *sync.Mutex        // Synchronize AddJob, Flush & Close routines
	killMU    *sync.Mutex        // Kill mutex
	flushMU   *sync.Mutex        // Ensures the Flush call is thread safe
	flushWG   *sync.WaitGroup    // WaitGroup to determine when all routines have flushed all jobs that have been queued
	wrkrWG    *sync.WaitGroup    // WaitGroup to notify when all workers have exited upon channel close
	chanWG    *sync.WaitGroup    // WaitGroup to notify when channel has drained
	startOnce *sync.Once         // Start routine once
	closeOnce *sync.Once         // Close channel once
	closed    bool               // Channel is closed if true
	kill      bool               // Force the program not to process any more items
	batchlen  uint               // Size of batch to buffer before sending to ActionFuncBatch or ActionFunc
	count     uint               // Number of concurrent routines to run
	ch        chan []interface{} // Communication channel. The array length is controlled by batchlen
	chFlush   []chan interface{} // One channel per go routine to notify each routine to flush its batch objects
}

// Starts a new named routine pool
//
// count sets the number of go routines to run
// batchlen limits the batch length, which is the length of the array passed to the ActionFuncBatch function.
// chanlen sets the size of the internal buffered channel
func New(name string, count, batchlen uint, chanlength int) *Routines {
	s := &Routines{
		name:      name,
		addJobMU:  &sync.Mutex{},
		killMU:    &sync.Mutex{},
		flushMU:   &sync.Mutex{},
		flushWG:   &sync.WaitGroup{},
		wrkrWG:    &sync.WaitGroup{},
		chanWG:    &sync.WaitGroup{},
		startOnce: &sync.Once{},
		closeOnce: &sync.Once{},
		batchlen:  batchlen,
		count:     count,
		ch:        make(chan []interface{}, chanlength),
		chFlush:   []chan interface{}{},
	}
	return s
}

// Start starts goroutines using ActionFunc or ActionFuncBatch as the callback.
func (r *Routines) Start(action interface{}) error {
	batchFn, err := castAction(r.name, action)
	if err != nil {
		return err
	}
	r.startOnce.Do(func() {
		for i := uint(0); i < r.count; i++ {
			id := &Id{
				Name: r.name,
				Num:  i,
			}
			flush := make(chan interface{}, 2)
			r.chFlush = append(r.chFlush, flush)
			go start(r, id, flush, batchFn)
		}
	})
	return nil
}

func castAction(name string, action interface{}) (ActionFuncBatch, error) {
	var singleFn ActionFunc
	var batchFn ActionFuncBatch
	switch v := action.(type) {
	case ActionFunc:
		singleFn = v
		batchFn = func(id *Id, buf []interface{}) {
			for _, b := range buf {
				singleFn(id, b)
			}
		}
	case func(*Id, interface{}):
		singleFn = v
		batchFn = func(id *Id, buf []interface{}) {
			for _, b := range buf {
				singleFn(id, b)
			}
		}
	case ActionFuncBatch:
		batchFn = v
	case func(*Id, []interface{}):
		batchFn = v
	default:
		return nil, newError(name, "action function should be of type ActionFunc or ActionFuncBatch, got an unknown action type")
	}
	return batchFn, nil
}

func start(r *Routines, id *Id, flush <-chan interface{}, action ActionFuncBatch) {
	r.wrkrWG.Add(1)
	defer r.wrkrWG.Done()
	var batch []interface{}
	for {
		select {
		case input, ok := <-r.ch:
			switch {
			case !ok:
				// On channel close
				if !r.IsKilled() {
					cnt := len(batch)
					if cnt > 0 {
						action(id, batch)
						batch = nil
					}
				}
				return
			case r.batchlen > 1:
				// Process in batches
				if !r.IsKilled() {
					for _, item := range input {
						batch = append(batch, item)
						cnt := uint(len(batch))
						if cnt >= r.batchlen {
							action(id, batch)
							batch = nil
						}
					}
				}
			default:
				// Process individually
				if !r.IsKilled() {
					for _, item := range input {
						action(id, []interface{}{item})
					}
				}
			}
			cnt := len(input)
			if cnt > 0 {
				r.chanWG.Add(-cnt)
			}
		case <-flush:
			cnt := len(batch)
			if cnt > 0 {
				action(id, batch)
				batch = nil
			}
			r.flushWG.Add(-1)
			r.flushWG.Wait()
		}
	}
}

// AddJobs adds a list of jobs to the queue.
func (r *Routines) AddJob(input interface{}) error {
	r.addJobMU.Lock()
	defer r.addJobMU.Unlock()
	if r.closed {
		return newError(r.name, "can not add job to queue, input queue is closed")
	}
	if r.kill {
		return newError(r.name, "can not add job to queue, process has been killed")
	}
	var payload []interface{}
	if list, ok := input.([]interface{}); ok {
		payload = list
	} else {
		payload = []interface{}{input}
	}
	cnt := len(payload)
	r.chanWG.Add(cnt)
	r.ch <- payload
	return nil
}

// Flush informs workers to flush any batches that have been queued. Blocks until all routines have flushed.
func (r *Routines) Flush() {
	r.flushMU.Lock()
	r.addJobMU.Lock()
	defer func() {
		r.flushMU.Unlock()
		r.addJobMU.Unlock()
	}()
	r.WaitChan()
	r.flushWG.Add(int(r.count))
	for _, ch := range r.chFlush {
		ch <- nil
	}
	r.flushWG.Wait()
}

// Close informs all go routines to shutdown
//
// Close will block until all items that have been queued or batched have been processed by ActionFunc or ActionFuncBatch.
// ActionFunc & ActionFuncBatch are passed as arguments to Start
func (r *Routines) Close() {
	r.closeOnce.Do(func() {
		r.addJobMU.Lock()
		defer r.addJobMU.Unlock()
		close(r.ch)
		r.Wait()
		r.closed = true
	})
}

// Kill will force all routines to stop without process the queued or batched jobs.
//
// when force is true, this will force an immediate shutdown instead of shutting down gracefully
func (r *Routines) Kill(force bool) {
	r.killMU.Lock()
	defer r.killMU.Unlock()
	r.kill = true
}

func (r *Routines) IsKilled() bool {
	return r.kill
}

// WaitChan waits for channel to drain
func (r *Routines) WaitChan() {
	r.chanWG.Wait()
}

// Wait waits for all go routines to shutdown. Shutdown is triggered by calling Close
func (r *Routines) Wait() {
	r.wrkrWG.Wait()
}

type Error struct {
	name string
	msg  string
	error
}

func newError(name, msg string) *Error {
	return &Error{
		name: name,
		msg:  msg,
	}
}

func (e *Error) Error() string {
	return fmt.Sprintf("routines:%s: %s", e.name, e.msg)
}
