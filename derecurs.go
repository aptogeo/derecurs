package derecurs

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultPoolSize is default pool size
	DefaultPoolSize = 8
	// DefaultCapacity is default capacity
	DefaultCapacity = 10000
)

// InputParameters type
type InputParameters interface{}

// InputParametersArray type
type InputParametersArray []InputParameters

// ComputedResult type
type ComputedResult interface{}

// ForkFn type
type ForkFn func(in InputParameters) (InputParametersArray, ComputedResult)

// MergeFn type
type MergeFn func(previous ComputedResult, current ComputedResult) ComputedResult

// Derecurs type
type Derecurs struct {
	result                   ComputedResult
	queue                    *Queue
	forkFn                   ForkFn
	mergeFn                  MergeFn
	ForkTimeOut              time.Duration
	StopIfForkTimeOutElapsed bool
	stop                     int32
	wg                       sync.WaitGroup
}

// NewDerecurs creates new derecurs
func NewDerecurs(forkFn ForkFn, mergeFn MergeFn) *Derecurs {
	return &Derecurs{
		forkFn:                   forkFn,
		mergeFn:                  mergeFn,
		queue:                    NewQueue(DefaultCapacity),
		ForkTimeOut:              100 * time.Millisecond,
		StopIfForkTimeOutElapsed: true,
	}
}

// Add adds input parameters
func (d *Derecurs) Add(ip InputParameters) {
	d.queue.Enqueue([]InputParameters{ip})
}

// AddArray adds input parameters array
func (d *Derecurs) AddArray(ipa InputParametersArray) {
	d.queue.Enqueue(ipa)
}

// Start starts
func (d *Derecurs) Start() {
	d.StartPool(DefaultPoolSize)
}

// StartPool starts in pool
func (d *Derecurs) StartPool(size int) {
	d.wgAdd()
	d.stop = 0
	var lock sync.Mutex
	go func() {
		defer d.wgDone()
		for i := 0; i < size; i++ {
			d.wgAdd()
			go func() {
				defer d.wgDone()
				for {
					if d.stop > 0 {
						break
					}
					ip := d.queue.Dequeue()
					if ip == nil {
						time.Sleep(d.ForkTimeOut)
						ip = d.queue.Dequeue()
						if ip == nil {
							if d.StopIfForkTimeOutElapsed {
								atomic.AddInt32(&d.stop, 1)
								break
							} else {
								continue
							}
						}
					}
					ipa, result := d.forkFn(ip)
					if ipa != nil {
						d.queue.Enqueue(ipa)
					}
					lock.Lock()
					d.result = d.mergeFn(d.result, result)
					lock.Unlock()
				}
			}()
		}
	}()
}

// Stop stops
func (d *Derecurs) Stop() {
	atomic.AddInt32(&d.stop, 1)
}

// Reset resets
func (d *Derecurs) Reset() {
	d.Stop()
	d.wg.Wait()
	d.queue = NewQueue(DefaultCapacity)
	d.result = nil
}

// WaitResult waits end and gets result
func (d *Derecurs) WaitResult() ComputedResult {
	d.wg.Wait()
	return d.result
}

func (d *Derecurs) wgAdd() {
	d.wg.Add(1)
}

func (d *Derecurs) wgDone() {
	d.wg.Done()
}

// Queue strut
type Queue struct {
	slice []InputParameters
	lock  sync.Mutex
}

// NewQueue creates new queue
func NewQueue(capacity int) *Queue {
	return &Queue{
		slice: make([]InputParameters, 0, capacity),
	}
}

// Enqueue enqueues
func (q *Queue) Enqueue(ipa InputParametersArray) {
	defer q.lock.Unlock()
	q.lock.Lock()
	q.slice = append(q.slice, ipa...)
}

// Dequeue dequeues
func (q *Queue) Dequeue() InputParameters {
	defer q.lock.Unlock()
	q.lock.Lock()
	if len(q.slice) > 0 {
		fn := q.slice[0]
		q.slice = q.slice[1:]
		return fn
	}
	return nil
}

// Len gets len
func (q *Queue) Len() int {
	defer q.lock.Unlock()
	q.lock.Lock()
	return len(q.slice)
}
