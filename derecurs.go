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
	queue                    *fnQueue
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
		queue:                    newFnQueue(DefaultCapacity),
		ForkTimeOut:              100 * time.Millisecond,
		StopIfForkTimeOutElapsed: true,
	}
}

// Add adds input parameters
func (d *Derecurs) Add(ip InputParameters) {
	d.queue.enqueue(func() (InputParametersArray, ComputedResult) { return d.forkFn(ip) })
}

// AddArray adds input parameters array
func (d *Derecurs) AddArray(ipa InputParametersArray) {
	for _, ip := range ipa {
		d.queue.enqueue(func() (InputParametersArray, ComputedResult) { return d.forkFn(ip) })
	}
}

// Start starts
func (d *Derecurs) Start() {
	d.StartPool(DefaultPoolSize)
}

// StartPool starts in pool
func (d *Derecurs) StartPool(size int) {
	d.wgAdd()
	d.stop = 0
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
					fn := d.queue.dequeue()
					if fn == nil {
						time.Sleep(d.ForkTimeOut)
						fn = d.queue.dequeue()
						if fn == nil {
							if d.StopIfForkTimeOutElapsed {
								atomic.AddInt32(&d.stop, 1)
								break
							} else {
								continue
							}
						}
					}
					ipa, result := fn()
					if ipa != nil {
						for _, ip := range ipa {
							d.queue.enqueue(func() (InputParametersArray, ComputedResult) { return d.forkFn(ip) })
						}
					}
					d.result = d.mergeFn(d.result, result)
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
	d.queue = newFnQueue(DefaultCapacity)
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

type fnQueue struct {
	slice [](func() (InputParametersArray, ComputedResult))
	lock  sync.Mutex
}

func newFnQueue(capacity int) *fnQueue {
	return &fnQueue{
		slice: make([](func() (InputParametersArray, ComputedResult)), 0, capacity),
	}
}

func (q *fnQueue) enqueue(fn func() (InputParametersArray, ComputedResult)) {
	defer q.lock.Unlock()
	q.lock.Lock()
	q.slice = append(q.slice, fn)
}

func (q *fnQueue) dequeue() func() (InputParametersArray, ComputedResult) {
	defer q.lock.Unlock()
	q.lock.Lock()
	if len(q.slice) > 0 {
		fn := q.slice[0]
		q.slice = q.slice[1:]
		return fn
	}
	return nil
}

func (q *fnQueue) len() int {
	return len(q.slice)
}
