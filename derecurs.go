package derecurs

import (
	"sync"
	"sync/atomic"
	"time"
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
func NewDerecurs(forkFn ForkFn, mergeFn MergeFn, forkTimeOut time.Duration) *Derecurs {
	return &Derecurs{
		forkFn:                   forkFn,
		mergeFn:                  mergeFn,
		queue:                    newFnQueue(10000),
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
		d.wgAdd()
		go func() {
			defer d.wgDone()
			ipa, result := fn()
			if ipa != nil {
				d.fork(ipa)
			}
			d.result = d.mergeFn(d.result, result)
		}()
	}
	d.wg.Wait()
}

// StartPool starts in pool
func (d *Derecurs) StartPool(num int) {
	for i := 0; i < num; i++ {
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
					d.fork(ipa)
				}
				d.result = d.mergeFn(d.result, result)
			}
		}()
	}
	d.wg.Wait()
}

// Stop stops
func (d *Derecurs) Stop() {
	atomic.AddInt32(&d.stop, 1)
}

// GetResult gets result
func (d *Derecurs) GetResult() ComputedResult {
	return d.result
}

func (d *Derecurs) fork(ipa InputParametersArray) {
	for _, ip := range ipa {
		if d.stop > 0 {
			break
		}
		d.queue.enqueue(func() (InputParametersArray, ComputedResult) { return d.forkFn(ip) })
	}
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
	q.lock.Lock()
	defer q.lock.Unlock()
	q.slice = append(q.slice, fn)
}

func (q *fnQueue) dequeue() func() (InputParametersArray, ComputedResult) {
	if len(q.slice) > 0 {
		q.lock.Lock()
		defer q.lock.Unlock()
		fn := q.slice[0]
		q.slice = q.slice[1:]
		return fn
	}
	return nil
}

func (q *fnQueue) len() int {
	return len(q.slice)
}
