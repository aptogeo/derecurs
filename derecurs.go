package derecurs

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/aptogeo/queue"
)

const (
	// DefaultPoolSize is default pool size
	DefaultPoolSize = 8
	// DefaultForkTimeOut is default fork timeout
	DefaultForkTimeOut = 100 * time.Millisecond
)

// InputParametersArray type
type InputParametersArray []interface{}

// Data type
type Data interface{}

// ForkFn type
type ForkFn func(inputParameters interface{}) (InputParametersArray, Data)

// MergeFn type
type MergeFn func(previous Data, current Data) Data

// StopFn type
type StopFn func(current Data) bool

// Derecurs type
type Derecurs struct {
	data                     Data
	queue                    *queue.Queue
	forkFn                   ForkFn
	mergeFn                  MergeFn
	stopFn                   StopFn
	stopAfterTime            time.Duration
	forkTimeOut              time.Duration
	stopIfForkTimeOutElapsed bool
	method                   queue.Method
	stop                     int32
	wg                       sync.WaitGroup
}

// NewDerecurs creates new derecurs
func NewDerecurs(forkFn ForkFn, mergeFn MergeFn) *Derecurs {
	return &Derecurs{
		forkFn:                   forkFn,
		mergeFn:                  mergeFn,
		queue:                    queue.NewQueue(),
		forkTimeOut:              100 * time.Millisecond,
		stopIfForkTimeOutElapsed: true,
		method:                   queue.Fifo,
	}
}

// SetStopFn sets stopFn
func (d *Derecurs) SetStopFn(stopFn StopFn) {
	d.stopFn = stopFn
}

// SetInitialData sets initial data
func (d *Derecurs) SetInitialData(data Data) {
	d.data = data
}

// SetStopAfterTime sets stopAfterTime
func (d *Derecurs) SetStopAfterTime(stopAfterTime time.Duration) {
	d.stopAfterTime = stopAfterTime
}

// SetForkTimeOut sets forkTimeOut
func (d *Derecurs) SetForkTimeOut(forkTimeOut time.Duration) {
	d.forkTimeOut = forkTimeOut
}

// SetStopIfForkTimeOutElapsed sets stopIfForkTimeOutElapsed
func (d *Derecurs) SetStopIfForkTimeOutElapsed(stopIfForkTimeOutElapsed bool) {
	d.stopIfForkTimeOutElapsed = stopIfForkTimeOutElapsed
}

// SetMethod sets method
func (d *Derecurs) SetMethod(method queue.Method) {
	d.queue.SetMethod(method)
}

// SetSortFn sets sort less function
func (d *Derecurs) SetSortFn(sortFn queue.SortFn) {
	d.queue.SetSortFn(sortFn)
}

// Add adds input parameters
func (d *Derecurs) Add(inputParameters interface{}) {
	d.queue.Enqueue([]interface{}{inputParameters})
}

// AddArray adds input parameters array
func (d *Derecurs) AddArray(inputParametersArray InputParametersArray) {
	d.queue.Enqueue(inputParametersArray)
}

// Start starts
func (d *Derecurs) Start() {
	d.StartPool(DefaultPoolSize)
}

// StartPool starts in pool
func (d *Derecurs) StartPool(size int) {
	startTime := time.Now()
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
					if d.stopAfterTime > 0 {
						if time.Now().Sub(startTime) > d.stopAfterTime {
							atomic.AddInt32(&d.stop, 1)
						}
					}
					if d.stop > 0 {
						break
					}
					forkStartTime := time.Now()
					ip := d.queue.Dequeue()
					for ip == nil {
						time.Sleep(d.forkTimeOut / 10)
						ip = d.queue.Dequeue()
						if ip == nil {
							if time.Now().Sub(forkStartTime) > d.forkTimeOut {
								if d.stopIfForkTimeOutElapsed {
									atomic.AddInt32(&d.stop, 1)
								}
								break
							}
						}
					}
					if ip == nil {
						continue
					}
					ipa, data := d.forkFn(ip)
					if ipa != nil {
						d.queue.Enqueue(ipa)
					}
					lock.Lock()
					d.data = d.mergeFn(d.data, data)
					if d.stopFn != nil && d.stopFn(d.data) {
						atomic.AddInt32(&d.stop, 1)
					}
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
	d.queue.Reset()
	d.data = nil
}

// Wait waits end and gets result Data
func (d *Derecurs) Wait() Data {
	d.wg.Wait()
	return d.data
}

func (d *Derecurs) wgAdd() {
	d.wg.Add(1)
}

func (d *Derecurs) wgDone() {
	d.wg.Done()
}
