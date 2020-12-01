package derecurs

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultPoolSize is default pool size
	DefaultPoolSize = 8
	// DefaultCapacity is default capacity
	DefaultCapacity = 10000
	// DefaultForkTimeOut is default fork timeout
	DefaultForkTimeOut = 100 * time.Millisecond
)

// InputParameters type
type InputParameters interface{}

// InputParametersArray type
type InputParametersArray []InputParameters

// Data type
type Data interface{}

// ForkFn type
type ForkFn func(in InputParameters) (InputParametersArray, Data)

// MergeFn type
type MergeFn func(previous Data, current Data) Data

// Derecurs type
type Derecurs struct {
	data                     Data
	queue                    *Queue
	forkFn                   ForkFn
	mergeFn                  MergeFn
	StopAfterTime            time.Duration
	ForkTimeOut              time.Duration
	StopIfForkTimeOutElapsed bool
	Random                   bool
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
		Random:                   false,
	}
}

// SetInitialData sets initial data
func (d *Derecurs) SetInitialData(data Data) {
	d.data = data
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
					if d.StopAfterTime > 0 {
						if time.Now().Sub(startTime) > d.StopAfterTime {
							atomic.AddInt32(&d.stop, 1)
						}
					}
					if d.stop > 0 {
						break
					}
					forkStartTime := time.Now()
					var ip InputParameters
					if d.Random {
						ip = d.queue.RandomDequeue()
					} else {
						ip = d.queue.Dequeue()
					}
					for ip == nil {
						time.Sleep(d.ForkTimeOut / 10)
						ip = d.queue.Dequeue()
						if ip == nil {
							if time.Now().Sub(forkStartTime) > d.ForkTimeOut {
								if d.StopIfForkTimeOutElapsed {
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

// Queue strut
type Queue struct {
	slice []InputParameters
	lock  sync.Mutex
	rand  *rand.Rand
}

// NewQueue creates new queue
func NewQueue(capacity int) *Queue {
	return &Queue{
		slice: make([]InputParameters, 0, capacity),
		rand:  rand.New(rand.NewSource(time.Now().UnixNano())),
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
		v := q.slice[0]
		q.slice = q.slice[1:]
		return v
	}
	return nil
}

// RandomDequeue random dequeues (do not peserve queue order)
func (q *Queue) RandomDequeue() InputParameters {
	defer q.lock.Unlock()
	q.lock.Lock()
	l := len(q.slice)
	if l > 0 {
		i := rand.Intn(l)
		v := q.slice[i]
		l--
		q.slice[i] = q.slice[l]
		q.slice = q.slice[:l]
		return v
	}
	return nil
}

// Len gets len
func (q *Queue) Len() int {
	defer q.lock.Unlock()
	q.lock.Lock()
	return len(q.slice)
}

// Reset resets
func (q *Queue) Reset() {
	defer q.lock.Unlock()
	q.lock.Lock()
	q.slice = q.slice[:0]
}
