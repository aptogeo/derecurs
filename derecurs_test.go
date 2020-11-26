package derecurs_test

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aptogeo/derecurs"
)

var (
	maxRound int64 = 7
)

func Test(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	var startTime time.Time

	startTime = time.Now()
	d := derecurs.NewDerecurs(
		func(in derecurs.InputParameters) (derecurs.InputParametersArray, derecurs.ComputedResult) {
			ins := make(derecurs.InputParametersArray, 0, 8)
			round := in.(int64)
			if round <= maxRound {
				for i := 0; i < 8; i++ {
					ins = append(ins, round+1)
				}
			}
			return ins, calc(round)
		},
		func(previous derecurs.ComputedResult, current derecurs.ComputedResult) derecurs.ComputedResult {
			if previous == nil {
				return current
			}
			return previous.(*res).addAtomic(current.(*res))
		},
	)
	d.Add(int64(1))
	d.Start()
	fmt.Fprintln(os.Stderr, fmt.Sprintf("derecurs %v in %v ms", d.GetResult().(*res).nb, time.Now().Sub(startTime).Milliseconds()))

	startTime = time.Now()
	d = derecurs.NewDerecurs(
		func(in derecurs.InputParameters) (derecurs.InputParametersArray, derecurs.ComputedResult) {
			ins := make(derecurs.InputParametersArray, 0, 8)
			round := in.(int64)
			if round <= maxRound {
				for i := 0; i < 8; i++ {
					ins = append(ins, round+1)
				}
			}
			return ins, calc(round)
		},
		func(previous derecurs.ComputedResult, current derecurs.ComputedResult) derecurs.ComputedResult {
			if previous == nil {
				return current
			}
			return previous.(*res).addAtomic(current.(*res))
		},
	)
	d.Add(int64(1))
	d.StartPool(runtime.NumCPU())
	fmt.Fprintln(os.Stderr, fmt.Sprintf("derecurs pool %v in %v ms", d.GetResult().(*res).nb, time.Now().Sub(startTime).Milliseconds()))
}

type res struct {
	nb  int64
	dur int64
}

func (r *res) addAtomic(a *res) *res {
	atomic.AddInt64(&r.nb, a.nb)
	atomic.AddInt64(&r.dur, a.dur)
	return r
}

func calc(val int64) *res {
	startTime := time.Now()
	var m int64 = 100
	var i int64
	for i = 0; i < m; i++ {
		math.Cos(float64(val+i) / float64(2*10))
	}
	d := int64(time.Now().Sub(startTime))
	return &res{1, d}
}
