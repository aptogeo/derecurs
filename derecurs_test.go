package derecurs_test

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/aptogeo/derecurs"
	"github.com/aptogeo/queue"
	"github.com/stretchr/testify/assert"
)

var (
	maxRound int64 = 7
)

func TestDefault(t *testing.T) {
	startTime := time.Now()
	d := createAndStart(0)
	res := d.Wait()
	fmt.Fprintln(os.Stderr, fmt.Sprintf("derecurs result: %v in %v ms", res.(*result).nb, time.Now().Sub(startTime).Milliseconds()))
	assert.Equal(t, res.(*result).nb, int64(2396745))
}

func TestPoolSize1(t *testing.T) {
	startTime := time.Now()
	d := createAndStart(1)
	res := d.Wait()
	fmt.Fprintln(os.Stderr, fmt.Sprintf("derecurs result: %v in %v ms", res.(*result).nb, time.Now().Sub(startTime).Milliseconds()))
	assert.Equal(t, res.(*result).nb, int64(2396745))
}

func TestPoolSize2(t *testing.T) {
	startTime := time.Now()
	d := createAndStart(2)
	res := d.Wait()
	fmt.Fprintln(os.Stderr, fmt.Sprintf("derecurs result: %v in %v ms", res.(*result).nb, time.Now().Sub(startTime).Milliseconds()))
	assert.Equal(t, res.(*result).nb, int64(2396745))
}

func TestPoolSize4(t *testing.T) {
	startTime := time.Now()
	d := createAndStart(4)
	res := d.Wait()
	fmt.Fprintln(os.Stderr, fmt.Sprintf("derecurs result: %v in %v ms", res.(*result).nb, time.Now().Sub(startTime).Milliseconds()))
	assert.Equal(t, res.(*result).nb, int64(2396745))
}

func TestWithPause(t *testing.T) {
	startTime := time.Now()
	d := createAndStart(0)
	time.Sleep(100 * time.Millisecond)
	d.Stop()
	res := d.Wait()
	fmt.Fprintln(os.Stderr, fmt.Sprintf("derecurs intermediate result: %v in %v ms", res.(*result).nb, time.Now().Sub(startTime).Milliseconds()))
	d.Start()
	res = d.Wait()
	fmt.Fprintln(os.Stderr, fmt.Sprintf("derecurs final: %v in %v ms", res.(*result).nb, time.Now().Sub(startTime).Milliseconds()))
	assert.Equal(t, res.(*result).nb, int64(2396745))
}

func TestStopAfter(t *testing.T) {
	startTime := time.Now()
	d := createAndStart(0)
	d.SetStopAfterTime(10 * time.Millisecond)
	res := d.Wait()
	fmt.Fprintln(os.Stderr, fmt.Sprintf("derecurs stop after result: %v in %v ms", res.(*result).nb, time.Now().Sub(startTime).Milliseconds()))
	assert.NotEqual(t, res.(*result).nb, int64(2396745))
}

func TestRandom(t *testing.T) {
	startTime := time.Now()
	d := createAndStart(0)
	d.SetMethod(queue.Random)
	res := d.Wait()
	fmt.Fprintln(os.Stderr, fmt.Sprintf("derecurs random result: %v in %v ms", res.(*result).nb, time.Now().Sub(startTime).Milliseconds()))
	assert.Equal(t, res.(*result).nb, int64(2396745))
}

func TestSort(t *testing.T) {
	startTime := time.Now()
	d := createAndStart(0)
	d.SetMethod(queue.Sort)
	d.SetSortFn(func(left, right interface{}) bool {
		return left.(*input).round < right.(*input).round
	})
	res := d.Wait()
	fmt.Fprintln(os.Stderr, fmt.Sprintf("derecurs sort result: %v in %v ms", res.(*result).nb, time.Now().Sub(startTime).Milliseconds()))
	assert.Equal(t, res.(*result).nb, int64(2396745))
}

func createAndStart(size int) *derecurs.Derecurs {
	d := derecurs.NewDerecurs(
		func(in interface{}) (derecurs.InputParametersArray, derecurs.Data) {
			ins := make(derecurs.InputParametersArray, 0, 8)
			round := in.(*input).round
			if round <= maxRound {
				for i := 0; i < 8; i++ {
					ins = append(ins, &input{round + 1})
				}
			}
			return ins, calc(round)
		},
		func(previous derecurs.Data, current derecurs.Data) derecurs.Data {
			if previous == nil {
				return current
			}
			return &result{previous.(*result).nb + current.(*result).nb, previous.(*result).dur + current.(*result).dur}
		},
	)
	d.Add(&input{1})
	if size <= 0 {
		runtime.GOMAXPROCS(derecurs.DefaultPoolSize)
		d.Start()
		fmt.Fprintln(os.Stderr, fmt.Sprintf("derecurs started with default pool size %v", derecurs.DefaultPoolSize))
	} else {
		runtime.GOMAXPROCS(size)
		d.StartPool(size)
		fmt.Fprintln(os.Stderr, fmt.Sprintf("derecurs started with pool size %v", size))
	}
	return d
}

func TestRecursive(t *testing.T) {
	startTime := time.Now()
	res := recursiveCalc(&input{1})
	fmt.Fprintln(os.Stderr, fmt.Sprintf("recursive: %v in %v ms", res.nb, time.Now().Sub(startTime).Milliseconds()))
	assert.Equal(t, res.nb, int64(2396745))
}

func recursiveCalc(in *input) *result {
	res := calc(in.round)
	if in.round <= maxRound {
		for i := 0; i < 8; i++ {
			res2 := recursiveCalc(&input{in.round + 1})
			res = &result{res.nb + res2.nb, res.dur + res2.dur}
		}
	}
	return res
}

type input struct {
	round int64
}

type result struct {
	nb  int64
	dur int64
}

func calc(val int64) *result {
	startTime := time.Now()
	var m int64 = 100
	var i int64
	for i = 0; i < m; i++ {
		math.Cos(float64(val+i) / float64(2*10))
	}
	d := int64(time.Now().Sub(startTime))
	return &result{1, d}
}
