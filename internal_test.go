// Copyright (C) 2024 Michael J. Fromberger. All Rights Reserved.

package scheddle

import (
	"sync"
	"testing"
	"time"
)

type testClock struct {
	μ   sync.Mutex
	cur time.Time
}

func (t *testClock) Now() time.Time {
	t.μ.Lock()
	defer t.μ.Unlock()
	return t.cur

}

func (t *testClock) Advance(d time.Duration) {
	t.μ.Lock()
	defer t.μ.Unlock()
	t.cur = t.cur.Add(d)
}

func TestQueue_basic(t *testing.T) {
	const tick = 50 * time.Millisecond
	const longTick = 80 * time.Millisecond
	clk := &testClock{cur: time.Now()}

	q := NewQueue(nil)
	q.now = clk.Now
	defer q.Close()

	vals := make(chan int)
	push := func(n int) Task {
		return Run(func() {
			t.Logf("[task] push %d", n)
			vals <- n
		})
	}
	want := func(want int) {
		got, ok := <-vals
		if !ok {
			t.Errorf("Channel closed, want %d", want)
		} else if got != want {
			t.Errorf("Got value %d, want %d", got, want)
		}
	}
	noVal := func(d time.Duration) {
		select {
		case p := <-vals:
			t.Errorf("Unexpected value %d", p)
		case <-time.After(d):
			t.Log("no more values for now, OK")
		}
	}

	// Schedule 5 tasks at tick intervals:
	//    ^----^----^----^----^
	//    1    2    3    4    5
	//
	// Schedule them "out of order". Verify
	// Verify that they are done in time order below.
	for i := range []int{2, 0, 4, 1, 3} {
		q.After(time.Duration(i)*tick, push(i+1))
	}

	// The first task should be eligible immediately; thereafter we should
	// schedule the rest in order even if multiple become eligible.
	//
	//     0-------A-------B-------C  # wakeups
	//     ^----^----^----^----^      # scheduled
	//     1    2    3    4    5
	//
	// While this is going on, add tasks later in the schedule to ensure they do
	// not conflict.
	q.After(10*tick, push(10))

	clk.Advance(longTick) // A
	q.After(11*tick, push(11))
	want(1)
	want(2)

	noVal(2 * tick)

	clk.Advance(longTick) // B
	want(3)
	q.After(12*tick, push(12))
	want(4)

	noVal(2 * tick)

	clk.Advance(longTick) // C
	want(5)
	q.After(13*tick, push(13))
	q.After(14*tick, push(14))

	noVal(2 * tick)

	// A lot of time passes, all the additional tasks should now be eligible and
	// should run in schedule order.
	clk.Advance(time.Minute)
	for i := range 5 {
		want(i + 10)
	}
}
