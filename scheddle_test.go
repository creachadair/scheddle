// Copyright (C) 2024 Michael J. Fromberger. All Rights Reserved.

package scheddle_test

import (
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/creachadair/scheddle"
	"github.com/creachadair/taskgroup"
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

	q := scheddle.NewQueue(&scheddle.Options{
		TimeNow: clk.Now,
	})
	defer q.Close()

	vals := make(chan int)
	push := func(n int) scheddle.Task {
		return scheddle.T(func() {
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

func TestQueue_run(t *testing.T) {
	q := scheddle.NewQueue(nil)

	g, start := taskgroup.New(nil).Limit(4)

	const numTasks = 20
	const tick = 10 * time.Millisecond

	done := make(chan struct{})
	q.After(numTasks*tick, scheddle.T(func() {
		t.Log("All tasks scheduled")
		close(done)
	}))
	defer q.Close()

	vals := make([]int, numTasks)
	for i := range vals {
		dur := rand.N(tick) - time.Millisecond
		q.After(dur, scheddle.T(func() {
			start(func() error {
				t.Logf("task %d (after %v)", i+1, dur)
				vals[i] = i + 1
				return nil
			})
		}))
	}

	<-done
	g.Wait()

	// Verify that all the tasks got their values in, i.e., that none of the
	// positions remained zero.
	var sum int
	for _, v := range vals {
		sum += v
	}
	if want := (numTasks * (numTasks + 1)) / 2; sum != want {
		t.Errorf("Checksum is %d, want %d", sum, want)
	}
}

func TestQueue_repeat(t *testing.T) {
	q := scheddle.NewQueue(nil)
	defer q.Close()

	var runs int
	q.After(10*time.Millisecond, &scheddle.Repeat{
		Task:  scheddle.T(func() { runs++ }),
		Every: 20 * time.Millisecond,
		Count: 5,
		End:   time.Now().Add(50 * time.Millisecond),
	})

	done := make(chan struct{})
	q.After(100*time.Millisecond, scheddle.T(func() {
		close(done)
	}))

	<-done

	if want := 3; runs != want {
		t.Errorf("Got %d runs, want %d", runs, want)
	}
}
