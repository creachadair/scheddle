// Copyright (C) 2024 Michael J. Fromberger. All Rights Reserved.

package scheddle_test

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/creachadair/scheddle"
	"github.com/creachadair/taskgroup"
)

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
