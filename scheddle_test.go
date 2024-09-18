// Copyright (C) 2024 Michael J. Fromberger. All Rights Reserved.

package scheddle_test

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/creachadair/scheddle"
	"github.com/creachadair/taskgroup"
	"github.com/fortytw2/leaktest"
)

func TestQueue_run(t *testing.T) {
	defer leaktest.Check(t)()

	q := scheddle.NewQueue(nil)
	defer q.Close()

	g, start := taskgroup.New(nil).Limit(4)

	const numTasks = 20
	const tick = 10 * time.Millisecond

	vals := make([]int, numTasks)
	for i := range vals {
		dur := rand.N(tick) - time.Millisecond
		q.After(dur, scheddle.Run(func() {
			start(func() error {
				t.Logf("task %d (after %v)", i+1, dur)
				vals[i] = i + 1
				return nil
			})
		}))
	}

	q.Wait(context.Background())
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

func TestQueue_Cancel(t *testing.T) {
	defer leaktest.Check(t)()

	q := scheddle.NewQueue(nil)
	defer q.Close()

	t.Run("Single", func(t *testing.T) {
		id := q.After(15*time.Second, scheddle.Run(func() {
			t.Error("This should not happen")
		}))
		t.Logf("Add task id=%v", id)
		if !q.Cancel(id) {
			t.Errorf("Cancel(%v): got false, want true", id)
		}
		if q.Cancel(id) {
			t.Errorf("Cancel(%v): got true, want false", id)
		}
		q.Wait(context.Background())
	})

	t.Run("Multiple", func(t *testing.T) {
		id1 := q.After(15*time.Second, scheddle.Run(func() {
			t.Error("This should not happen")
		}))
		t.Logf("Add task id=%v", id1)

		var id2 scheddle.ID
		id2 = q.After(20*time.Millisecond, scheddle.Run(func(ctx context.Context) {
			if got := scheddle.TaskID(ctx); got != id2 {
				t.Errorf("Task got ID %v, want %v", got, id2)
			} else {
				t.Logf("Task %d ran OK", got)
			}
		}))
		t.Logf("Add task id=%v", id2)

		if !q.Cancel(id1) {
			t.Errorf("Cancel(%v): got false, want true", id1)
		}

		q.Wait(context.Background())

		if q.Cancel(id2) {
			t.Errorf("Cancel(%v): got true, want false", id2)
		}
	})
}

func TestQueue_repeat(t *testing.T) {
	defer leaktest.Check(t)()

	q := scheddle.NewQueue(nil)
	defer q.Close()

	var runs int
	q.After(10*time.Millisecond, &scheddle.Repeat{
		Task:  scheddle.Run(func() { runs++ }),
		Every: 20 * time.Millisecond,
		Count: 5,
		End:   time.Now().Add(50 * time.Millisecond),
	})
	q.Wait(context.Background())

	if want := 3; runs != want {
		t.Errorf("Got %d runs, want %d", runs, want)
	}
}

func TestQueue_panic(t *testing.T) {
	defer leaktest.Check(t)()

	q := scheddle.NewQueue(nil)
	defer q.Close()

	done := make(chan struct{})
	q.After(10*time.Millisecond, scheddle.Run(func() {
		panic("failure")
	}))
	q.After(20*time.Millisecond, scheddle.Run(func() {
		close(done)
	}))

	select {
	case <-time.After(2 * time.Second):
		t.Error("Second task did not complete")
	case <-done:
		t.Log("Second task completed OK")
	}
}

func TestQueue_Wait(t *testing.T) {
	defer leaktest.Check(t)()

	t.Run("BeforeFirst", func(t *testing.T) {
		q := scheddle.NewQueue(nil)
		defer q.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		if !q.Wait(ctx) {
			t.Errorf("Timed out waiting for queue: %v", ctx.Err())
		}
	})
	t.Run("AfterClose", func(t *testing.T) {
		q := scheddle.NewQueue(nil)
		q.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		if !q.Wait(ctx) {
			t.Errorf("Timed out waiting for queue: %v", ctx.Err())
		}
	})
}
