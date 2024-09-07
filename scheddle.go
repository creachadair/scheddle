// Copyright (C) 2024 Michael J. Fromberger. All Rights Reserved.

// Package scheddle defines a basic time-based task queue.
//
// # Usage
//
// Construct a [Queue] and add tasks to it:
//
//	q := scheddle.NewQueue(nil) // nil for default options
//
//	// Specify a specific due time with At.
//	q.At(dueTime, task1)
//
//	// Specify an elapsed time with After.
//	q.After(10*time.Minute, task2)
//
// A [Task] is any value exporting a Run method with the signature:
//
//	Run(context.Context) error
//
// You can adapt existing functions to this type using [Run].
//
// To stop the scheduler and discard any remaining tasks, use:
//
//	q.Close()
//
// To wait for the queue to become empty without interrupting the scheduler, use:
//
//	q.Wait(ctx)
package scheddle

import (
	"context"
	"sync"
	"time"

	"github.com/creachadair/mds/heapq"
	"github.com/creachadair/msync"
)

// A Queue implements a basic time-based task queue. Add tasks to the queue
// using [At] and [After]. Call [Close] to close down the scheduler.
//
// A Queue executes tasks sequentially, in time order, with a single goroutine
// that runs in the background. If multiple tasks are due at exactly the same
// time, the scheduler will execute them in unspecified order.
//
// Each task is executed fully before another task is considered, so a Task can
// safely modify its own state without locking.  Tasks that wish to execute in
// parallel may start additional goroutines, but then must manage their own
// state and lifecycle appropriately.
type Queue struct {
	// Initialized at construction.
	now    func() time.Time
	timer  *time.Timer
	cancel context.CancelFunc
	done   chan struct{}
	empty  *msync.Trigger

	μ    sync.Mutex
	todo *heapq.Queue[entry]
}

// NewQueue constructs a new empty [Queue] with the specified options.
// If opts == nil, default options are provided as described by [Options].
func NewQueue(opts *Options) *Queue {
	ctx, cancel := context.WithCancel(context.Background())
	t := time.NewTimer(0)
	t.Stop()
	q := &Queue{
		now:    time.Now, // hooked for testing; see internal_test.go
		timer:  t,
		cancel: cancel,
		done:   make(chan struct{}),
		empty:  msync.NewTrigger(),
		todo:   heapq.New(compareEntries),
	}
	go q.schedule(ctx)
	return q
}

// Close stops the scheduler, discarding any pending tasks, and waits for it to
// terminate.
func (q *Queue) Close() error {
	q.cancel()
	<-q.done
	return nil
}

// Wait blocks until the queue is empty and the scheduler is idle, or ctx ends.
// If ctx ends before the queue is empty, Wait returns false; otherwise true.
// Note that this does not close the queue or prevent more tasks from being
// added.
func (q *Queue) Wait(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-q.empty.Ready():
		return true
	}
}

// At schedules task to be executed at the specified time.  If due is in the
// past, the task will be immediately eligible to run.
func (q *Queue) At(due time.Time, task Task) {
	q.μ.Lock()
	defer q.μ.Unlock()

	q.todo.Add(entry{due: due, task: task})
	q.timer.Reset(0)
}

// After schedules task to be executed after the specified duration.
// If d ≤ 0, the task will be immediately eligible to run.
func (q *Queue) After(d time.Duration, task Task) { q.At(q.Now().Add(d), task) }

// Now reports the current time as observed by q.
func (q *Queue) Now() time.Time {
	if q.now == nil {
		return time.Now()
	}
	return q.now()
}

func (q *Queue) schedule(ctx context.Context) {
	defer close(q.done)
	for {
		select {
		case <-ctx.Done():
			return
		case <-q.timer.C:
			for {
				// Process as many due tasks as are available.
				next, ok := q.popReady()
				if ok {
					if err := next.task.Run(ctx); err == nil {
						if r, ok := next.task.(Rescheduler); ok {
							r.Reschedule(q)
						}
					}
					continue // check for another due task
				}

				// Reaching here, no more tasks are due -- either the next task is
				// in the future, or the queue is (at least momentarily) empty.
				//
				// If next.task == nil, it means we got a zero entry due to an
				// empty queue; otherwise the task was in the future, at least as
				// of the moment when we looked at it.
				if next.task == nil {
					q.empty.Signal()
				} else {
					q.timer.Reset(next.due.Sub(q.Now()))
				}
				break
			}
		}
	}
}

// popReady returns the frontmost task on the queue, and reports whether it is
// eligible to be run. If so, the task is popped off the queue before returning.
// If the queue is empty, it returns entry{}, false.
func (q *Queue) popReady() (entry, bool) {
	q.μ.Lock()
	defer q.μ.Unlock()
	next, ok := q.todo.Peek(0)
	if !ok || next.due.After(q.Now()) {
		return next, false
	}
	q.todo.Pop()
	return next, true
}

type entry struct {
	due  time.Time
	task Task
}

// compareEntries orders entries non-decreasing by due time.
func compareEntries(a, b entry) int { return a.due.Compare(b.due) }

// Options are optional settings for a [Queue]. A nil Options is ready for use
// and provides default values as described.  There are currently no options to
// set; this is reserved for future expansion.
type Options struct{}
