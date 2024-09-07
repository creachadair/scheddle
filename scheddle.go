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
// Tasks are executed sequentially in time order by a single goroutine that runs
// in the background. To stop the scheduler and discard any pending tasks, use
//
//	q.Close()
package scheddle

import (
	"context"
	"sync"
	"time"

	"github.com/creachadair/mds/heapq"
)

// A Queue implements a basic time-based task queue.
// Add tasks to the queue using [Add] and [After].
// Call [Close] to close down the scheduler.
type Queue struct {
	// Initialized at construction.
	now    func() time.Time
	timer  *time.Timer
	cancel context.CancelFunc
	done   chan struct{}

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
		now:    time.Now,
		timer:  t,
		cancel: cancel,
		done:   make(chan struct{}),
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
				next, ok := q.popReady()
				if ok {
					if err := next.task.Run(); err == nil {
						if r, ok := next.task.(Rescheduler); ok {
							r.Reschedule(q)
						}
					}
					continue // check for another due task
				}

				// No more due tasks. If there is a pending task, it is in the
				// future, so set a wakeup for then.
				if !next.due.IsZero() {
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
