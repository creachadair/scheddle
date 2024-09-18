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
//
// # Cancellation
//
// Adding a task returns an ID:
//
//	id := q.After(10*time.Second, task3)
//
// Using this ID, you can cancel the task:
//
//	if q.Cancel(id) {
//	   log.Printf("Task %d successfully cancelled", id)
//	}
//
// Cancel reports false if the task has already been scheduled.
package scheddle

import (
	"context"
	"fmt"
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
	cancel context.CancelFunc
	done   chan struct{}
	timer  *time.Timer
	idle   msync.Trigger

	μ      sync.Mutex
	lastID ID
	todo   *heapq.Queue[*entry]
	byID   map[ID]*entry
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
		todo: heapq.New(compareEntries).Update(func(e *entry, pos int) {
			e.pos = pos
		}),
		byID: make(map[ID]*entry),
	}
	q.idle.Set() // initially idle
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
	case <-q.done:
		return true // queue is closed
	case <-q.idle.Ready():
		return true
	}
}

// At schedules task to be executed at the specified time and returns its ID.
// If due is in the past, the task will be immediately eligible to run.
func (q *Queue) At(due time.Time, task Task) ID {
	q.μ.Lock()
	defer q.μ.Unlock()

	q.lastID++
	e := &entry{due: due, task: task, id: q.lastID}
	q.byID[e.id] = e
	if q.todo.Add(e) == 0 {
		q.wakeScheduler()
	}
	return e.id
}

// After schedules task to be executed after the specified duration and returns
// its ID.  If d ≤ 0, the task will be immediately eligible to run.
func (q *Queue) After(d time.Duration, task Task) ID { return q.At(q.Now().Add(d), task) }

// Cancel cancels the specified task, and reports whether the task was present.
// If the specified ID did not exist, or corresponds to a task that was already
// scheduled, Cancel reports false.
func (q *Queue) Cancel(id ID) bool {
	q.μ.Lock()
	defer q.μ.Unlock()

	e, ok := q.byID[id]
	if !ok {
		return false
	}
	q.todo.Remove(e.pos)
	delete(q.byID, id)
	if e.pos == 0 {
		q.wakeScheduler()
	}
	return true
}

func (q *Queue) wakeScheduler() { q.idle.Reset(); q.timer.Reset(0) }

// Now reports the current time as observed by q.
func (q *Queue) Now() time.Time { return q.now() }

func (q *Queue) schedule(ctx context.Context) {
	defer close(q.done)
	for {
		select {
		case <-ctx.Done():
			return
		case <-q.timer.C:
			q.idle.Reset()
			for {
				// Process as many due tasks as are available.
				next, ok := q.popReady()
				if ok {
					err := func() (err error) {
						// If the task panics, turn the panic into an error so that
						// the scheduler does not exit.
						//
						// TODO(creachadair): Maybe log or pass to a callback.
						defer func() {
							if p := recover(); p != nil {
								err = fmt.Errorf("task panic (recovered): %v", p)
							}
						}()
						rctx := context.WithValue(ctx, taskIDKey{}, next.id)
						return next.task.Run(rctx)
					}()
					if err == nil {
						if r, ok := next.task.(Rescheduler); ok {
							r.Reschedule(q)
						}
					}
					continue // check for another due task
				}

				// Reaching here, no more tasks are due -- either the next task is
				// in the future, or the queue is (at least momentarily) empty.
				//
				// If next == nil, it means we got a zero entry due to an empty
				// queue; otherwise the task was in the future, at least as of the
				// moment when we looked at it.
				if next == nil {
					q.idle.Set()
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
// If the queue is empty, it returns nil, false.
func (q *Queue) popReady() (*entry, bool) {
	q.μ.Lock()
	defer q.μ.Unlock()
	next, ok := q.todo.Peek(0)
	if !ok || next.due.After(q.Now()) {
		return next, false
	}
	q.todo.Pop()
	delete(q.byID, next.id)
	return next, true
}

type entry struct {
	due  time.Time
	task Task
	pos  int
	id   ID
}

// compareEntries orders entries non-decreasing by due time.
func compareEntries(a, b *entry) int { return a.due.Compare(b.due) }

// Options are optional settings for a [Queue]. A nil Options is ready for use
// and provides default values as described.  There are currently no options to
// set; this is reserved for future expansion.
type Options struct{}

// An ID is a unique identifier assigned to each task added to a Queue.
// An ID assigned by the Queue is always positive.
type ID int64

type taskIDKey struct{}

// TaskID returns the task ID associated with ctx, or 0.  The context passed to
// a running task has this value.
func TaskID(ctx context.Context) ID {
	if v, ok := ctx.Value(taskIDKey{}).(ID); ok {
		return v
	}
	return 0
}
