// Copyright (C) 2024 Michael J. Fromberger. All Rights Reserved.

// Package scheddle defines a basic time-based task queue.
//
// # Usage
//
// Construct a [Queue] and add tasks to it:
//
//	q := scheddle.NewQueue(nil) // nil for default options
//
//	// Specify a specific due time with Add.
//	q.Add(dueTime, task1)
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
	run    func(Task)
	timer  *time.Timer
	cancel context.CancelFunc
	done   chan struct{}

	μ    sync.Mutex
	todo *heapq.Queue[entry]
}

// Task is a unit of schedulable work in a [Queue].
type Task func()

// NewQueue constructs a new empty [Queue] with the specified options.
// If opts == nil, default options are provided as described by [Options].
func NewQueue(opts *Options) *Queue {
	ctx, cancel := context.WithCancel(context.Background())
	t := time.NewTimer(0)
	t.Stop()
	q := &Queue{
		now:    opts.timeNow(),
		run:    opts.runFunc(),
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

// Add schedules task to be executed at the specified time.  If due is in the
// past, the task will be immediately eligible to run.
func (q *Queue) Add(due time.Time, task Task) {
	q.μ.Lock()
	defer q.μ.Unlock()

	q.todo.Add(entry{due: due, task: task})
	q.timer.Reset(0)
}

// After schedules task to be executed after the specified duration.
// If d ≤ 0, the task will be immediately eligible to run.
func (q *Queue) After(d time.Duration, task Task) {
	q.Add(q.timeNow().Add(d), task)
}

func (q *Queue) timeNow() time.Time {
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
					q.run(next.task)
					continue // check for another due task
				}

				// No more due tasks. If there is a pending task, it is in the
				// future, so set a wakeup for then.
				if !next.due.IsZero() {
					q.timer.Reset(next.due.Sub(q.timeNow()))
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
	if !ok || next.due.After(q.timeNow()) {
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
// and provides default values as described.
type Options struct {
	// TimeNow, if non-nil, is used as the time source for evaluating scheduling
	// decisions. By default, the queue uses [time.Now].
	TimeNow func() time.Time

	// Run, if non-nil, is called for each eligible task by the scheduler, to
	// execute the task. The default calls the task function directly. You can
	// override this to schedule tasks to run in separate goroutines, or to
	// handle common task plumbing.
	Run func(Task)
}

func (o *Options) timeNow() func() time.Time {
	if o != nil {
		return o.TimeNow
	}
	return nil
}

func (o *Options) runFunc() func(Task) {
	if o == nil || o.Run == nil {
		return func(t Task) { t() }
	}
	return o.Run
}
