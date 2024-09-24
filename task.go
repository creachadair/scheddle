// Copyright (C) 2024 Michael J. Fromberger. All Rights Reserved.

package scheddle

import (
	"context"
	"fmt"
	"time"
)

// Run adapts f to a [Task]. If the concrete type of f satisfies the Task
// interface, it is returned directly; otherwise f must be one of:
//
//	func()
//	func() error
//	func(context.Context)
//	func(context.Context) error
//
// Any of these types is converted into a Task that runs the function.  For any
// other type, Run will panic.
func Run(f any) Task {
	switch t := f.(type) {
	case func():
		return runFunc(func(context.Context) error { t(); return nil })
	case func() error:
		return runFunc(func(context.Context) error { return t() })
	case func(context.Context):
		return runFunc(func(ctx context.Context) error { t(ctx); return nil })
	case func(context.Context) error:
		return runFunc(t)
	case Task:
		return t
	default:
		panic(fmt.Sprintf("cannot convert %T to a Task", f))
	}
}

// Repeat is a Task that wraps another task to cause it to be repeated when it
// successfully executes.
type Repeat struct {
	// Task is the task to be repeated.
	Task

	// Every gives the duration between repeats. If Every ≤ 0, the task executes
	// only once.
	Every time.Duration

	// Count, if positive, limits the number of executions. If Count ≤ 0, there
	// is no limit to the number of repetitions.
	Count int

	// End, if non-zero, specifies the time after which execution ends.  If End
	// is the zero time, the task will repeat indefinitely.
	End time.Time

	runs int // number of runs elapsed so far
}

// Run implements the [Task] interface.
func (r *Repeat) Run(ctx context.Context) error {
	if err := r.Task.Run(ctx); err != nil {
		return err
	}
	if r.Every <= 0 {
		return nil
	}
	r.runs++
	if r.Count > 0 && r.runs >= r.Count {
		return nil
	}
	q := TaskQueue(ctx)
	if r.End.IsZero() || q.Now().Before(r.End) {
		q.After(r.Every, r)
	}
	return nil
}

// A runFunc is a Task that runs by calling the function.
type runFunc func(context.Context) error

// Run executes the task by calling f.
func (f runFunc) Run(ctx context.Context) error { return f(ctx) }

// A Task represents a unit of work that can be scheduled by a [Queue].
// When a task reaches the front of the queue, the scheduler calls Run.
type Task interface {
	// Run executes the task and reports success (nil) or an error.
	// The context passed to a task executed by a [Queue] will return useful
	// values from the [TaskID] and [TaskQueue] functions.  The context will be
	// cancelled if the task is executing when the Queue is closed.
	Run(context.Context) error
}
