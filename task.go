// Copyright (C) 2024 Michael J. Fromberger. All Rights Reserved.

package scheddle

import (
	"fmt"
	"time"
)

// T adapts val to a Task. If the concrete type of val satisfies the Task
// interface, it is returned directly; otherwise val must be one of:
//
//	func()
//	func() error
//
// Either of these types is converted into a Task that runs the function once.
// For any other type, T will panic.
func T(val any) Task {
	switch t := val.(type) {
	case func():
		return runThunk(t)
	case func() error:
		return runFunc(t)
	case Task:
		return t
	default:
		panic(fmt.Sprintf("cannot convert %T to a Task", val))
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

// Reschedule implements part of the [Task] interface. In this implementation,
// it reschedules r if it has not yet used up its run count, and the time is
// prior to the specified ending time.
func (r *Repeat) Reschedule(q *Queue) {
	if r.Every <= 0 {
		return
	}
	r.runs++
	if r.Count > 0 && r.runs >= r.Count {
		return
	} else if !r.End.IsZero() && q.Now().After(r.End) {
		return
	}
	q.After(r.Every, r)
}

// A runFunc is a Task that runs by calling the function.
type runFunc func() error

// Run executes the task by calling f.
func (f runFunc) Run() error { return f() }

// A runThunk is a Task that runs by calling the function and reporting nil.
type runThunk func()

func (f runThunk) Run() error { f(); return nil }

// A Task represents a unit of work that can be scheduled by a [Queue].
// When a task reaches the front of the queue, the scheduler calls Run.
type Task interface {
	// Run executes the task and reports success (nil) or an error.
	Run() error
}

// Rescheduler is an optional interface that may be implemented by a [Task].
// If so, then whenever the task successfully completes, its Reschedule method
// is called to allow it to reschedule itself or other tasks.
type Rescheduler interface {
	// Reschedule allows the receiver to add new tasks to q, if it wishes.
	Reschedule(q *Queue)
}
