package pipeline

import (
	"context"
	"sync"
)

// group runs a set of goroutines, keeps the first error, and cancels the rest
// when one fails.
//
// This is golang.org/x/sync/errgroup's behaviour, written out here rather than
// imported. Two reasons, in order of importance:
//
//  1. docs/ENGINEERING-RULES.md R3 puts pipeline orchestration on the from-scratch list. The
//     coordination is thirty lines, and writing it out keeps the pipeline's
//     failure and cancellation semantics visible in this repository rather
//     than deferred to a dependency.
//  2. It keeps distbackup's core at zero third-party dependencies, so
//     `go test ./...` runs with the machine offline — which docs/ENGINEERING-RULES.md R7
//     requires and which a module download would quietly break. (Adding
//     x/sync also forced the go directive from 1.23 to 1.25 and pulled a new
//     toolchain, which was not a change worth taking for thirty lines.)
//
// The zero value is not usable; call newGroup.
type group struct {
	wg     sync.WaitGroup
	cancel context.CancelFunc

	// once ensures only the first error is kept. Later failures are almost
	// always consequences of the first — a worker seeing a closed channel, a
	// context cancellation — and reporting one of those instead of the real
	// cause makes debugging much harder.
	once sync.Once
	err  error
}

// newGroup returns a group and a derived context that is cancelled when any
// goroutine returns an error or when Wait returns.
func newGroup(ctx context.Context) (*group, context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	return &group{cancel: cancel}, ctx
}

// Go runs fn in a new goroutine.
func (g *group) Go(fn func() error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		if err := fn(); err != nil {
			g.once.Do(func() {
				g.err = err
				// Cancelling here is what makes the pipeline shut down
				// promptly instead of running to completion after a failure.
				// Every channel operation in this package selects on
				// ctx.Done(), so this unblocks all of them.
				g.cancel()
			})
		}
	}()
}

// Wait blocks until every goroutine has returned, then returns the first
// error, if any.
//
// It always cancels the derived context, so a caller that returns early
// cannot leak the context's resources.
func (g *group) Wait() error {
	g.wg.Wait()
	g.cancel()
	return g.err
}

// send delivers v on ch, or gives up if ctx is done.
//
// Every channel send in this package goes through here. A bare `ch <- v`
// blocks forever if the receiver has already exited — which is exactly what
// happens when another stage fails — and that turns a clean error into a
// deadlock that only reproduces under load.
func send[T any](ctx context.Context, ch chan<- T, v T) bool {
	select {
	case ch <- v:
		return true
	case <-ctx.Done():
		return false
	}
}

// recv takes a value from ch, reporting whether one was received.
//
// The second return distinguishes "channel closed" from "context cancelled",
// which the caller needs in order to decide between finishing normally and
// abandoning work.
func recv[T any](ctx context.Context, ch <-chan T) (v T, ok bool, canceled bool) {
	select {
	case v, ok = <-ch:
		return v, ok, false
	case <-ctx.Done():
		var zero T
		return zero, false, true
	}
}
