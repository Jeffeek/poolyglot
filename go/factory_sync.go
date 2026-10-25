package connpool

import "io"

// ConnectionFactory is the ONE seam between the pool and the outside world.
//
// To pool anything (DB driver, HTTP client, SSH session, socket, gRPC channel...)
// implement ConnectionFactory - or just build one from functions via FuncFactory.
type ConnectionFactory[T any] interface {
	// Create opens a new connection. Should enforce its own connect timeout.
	Create() (T, error)
	// Validate is a cheap liveness check (e.g. SELECT 1). Return false = dead.
	Validate(conn T) bool
	// Reset is called on release to scrub session state (e.g. rollback).
	Reset(conn T) error
	// Close disposes of a connection.
	Close(conn T) error
	// IsFatalError reports whether err (raised by user code using the connection)
	// proves the connection is broken, so it is discarded without further checks.
	IsFatalError(err error) bool
}

// FuncFactory builds a ConnectionFactory from plain functions. Nil funcs fall
// back to defaults matching ConnectionFactory's documented behavior.
type FuncFactory[T any] struct {
	CreateFn       func() (T, error)
	ValidateFn     func(T) bool
	ResetFn        func(T) error
	CloseFn        func(T) error
	IsFatalErrorFn func(error) bool
}

func (f FuncFactory[T]) Create() (T, error) {
	return f.CreateFn()
}

func (f FuncFactory[T]) Validate(conn T) bool {
	if f.ValidateFn == nil {
		return true
	}
	return f.ValidateFn(conn)
}

func (f FuncFactory[T]) Reset(conn T) error {
	if f.ResetFn == nil {
		return nil
	}
	return f.ResetFn(conn)
}

func (f FuncFactory[T]) Close(conn T) error {
	if f.CloseFn != nil {
		return f.CloseFn(conn)
	}
	if closer, ok := any(conn).(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (f FuncFactory[T]) IsFatalError(err error) bool {
	if f.IsFatalErrorFn == nil {
		return false
	}
	return f.IsFatalErrorFn(err)
}
