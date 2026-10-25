package connpool

import (
	"context"
	"io"
)

// AsyncConnectionFactory is the seam users implement to pool anything asynchronously.
type AsyncConnectionFactory[T any] interface {
	Create(ctx context.Context) (T, error)
	Validate(ctx context.Context, conn T) bool
	Reset(ctx context.Context, conn T) error
	Close(ctx context.Context, conn T) error
	IsFatalError(err error) bool
}

// AsyncFuncFactory builds an AsyncConnectionFactory from plain functions.
// Nil functions fall back to defaults (Validate->true, Reset->nil,
// Close->io.Closer if implemented else nil, IsFatalError->false).
type AsyncFuncFactory[T any] struct {
	CreateFn       func(context.Context) (T, error)
	ValidateFn     func(context.Context, T) bool
	ResetFn        func(context.Context, T) error
	CloseFn        func(context.Context, T) error
	IsFatalErrorFn func(error) bool
}

func (f AsyncFuncFactory[T]) Create(ctx context.Context) (T, error) {
	return f.CreateFn(ctx)
}

func (f AsyncFuncFactory[T]) Validate(ctx context.Context, conn T) bool {
	if f.ValidateFn == nil {
		return true
	}
	return f.ValidateFn(ctx, conn)
}

func (f AsyncFuncFactory[T]) Reset(ctx context.Context, conn T) error {
	if f.ResetFn == nil {
		return nil
	}
	return f.ResetFn(ctx, conn)
}

func (f AsyncFuncFactory[T]) Close(ctx context.Context, conn T) error {
	if f.CloseFn != nil {
		return f.CloseFn(ctx, conn)
	}
	if closer, ok := any(conn).(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (f AsyncFuncFactory[T]) IsFatalError(err error) bool {
	if f.IsFatalErrorFn == nil {
		return false
	}
	return f.IsFatalErrorFn(err)
}
