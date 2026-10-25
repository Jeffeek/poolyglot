package connpool

import (
	"context"
	"errors"
	"io"
	"testing"
)

// conn is a minimal io.Closer connection used to exercise FuncFactory/AsyncFuncFactory
// default behaviors, mirroring python tests/factories/test_factories.py and
// java FactoryTest.java (Conn implements AutoCloseable / has a close() method).
type conn struct {
	closed bool
}

func (c *conn) Close() error {
	c.closed = true
	return nil
}

var _ io.Closer = (*conn)(nil)

// plainConn has no Close method, exercising the "no io.Closer" default-close branch.
type plainConn struct{}

func TestFuncFactory_Defaults(t *testing.T) {
	f := FuncFactory[*conn]{
		CreateFn: func() (*conn, error) { return &conn{}, nil },
	}
	c, err := f.Create()
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !f.Validate(c) {
		t.Error("nil ValidateFn should default to true")
	}
	if f.IsFatalError(errors.New("boom")) {
		t.Error("nil IsFatalErrorFn should default to false")
	}
	if err := f.Reset(c); err != nil {
		t.Errorf("nil ResetFn should default to no-op, got %v", err)
	}
	if err := f.Close(c); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !c.closed {
		t.Error("nil CloseFn should fall back to io.Closer.Close")
	}
}

func TestFuncFactory_CloseWithoutIOCloser(t *testing.T) {
	f := FuncFactory[*plainConn]{
		CreateFn: func() (*plainConn, error) { return &plainConn{}, nil },
	}
	c, _ := f.Create()
	if err := f.Close(c); err != nil {
		t.Errorf("no io.Closer and no CloseFn should be a silent no-op, got %v", err)
	}
}

func TestFuncFactory_AllCallbacks(t *testing.T) {
	var resets []*conn
	var closed []*conn
	f := FuncFactory[*conn]{
		CreateFn:   func() (*conn, error) { return &conn{}, nil },
		ValidateFn: func(c *conn) bool { return true },
		ResetFn: func(c *conn) error {
			resets = append(resets, c)
			return nil
		},
		CloseFn: func(c *conn) error {
			closed = append(closed, c)
			return nil
		},
		IsFatalErrorFn: func(err error) bool {
			var target *myFatalErr
			return errors.As(err, &target)
		},
	}
	c, err := f.Create()
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !f.Validate(c) {
		t.Error("ValidateFn should report true")
	}
	if err := f.Reset(c); err != nil {
		t.Errorf("Reset: %v", err)
	}
	if len(resets) != 1 || resets[0] != c {
		t.Errorf("ResetFn should have been called with c, got %v", resets)
	}
	if !f.IsFatalError(&myFatalErr{}) {
		t.Error("IsFatalErrorFn should report fatal for myFatalErr")
	}
	if f.IsFatalError(errors.New("other")) {
		t.Error("IsFatalErrorFn should report non-fatal for unrelated errors")
	}
	if err := f.Close(c); err != nil {
		t.Errorf("Close: %v", err)
	}
	if len(closed) != 1 || closed[0] != c {
		t.Errorf("explicit CloseFn should have been used instead of io.Closer, got %v", closed)
	}
	if c.closed {
		t.Error("explicit CloseFn should bypass the connection's own Close method")
	}
}

func TestFuncFactory_ResetFnError(t *testing.T) {
	f := FuncFactory[*conn]{
		CreateFn: func() (*conn, error) { return &conn{}, nil },
		ResetFn:  func(c *conn) error { return errResetFailed },
	}
	c, _ := f.Create()
	if err := f.Reset(c); !errors.Is(err, errResetFailed) {
		t.Errorf("Reset should propagate ResetFn error, got %v", err)
	}
}

func TestFuncFactory_CloseFnError(t *testing.T) {
	f := FuncFactory[*conn]{
		CreateFn: func() (*conn, error) { return &conn{}, nil },
		CloseFn:  func(c *conn) error { return errBackendDown },
	}
	c, _ := f.Create()
	if err := f.Close(c); !errors.Is(err, errBackendDown) {
		t.Errorf("Close should propagate CloseFn error, got %v", err)
	}
}

func TestFuncFactory_CreateFnError(t *testing.T) {
	f := FuncFactory[*conn]{
		CreateFn: func() (*conn, error) { return nil, errBackendDown },
	}
	if _, err := f.Create(); !errors.Is(err, errBackendDown) {
		t.Errorf("Create should propagate CreateFn error, got %v", err)
	}
}

type myFatalErr struct{}

func (e *myFatalErr) Error() string { return "fatal" }

// ---- AsyncFuncFactory ----

func TestAsyncFuncFactory_Defaults(t *testing.T) {
	ctx := context.Background()
	f := AsyncFuncFactory[*conn]{
		CreateFn: func(ctx context.Context) (*conn, error) { return &conn{}, nil },
	}
	c, err := f.Create(ctx)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !f.Validate(ctx, c) {
		t.Error("nil ValidateFn should default to true")
	}
	if f.IsFatalError(errors.New("boom")) {
		t.Error("nil IsFatalErrorFn should default to false")
	}
	if err := f.Reset(ctx, c); err != nil {
		t.Errorf("nil ResetFn should default to no-op, got %v", err)
	}
	if err := f.Close(ctx, c); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !c.closed {
		t.Error("nil CloseFn should fall back to io.Closer.Close")
	}
}

func TestAsyncFuncFactory_CloseWithoutIOCloser(t *testing.T) {
	ctx := context.Background()
	f := AsyncFuncFactory[*plainConn]{
		CreateFn: func(ctx context.Context) (*plainConn, error) { return &plainConn{}, nil },
	}
	c, _ := f.Create(ctx)
	if err := f.Close(ctx, c); err != nil {
		t.Errorf("no io.Closer and no CloseFn should be a silent no-op, got %v", err)
	}
}

func TestAsyncFuncFactory_AllCallbacks(t *testing.T) {
	ctx := context.Background()
	var resets []*conn
	var closed []*conn
	f := AsyncFuncFactory[*conn]{
		CreateFn:   func(ctx context.Context) (*conn, error) { return &conn{}, nil },
		ValidateFn: func(ctx context.Context, c *conn) bool { return true },
		ResetFn: func(ctx context.Context, c *conn) error {
			resets = append(resets, c)
			return nil
		},
		CloseFn: func(ctx context.Context, c *conn) error {
			closed = append(closed, c)
			return nil
		},
		IsFatalErrorFn: func(err error) bool {
			var target *myFatalErr
			return errors.As(err, &target)
		},
	}
	c, err := f.Create(ctx)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !f.Validate(ctx, c) {
		t.Error("ValidateFn should report true")
	}
	if err := f.Reset(ctx, c); err != nil {
		t.Errorf("Reset: %v", err)
	}
	if len(resets) != 1 || resets[0] != c {
		t.Errorf("ResetFn should have been called with c, got %v", resets)
	}
	if !f.IsFatalError(&myFatalErr{}) {
		t.Error("IsFatalErrorFn should report fatal for myFatalErr")
	}
	if f.IsFatalError(errors.New("other")) {
		t.Error("IsFatalErrorFn should report non-fatal for unrelated errors")
	}
	if err := f.Close(ctx, c); err != nil {
		t.Errorf("Close: %v", err)
	}
	if len(closed) != 1 || closed[0] != c {
		t.Errorf("explicit CloseFn should have been used instead of io.Closer, got %v", closed)
	}
	if c.closed {
		t.Error("explicit CloseFn should bypass the connection's own Close method")
	}
}

func TestAsyncFuncFactory_ResetFnError(t *testing.T) {
	ctx := context.Background()
	f := AsyncFuncFactory[*conn]{
		CreateFn: func(ctx context.Context) (*conn, error) { return &conn{}, nil },
		ResetFn:  func(ctx context.Context, c *conn) error { return errResetFailed },
	}
	c, _ := f.Create(ctx)
	if err := f.Reset(ctx, c); !errors.Is(err, errResetFailed) {
		t.Errorf("Reset should propagate ResetFn error, got %v", err)
	}
}

func TestAsyncFuncFactory_CloseFnError(t *testing.T) {
	ctx := context.Background()
	f := AsyncFuncFactory[*conn]{
		CreateFn: func(ctx context.Context) (*conn, error) { return &conn{}, nil },
		CloseFn:  func(ctx context.Context, c *conn) error { return errBackendDown },
	}
	c, _ := f.Create(ctx)
	if err := f.Close(ctx, c); !errors.Is(err, errBackendDown) {
		t.Errorf("Close should propagate CloseFn error, got %v", err)
	}
}

func TestAsyncFuncFactory_CreateFnError(t *testing.T) {
	ctx := context.Background()
	f := AsyncFuncFactory[*conn]{
		CreateFn: func(ctx context.Context) (*conn, error) { return nil, errBackendDown },
	}
	if _, err := f.Create(ctx); !errors.Is(err, errBackendDown) {
		t.Errorf("Create should propagate CreateFn error, got %v", err)
	}
}

func TestAsyncFuncFactory_CreateFnRespectsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f := AsyncFuncFactory[*conn]{
		CreateFn: func(ctx context.Context) (*conn, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return &conn{}, nil
		},
	}
	if _, err := f.Create(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Create should surface a canceled context, got %v", err)
	}
}

// FakeFactory / AsyncFakeFactory (from support_test.go) also implement the
// factory interfaces end-to-end; exercise their IsFatalError wiring here since
// no other factory-focused test file does.
func TestFakeFactory_IsFatalError(t *testing.T) {
	f := &FakeFactory{Fatal: func(err error) bool { return errors.Is(err, errBackendDown) }}
	if !f.IsFatalError(errBackendDown) {
		t.Error("expected errBackendDown to be fatal")
	}
	if f.IsFatalError(errResetFailed) {
		t.Error("expected errResetFailed to not be fatal")
	}
}

func TestFakeFactory_IsFatalError_NilFatalFn(t *testing.T) {
	f := &FakeFactory{}
	if f.IsFatalError(errBackendDown) {
		t.Error("nil Fatal fn should default to false")
	}
}

func TestAsyncFakeFactory_Delegates(t *testing.T) {
	ctx := context.Background()
	af := newAsyncFakeFactory()
	af.Inner.Fatal = func(err error) bool { return errors.Is(err, errBackendDown) }

	c, err := af.Create(ctx)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !af.Validate(ctx, c) {
		t.Error("expected healthy connection to validate")
	}
	if err := af.Reset(ctx, c); err != nil {
		t.Errorf("Reset: %v", err)
	}
	if !af.IsFatalError(errBackendDown) {
		t.Error("expected delegated IsFatalError to report fatal")
	}
	if err := af.Close(ctx, c); err != nil {
		t.Errorf("Close: %v", err)
	}
	if !c.Closed {
		t.Error("expected underlying connection to be closed")
	}
}

func TestAsyncFakeFactory_CreateDelay_ContextCanceled(t *testing.T) {
	af := newAsyncFakeFactory()
	af.CreateDelay = 10
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := af.Create(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled while waiting for CreateDelay, got %v", err)
	}
}
