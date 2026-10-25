package connpool

import (
	"context"
	"errors"
	"sort"
	"testing"
)

// ----------------------------------------------------------------- sync

func TestRegistry_LazyCreationAndPoolPerKey(t *testing.T) {
	var made []string
	factoryFor := func(key string) ConnectionFactory[*FakeConn] {
		made = append(made, key)
		return &FakeFactory{}
	}
	reg := NewPoolRegistry(factoryFor, quietConfig(), nil)

	var a1, a2, b *FakeConn

	if err := reg.Use("a", Timeout{}, func(c *FakeConn) error { a1 = c; return nil }); err != nil {
		t.Fatalf("Use(a) #1: %v", err)
	}
	if err := reg.Use("a", Timeout{}, func(c *FakeConn) error { a2 = c; return nil }); err != nil {
		t.Fatalf("Use(a) #2: %v", err)
	}
	if err := reg.Use("b", Timeout{}, func(c *FakeConn) error { b = c; return nil }); err != nil {
		t.Fatalf("Use(b): %v", err)
	}

	if a1 != a2 {
		t.Errorf("expected same connection reused for key 'a', got %v vs %v", a1, a2)
	}
	if b == a1 {
		t.Errorf("expected distinct connection for key 'b'")
	}

	sort.Strings(made)
	if len(made) != 2 || made[0] != "a" || made[1] != "b" {
		t.Errorf("expected factoryFor called once per key [a b], got %v", made)
	}

	if err := reg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := reg.Pool("a"); err == nil {
		t.Error("expected error from Pool() after Close()")
	}
}

func TestRegistry_PoolReturnsSameInstanceForSameKey(t *testing.T) {
	factoryFor := func(key string) ConnectionFactory[*FakeConn] {
		return &FakeFactory{}
	}
	reg := NewPoolRegistry(factoryFor, quietConfig(), nil)
	defer reg.Close()

	p1, err := reg.Pool("x")
	if err != nil {
		t.Fatalf("Pool: %v", err)
	}
	p2, err := reg.Pool("x")
	if err != nil {
		t.Fatalf("Pool: %v", err)
	}
	if p1 != p2 {
		t.Error("expected same pool instance for the same key")
	}
}

func TestRegistry_PoolNamedAfterKey(t *testing.T) {
	factoryFor := func(key string) ConnectionFactory[*FakeConn] {
		return &FakeFactory{}
	}
	reg := NewPoolRegistry(factoryFor, quietConfig(), nil)
	defer reg.Close()

	pool, err := reg.Pool("my-key")
	if err != nil {
		t.Fatalf("Pool: %v", err)
	}
	if pool.Name != "my-key" {
		t.Errorf("expected pool name 'my-key', got %q", pool.Name)
	}
}

func TestRegistry_OptsTemplatePreserved(t *testing.T) {
	factoryFor := func(key string) ConnectionFactory[*FakeConn] {
		return &FakeFactory{}
	}
	clock := NewManualClock(1000.0)
	rec := &Recorder{}
	reg := NewPoolRegistry(factoryFor, quietConfig(), &Options[*FakeConn]{
		Name:      "ignored-template-name",
		Clock:     clock,
		Listeners: []PoolListener[*FakeConn]{rec},
	})
	defer reg.Close()

	pool, err := reg.Pool("a")
	if err != nil {
		t.Fatalf("Pool: %v", err)
	}
	if pool.Name != "a" {
		t.Errorf("expected pool name overridden to key 'a', got %q", pool.Name)
	}

	conn, err := pool.Acquire(Timeout{})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := pool.Release(conn); err != nil {
		t.Fatalf("Release: %v", err)
	}
	events := rec.events()
	if len(events) == 0 {
		t.Error("expected listener from opts template to be attached to per-key pool")
	}
}

func TestRegistry_ConfigError(t *testing.T) {
	factoryFor := func(key string) ConnectionFactory[*FakeConn] {
		return &FakeFactory{}
	}
	badCfg := quietConfig()
	badCfg.MinSize = -1
	reg := NewPoolRegistry(factoryFor, badCfg, nil)

	if _, err := reg.Pool("a"); err == nil {
		t.Error("expected error for invalid pool config")
	}
}

func TestRegistry_UsePropagatesPoolError(t *testing.T) {
	factoryFor := func(key string) ConnectionFactory[*FakeConn] {
		return &FakeFactory{}
	}
	badCfg := quietConfig()
	badCfg.MinSize = -1
	reg := NewPoolRegistry(factoryFor, badCfg, nil)

	err := reg.Use("a", Timeout{}, func(c *FakeConn) error { return nil })
	if err == nil {
		t.Error("expected Use to propagate pool creation error")
	}
}

func TestRegistry_CloseClosesEveryPool(t *testing.T) {
	factories := map[string]*FakeFactory{}
	factoryFor := func(key string) ConnectionFactory[*FakeConn] {
		f := &FakeFactory{}
		factories[key] = f
		return f
	}
	reg := NewPoolRegistry(factoryFor, quietConfig(), nil)

	for _, key := range []string{"a", "b"} {
		if err := reg.Use(key, Timeout{}, func(c *FakeConn) error { return nil }); err != nil {
			t.Fatalf("Use(%s): %v", key, err)
		}
	}

	if err := reg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for key, f := range factories {
		if f.createdCount() == 0 {
			t.Fatalf("expected a connection created for key %s", key)
		}
		for _, c := range f.Created {
			if !c.Closed {
				t.Errorf("expected connection for key %s to be closed after registry Close", key)
			}
		}
	}
}

func TestRegistry_DoubleCloseIsSafe(t *testing.T) {
	factoryFor := func(key string) ConnectionFactory[*FakeConn] {
		return &FakeFactory{}
	}
	reg := NewPoolRegistry(factoryFor, quietConfig(), nil)
	if err := reg.Use("a", Timeout{}, func(c *FakeConn) error { return nil }); err != nil {
		t.Fatalf("Use: %v", err)
	}
	if err := reg.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}
	if err := reg.Close(); err != nil {
		t.Fatalf("Close #2 should be safe: %v", err)
	}
}

func TestRegistry_UseAfterCloseReturnsError(t *testing.T) {
	factoryFor := func(key string) ConnectionFactory[*FakeConn] {
		return &FakeFactory{}
	}
	reg := NewPoolRegistry(factoryFor, quietConfig(), nil)
	if err := reg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	err := reg.Use("a", Timeout{}, func(c *FakeConn) error { return nil })
	if err == nil {
		t.Error("expected error using registry after Close")
	}
}

// ----------------------------------------------------------------- async

func TestAsyncRegistry_OnePoolPerKey(t *testing.T) {
	ctx := context.Background()
	var made []string
	factoryFor := func(key string) AsyncConnectionFactory[*FakeConn] {
		made = append(made, key)
		return newAsyncFakeFactory()
	}
	reg := NewAsyncPoolRegistry(factoryFor, quietConfig(), nil)

	var a1, a2, b *FakeConn

	if err := reg.Use(ctx, "a", Timeout{}, func(c *FakeConn) error { a1 = c; return nil }); err != nil {
		t.Fatalf("Use(a) #1: %v", err)
	}
	if err := reg.Use(ctx, "a", Timeout{}, func(c *FakeConn) error { a2 = c; return nil }); err != nil {
		t.Fatalf("Use(a) #2: %v", err)
	}
	if err := reg.Use(ctx, "b", Timeout{}, func(c *FakeConn) error { b = c; return nil }); err != nil {
		t.Fatalf("Use(b): %v", err)
	}

	if a1 != a2 {
		t.Errorf("expected same connection reused for key 'a', got %v vs %v", a1, a2)
	}
	if b == a1 {
		t.Errorf("expected distinct connection for key 'b'")
	}

	sort.Strings(made)
	if len(made) != 2 || made[0] != "a" || made[1] != "b" {
		t.Errorf("expected factoryFor called once per key [a b], got %v", made)
	}

	if err := reg.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := reg.Pool("a"); err == nil {
		t.Error("expected error from Pool() after Close()")
	}
}

func TestAsyncRegistry_PoolReturnsSameInstanceForSameKey(t *testing.T) {
	ctx := context.Background()
	factoryFor := func(key string) AsyncConnectionFactory[*FakeConn] {
		return newAsyncFakeFactory()
	}
	reg := NewAsyncPoolRegistry(factoryFor, quietConfig(), nil)
	defer reg.Close(ctx)

	p1, err := reg.Pool("x")
	if err != nil {
		t.Fatalf("Pool: %v", err)
	}
	p2, err := reg.Pool("x")
	if err != nil {
		t.Fatalf("Pool: %v", err)
	}
	if p1 != p2 {
		t.Error("expected same pool instance for the same key")
	}
}

func TestAsyncRegistry_PoolNamedAfterKey(t *testing.T) {
	ctx := context.Background()
	factoryFor := func(key string) AsyncConnectionFactory[*FakeConn] {
		return newAsyncFakeFactory()
	}
	reg := NewAsyncPoolRegistry(factoryFor, quietConfig(), nil)
	defer reg.Close(ctx)

	pool, err := reg.Pool("my-key")
	if err != nil {
		t.Fatalf("Pool: %v", err)
	}
	if pool.Name != "my-key" {
		t.Errorf("expected pool name 'my-key', got %q", pool.Name)
	}
}

func TestAsyncRegistry_OptsTemplatePreserved(t *testing.T) {
	ctx := context.Background()
	factoryFor := func(key string) AsyncConnectionFactory[*FakeConn] {
		return newAsyncFakeFactory()
	}
	clock := NewManualClock(1000.0)
	rec := &Recorder{}
	reg := NewAsyncPoolRegistry(factoryFor, quietConfig(), &Options[*FakeConn]{
		Name:      "ignored-template-name",
		Clock:     clock,
		Listeners: []PoolListener[*FakeConn]{rec},
	})
	defer reg.Close(ctx)

	pool, err := reg.Pool("a")
	if err != nil {
		t.Fatalf("Pool: %v", err)
	}
	if pool.Name != "a" {
		t.Errorf("expected pool name overridden to key 'a', got %q", pool.Name)
	}

	conn, err := pool.Acquire(ctx, Timeout{})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := pool.Release(conn); err != nil {
		t.Fatalf("Release: %v", err)
	}
	events := rec.events()
	if len(events) == 0 {
		t.Error("expected listener from opts template to be attached to per-key pool")
	}
}

func TestAsyncRegistry_ConfigError(t *testing.T) {
	factoryFor := func(key string) AsyncConnectionFactory[*FakeConn] {
		return newAsyncFakeFactory()
	}
	badCfg := quietConfig()
	badCfg.MinSize = -1
	reg := NewAsyncPoolRegistry(factoryFor, badCfg, nil)

	if _, err := reg.Pool("a"); err == nil {
		t.Error("expected error for invalid pool config")
	}
}

func TestAsyncRegistry_UsePropagatesPoolError(t *testing.T) {
	ctx := context.Background()
	factoryFor := func(key string) AsyncConnectionFactory[*FakeConn] {
		return newAsyncFakeFactory()
	}
	badCfg := quietConfig()
	badCfg.MinSize = -1
	reg := NewAsyncPoolRegistry(factoryFor, badCfg, nil)

	err := reg.Use(ctx, "a", Timeout{}, func(c *FakeConn) error { return nil })
	if err == nil {
		t.Error("expected Use to propagate pool creation error")
	}
}

func TestAsyncRegistry_CloseClosesEveryPool(t *testing.T) {
	ctx := context.Background()
	factories := map[string]*FakeFactory{}
	factoryFor := func(key string) AsyncConnectionFactory[*FakeConn] {
		af := newAsyncFakeFactory()
		factories[key] = af.Inner
		return af
	}
	reg := NewAsyncPoolRegistry(factoryFor, quietConfig(), nil)

	for _, key := range []string{"a", "b"} {
		if err := reg.Use(ctx, key, Timeout{}, func(c *FakeConn) error { return nil }); err != nil {
			t.Fatalf("Use(%s): %v", key, err)
		}
	}

	if err := reg.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for key, f := range factories {
		if f.createdCount() == 0 {
			t.Fatalf("expected a connection created for key %s", key)
		}
		for _, c := range f.Created {
			if !c.Closed {
				t.Errorf("expected connection for key %s to be closed after registry Close", key)
			}
		}
	}
}

func TestAsyncRegistry_DoubleCloseIsSafe(t *testing.T) {
	ctx := context.Background()
	factoryFor := func(key string) AsyncConnectionFactory[*FakeConn] {
		return newAsyncFakeFactory()
	}
	reg := NewAsyncPoolRegistry(factoryFor, quietConfig(), nil)
	if err := reg.Use(ctx, "a", Timeout{}, func(c *FakeConn) error { return nil }); err != nil {
		t.Fatalf("Use: %v", err)
	}
	if err := reg.Close(ctx); err != nil {
		t.Fatalf("Close #1: %v", err)
	}
	if err := reg.Close(ctx); err != nil {
		t.Fatalf("Close #2 should be safe: %v", err)
	}
}

func TestAsyncRegistry_UseAfterCloseReturnsError(t *testing.T) {
	ctx := context.Background()
	factoryFor := func(key string) AsyncConnectionFactory[*FakeConn] {
		return newAsyncFakeFactory()
	}
	reg := NewAsyncPoolRegistry(factoryFor, quietConfig(), nil)
	if err := reg.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	err := reg.Use(ctx, "a", Timeout{}, func(c *FakeConn) error { return nil })
	if err == nil {
		t.Error("expected error using registry after Close")
	}
}

func TestAsyncRegistry_ContextCancellationPropagates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	factoryFor := func(key string) AsyncConnectionFactory[*FakeConn] {
		return newAsyncFakeFactory()
	}
	reg := NewAsyncPoolRegistry(factoryFor, quietConfig(), nil)
	defer reg.Close(context.Background())

	err := reg.Use(ctx, "a", Timeout{}, func(c *FakeConn) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}
