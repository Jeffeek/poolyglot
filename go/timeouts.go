package connpool

type timeoutKind uint8

const (
	tkUnset    timeoutKind = iota // use the configured default
	tkInfinite                    // wait forever
	tkOf                          // wait a specific number of seconds (0 = don't wait)
)

// Timeout is the per-call acquire timeout argument. It is one of three things:
//
//	Unset()        -> use config.AcquireTimeout
//	Infinite()     -> wait forever (also OfNullable(nil))
//	Of(seconds)    -> wait that long (0 = don't wait)
type Timeout struct {
	kind timeoutKind
	secs float64
}

// Unset means "use the configured default".
func Unset() Timeout { return Timeout{kind: tkUnset} }

// Infinite means "wait forever".
func Infinite() Timeout { return Timeout{kind: tkInfinite} }

// Of returns a timeout of the given seconds (0 = don't wait).
func Of(seconds float64) Timeout { return Timeout{kind: tkOf, secs: seconds} }

// OfNullable maps nil -> Infinite and a non-nil pointer -> Of(*seconds).
func OfNullable(seconds *float64) Timeout {
	if seconds == nil {
		return Infinite()
	}
	return Of(*seconds)
}

// computeDeadline resolves a Timeout against a default (nil = forever) at time now.
// Returns nil for "no deadline / wait forever".
func computeDeadline(t Timeout, def *float64, now float64) *float64 {
	switch t.kind {
	case tkUnset:
		if def == nil {
			return nil
		}
		d := now + *def
		return &d
	case tkInfinite:
		return nil
	default: // tkOf
		d := now + t.secs
		return &d
	}
}

// remaining returns the seconds left until deadline (nil = no deadline).
func remaining(deadline *float64, now float64) *float64 {
	if deadline == nil {
		return nil
	}
	r := *deadline - now
	return &r
}
