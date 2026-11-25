package ratebudget

import "sync"

// Group coalesces identical in-flight requests: the first caller runs fn and
// every caller with the same key waits for and shares its result. A key is
// free again once the call returns, so a later request runs fn again.
//
// The result is an any so the group carries no broker or HTTP type; callers
// type-assert their own response.
type Group struct {
	mu sync.Mutex
	m  map[string]*call
}

type call struct {
	done  chan struct{}
	value any
	err   error
	panic any
}

// Do runs fn once for every concurrent caller with key. A panic in fn is
// delivered to every caller after the in-flight entry is cleared, so a
// panicking call never wedges later requests.
func (g *Group) Do(key string, fn func() (any, error)) (any, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		<-c.done
		if c.panic != nil {
			panic(c.panic)
		}
		return c.value, c.err
	}
	c := &call{done: make(chan struct{})}
	g.m[key] = c
	g.mu.Unlock()

	func() {
		defer func() {
			if r := recover(); r != nil {
				c.panic = r
			}
		}()
		c.value, c.err = fn()
	}()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
	close(c.done)

	if c.panic != nil {
		panic(c.panic)
	}
	return c.value, c.err
}
