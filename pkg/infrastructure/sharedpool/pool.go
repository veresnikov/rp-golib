package sharedpool

import (
	"errors"
	"sync"
)

type WrappedValueReleaseFunc func() error

type WrappedValue[V any] struct {
	value   V
	release WrappedValueReleaseFunc
}

type SharedValue[K comparable, V any] struct {
	wrappedValue *WrappedValue[V]

	key     K
	count   int
	release func(key K) error
}

func (v *SharedValue[K, V]) Value() V {
	return v.wrappedValue.value
}

func (v *SharedValue[K, V]) Release() error {
	return v.release(v.key)
}

type ValueFactory[K comparable, V any] func(key K) (V, WrappedValueReleaseFunc, error)

func NewPool[K comparable, V any](factory ValueFactory[K, V]) *Pool[K, V] {
	return &Pool[K, V]{
		valueFactory: factory,

		mu:   new(sync.Mutex),
		pool: make(map[K]*SharedValue[K, V]),
	}
}

type Pool[K comparable, V any] struct {
	valueFactory ValueFactory[K, V]

	mu   *sync.Mutex
	pool map[K]*SharedValue[K, V]
}

func (p *Pool[K, V]) Get(key K) (*SharedValue[K, V], error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	sv, ok := p.pool[key]
	if ok {
		sv.count++
	}
	if sv == nil {
		v, releaseFunc, err := p.valueFactory(key)
		if err != nil {
			return nil, err
		}
		sv = &SharedValue[K, V]{
			wrappedValue: &WrappedValue[V]{
				value:   v,
				release: releaseFunc,
			},
			key:     key,
			count:   1,
			release: p.release,
		}
		p.pool[key] = sv
	}
	return sv, nil
}

func (p *Pool[K, V]) release(key K) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	sv, ok := p.pool[key]
	if !ok {
		return errors.New("value not found in pool")
	}
	if sv.count == 1 {
		err := sv.wrappedValue.release()
		delete(p.pool, key)
		return err
	}
	sv.count--
	return nil
}
