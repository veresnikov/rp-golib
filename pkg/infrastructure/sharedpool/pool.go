package sharedpool

import (
	"errors"
	"io"
	"sync"
)

type SharedValue[K comparable, V io.Closer] struct {
	v V

	key     K
	count   int
	release func(key K) error
}

func (v *SharedValue[K, V]) Value() V {
	return v.v
}

func (v *SharedValue[K, V]) Close() error {
	return v.release(v.key)
}

type ValueFactory[K comparable, V io.Closer] func(key K) (V, error)

func NewPool[K comparable, V io.Closer](factory ValueFactory[K, V]) *Pool[K, V] {
	return &Pool[K, V]{
		valueFactory: factory,
		pool:         make(map[K]*SharedValue[K, V]),
	}
}

type Pool[K comparable, V io.Closer] struct {
	valueFactory ValueFactory[K, V]

	mu   sync.Mutex
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
		v, err := p.valueFactory(key)
		if err != nil {
			return nil, err
		}
		sv = &SharedValue[K, V]{
			v:       v,
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
		err := sv.v.Close()
		delete(p.pool, key)
		return err
	}
	sv.count--
	return nil
}
