package sharedpool

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

type someValue struct {
	ctx    context.Context
	closed bool
}

func (s *someValue) Close() error {
	if s.closed {
		return errors.New("already closed")
	}
	s.closed = true
	return nil
}

type somePool struct {
	pool *Pool[context.Context, io.Closer]
}

func (sp *somePool) Get(ctx context.Context) (io.Closer, error) {
	sharedValue, err := sp.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	return &wrappedSomeValue{
		Closer:      sharedValue.Value(),
		releaseFunc: sharedValue.Close,
	}, nil
}

type wrappedSomeValue struct {
	io.Closer
	releaseFunc func() error
}

func (v *wrappedSomeValue) Close() error {
	return v.releaseFunc()
}

func TestSharedPool(t *testing.T) {
	t.Run("reusing some value", func(t *testing.T) {
		ctx := t.Context()
		sv := &someValue{ctx: ctx}
		sp := &somePool{
			pool: NewPool[context.Context, io.Closer](
				func(_ context.Context) (io.Closer, error) {
					return sv, nil
				},
			),
		}

		v1, err := sp.Get(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, sp.pool.pool[ctx].count)

		v2, err := sp.Get(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 2, sp.pool.pool[ctx].count)

		assert.NoError(t, v1.Close())
		assert.Equal(t, false, sv.closed)

		assert.NoError(t, v2.Close())
		assert.Equal(t, true, sv.closed)
	})
}
