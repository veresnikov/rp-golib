package mysql

import (
	"context"
	"fmt"
	"time"

	"gitea.xscloud.ru/xscloud/golib/pkg/common/errors"
	"gitea.xscloud.ru/xscloud/golib/pkg/infrastructure/sharedpool"

	liberrors "github.com/pkg/errors"
)

type Locker interface {
	ExecuteWithLock(ctx context.Context, lockName string, lockTimeout time.Duration, callback func() error) error
}

func NewLocker(pool ConnectionPool) Locker {
	return &locker{
		pool: sharedpool.NewPool[context.Context, *wrappedConnection](
			func(ctx context.Context) (*wrappedConnection, sharedpool.WrappedValueReleaseFunc, error) {
				conn, err := pool.TransactionalConnection(ctx)
				if err != nil {
					return nil, nil, err
				}
				wc := &wrappedConnection{
					TransactionalConnection: conn,
				}
				return wc, wc.release, nil
			},
		),
	}
}

type locker struct {
	pool *sharedpool.Pool[context.Context, *wrappedConnection]
}

func (l locker) ExecuteWithLock(ctx context.Context, lockName string, lockTimeout time.Duration, callback func() error) (err error) {
	wc, err := l.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, wc.Release())
	}()
	err = wc.Value().appendLock(ctx, lockName, lockTimeout)
	if err != nil {
		return err
	}

	defer func() {
		if r := recover(); r != nil {
			err = errors.Join(err, fmt.Errorf("panic: %v", r))
		}
	}()
	err = callback()
	return err
}

type wrappedConnection struct {
	TransactionalConnection
	locks []Lock
}

func (wc *wrappedConnection) Close() error {
	return nil
}

func (wc *wrappedConnection) appendLock(ctx context.Context, lockName string, lockTimeout time.Duration) error {
	lock := NewLock(ctx, lockName, lockTimeout, wc)
	err := lock.Lock()
	if err != nil {
		return liberrors.WithStack(err)
	}
	wc.locks = append(wc.locks, lock)
	return nil
}

func (wc *wrappedConnection) release() error {
	var err error
	for _, lock := range wc.locks {
		err = errors.Join(err, liberrors.WithStack(lock.Unlock()))
	}
	return errors.Join(err, wc.TransactionalConnection.Close())
}
