package mysql

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gitea.xscloud.ru/xscloud/golib/pkg/infrastructure/sharedpool"
	"gitea.xscloud.ru/xscloud/golib/pkg/internal/errors"
)

type Locker interface {
	ExecuteWithLock(ctx context.Context, lockName string, lockTimeout time.Duration, callback func() error) error
}

func NewLocker(pool ConnectionPool) Locker {
	return &locker{
		pool: sharedpool.NewPool[context.Context, *wrappedLockedConnection](
			func(ctx context.Context) (*wrappedLockedConnection, error) {
				conn, err := pool.TransactionalConnection(ctx)
				if err != nil {
					return nil, err
				}

				wc := &wrappedLockedConnection{
					TransactionalConnection: conn,
				}
				return wc, nil
			},
		),
	}
}

type locker struct {
	pool *sharedpool.Pool[context.Context, *wrappedLockedConnection]
}

func (l locker) ExecuteWithLock(ctx context.Context, lockName string, lockTimeout time.Duration, callback func() error) (err error) {
	wc, err := l.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, wc.Close())
	}()

	err = wc.Value().appendLock(ctx, lockName, lockTimeout)
	if err != nil {
		return err
	}

	defer func() {
		if r := recover(); r != nil {
			err = errors.Join(err, fmt.Errorf("panic: %v", r))
			panic(r)
		}
	}()

	err = callback()
	return err
}

type wrappedLockedConnection struct {
	TransactionalConnection

	mu    sync.Mutex
	locks []Lock
}

func (wc *wrappedLockedConnection) appendLock(ctx context.Context, lockName string, lockTimeout time.Duration) error {
	wc.mu.Lock()
	defer wc.mu.Unlock()

	lock := NewLock(ctx, lockName, lockTimeout, wc)
	err := lock.Lock()
	if err != nil {
		return err
	}
	wc.locks = append(wc.locks, lock)
	return nil
}

func (wc *wrappedLockedConnection) Close() error {
	wc.mu.Lock()
	defer wc.mu.Unlock()

	var err error
	for _, lock := range wc.locks {
		err = errors.Join(err, lock.Unlock())
	}
	return errors.Join(err, wc.TransactionalConnection.Close())
}
