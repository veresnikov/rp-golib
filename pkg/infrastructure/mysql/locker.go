package mysql

import (
	"context"
	"time"

	commonerr "gitea.xscloud.ru/xscloud/golib/pkg/common/errors"
	"gitea.xscloud.ru/xscloud/golib/pkg/infrastructure/sharedpool"
)

type Locker interface {
	ExecuteWithLock(ctx context.Context, lockName string, timeout time.Duration, client ClientContext, callback func() error) error
}

func NewLocker() Locker {
	return &locker{
		pool: sharedpool.NewPool[context.Context, *wrappedLock](
			func(_ context.Context) (*wrappedLock, sharedpool.WrappedValueReleaseFunc, error) {
				wl := &wrappedLock{
					state: unlocked,
				}
				return wl, wl.Unlock, nil
			},
		),
	}
}

type locker struct {
	pool *sharedpool.Pool[context.Context, *wrappedLock]
}

func (l locker) ExecuteWithLock(ctx context.Context, lockName string, timeout time.Duration, client ClientContext, callback func() error) error {
	lock, err := l.pool.Get(ctx)
	if err != nil {
		return err
	}
	if lock.Value().lock == nil {
		lock.Value().lock = NewLock(ctx, lockName, timeout, client)
		err = lock.Value().lock.Lock()
		if err != nil {
			return err
		}
	}
	return commonerr.Join(callback(), lock.Release())
}

const (
	unlocked = iota
	locked
)

type wrappedLock struct {
	lock  Lock
	state int
}

func (wl *wrappedLock) Lock() error {
	wl.state = locked
	if wl.lock != nil && wl.state == unlocked {
		return wl.lock.Lock()
	}
	return nil
}

func (wl *wrappedLock) Unlock() error {
	if wl.lock != nil && wl.state == locked {
		wl.state = unlocked
		return wl.lock.Unlock()
	}
	return nil
}
