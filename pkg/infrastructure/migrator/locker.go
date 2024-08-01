package migrator

import (
	"context"
	"time"

	"gitea.xscloud.ru/xscloud/golib/pkg/infrastructure/mysql"

	"github.com/pkg/errors"
)

const (
	migrationLockName    = "migration"
	migrationLockTimeout = time.Second * 5
)

func newLocker(client mysql.ClientContext) *locker {
	return &locker{
		client: client,
	}
}

type locker struct {
	client mysql.ClientContext
	lock   mysql.Lock
}

func (m *locker) Lock(ctx context.Context) error {
	if m.lock == nil {
		m.lock = mysql.NewLock(ctx, migrationLockName, migrationLockTimeout, m.client)
	}
	return errors.WithStack(m.lock.Lock())
}

func (m *locker) Unlock() error {
	if m.lock == nil {
		return errors.New("migration locker is nil")
	}
	return errors.WithStack(m.lock.Unlock())
}
