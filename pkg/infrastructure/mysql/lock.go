package mysql

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

var (
	ErrLockTimeout   = errors.New("lock timed out")
	ErrLockNotLocked = errors.New("lock not locked")
	ErrLockNotFound  = errors.New("lock not found")
)

type Lock interface {
	Lock() error
	Unlock() error
}

func NewLock(ctx context.Context, lockName string, timeout time.Duration, client ClientContext) Lock {
	return &lock{
		ctx:      ctx,
		lockName: lockName,
		timeout:  timeout,
		client:   client,
	}
}

type lock struct {
	ctx      context.Context
	lockName string
	timeout  time.Duration
	client   ClientContext
}

func (l lock) Lock() error {
	const sqlQuery = "SELECT GET_LOCK(SUBSTRING(CONCAT(?, '.', DATABASE()), 1, 64), ?)"
	var result int
	err := l.client.GetContext(l.ctx, &result, sqlQuery, l.lockName, int(l.timeout.Seconds()))
	if result == 0 && err == nil {
		return ErrLockTimeout
	}
	return err
}

func (l lock) Unlock() (err error) {
	const sqlQuery = "SELECT RELEASE_LOCK(SUBSTRING(CONCAT(?, '.', DATABASE()), 1, 64))"
	var result sql.NullInt32
	err = l.client.GetContext(l.ctx, &result, sqlQuery, l.lockName)
	if err == nil {
		if !result.Valid {
			err = ErrLockNotFound
			return err
		}
		if result.Int32 == 0 {
			err = ErrLockNotLocked
			return err
		}
	}
	return err
}
