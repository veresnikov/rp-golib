package mysql

import (
	"context"
	"time"
)

type LockableUnitOfWork[RepositoryProvider any] interface {
	ExecuteWithLockableUnitOfWork(ctx context.Context, lockName string, lockTimeout time.Duration, callback func(provider RepositoryProvider) error) error
}

func NewLockableUnitOfWork[RepositoryProvider any](
	unitOfWork UnitOfWork[RepositoryProvider],
	locker Locker,
) LockableUnitOfWork[RepositoryProvider] {
	return &lockableUnitOfWork[RepositoryProvider]{
		unitOfWork: unitOfWork,
		locker:     locker,
	}
}

type lockableUnitOfWork[RepositoryProvider any] struct {
	unitOfWork UnitOfWork[RepositoryProvider]
	locker     Locker
}

func (uow lockableUnitOfWork[RepositoryProvider]) ExecuteWithLockableUnitOfWork(ctx context.Context, lockName string, lockTimeout time.Duration, callback func(provider RepositoryProvider) error) error {
	return uow.locker.ExecuteWithLock(ctx, lockName, lockTimeout, func() error {
		return uow.unitOfWork.ExecuteWithUnitOfWork(ctx, callback)
	})
}
