package mysql

import (
	"context"
	"time"
)

type LockableUnitOfWork[RepositoryProvider any] interface {
	ExecuteWithClientContext(ctx context.Context, lockName string, lockTimeout time.Duration, callback func(client ClientContext) error) error
	ExecuteWithRepositoryProvider(ctx context.Context, lockName string, lockTimeout time.Duration, callback func(provider RepositoryProvider) error) error
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

func (uow lockableUnitOfWork[RepositoryProvider]) ExecuteWithClientContext(ctx context.Context, lockName string, lockTimeout time.Duration, callback func(client ClientContext) error) error {
	return uow.locker.ExecuteWithLock(ctx, lockName, lockTimeout, func() error {
		return uow.unitOfWork.ExecuteWithClientContext(ctx, callback)
	})
}
func (uow lockableUnitOfWork[RepositoryProvider]) ExecuteWithRepositoryProvider(ctx context.Context, lockName string, lockTimeout time.Duration, callback func(provider RepositoryProvider) error) error {
	return uow.locker.ExecuteWithLock(ctx, lockName, lockTimeout, func() error {
		return uow.unitOfWork.ExecuteWithRepositoryProvider(ctx, callback)
	})
}
