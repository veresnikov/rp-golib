package mysql

import (
	"context"
	"fmt"

	"gitea.xscloud.ru/xscloud/golib/pkg/infrastructure/sharedpool"
	"gitea.xscloud.ru/xscloud/golib/pkg/internal/errors"
)

type RepositoryProviderBuilder[RepositoryProvider any] func(client ClientContext) RepositoryProvider

type UnitOfWork[RepositoryProvider any] interface {
	ExecuteWithUnitOfWork(ctx context.Context, callback func(provider RepositoryProvider) error) error
}

func NewUnitOfWork[RepositoryProvider any](
	pool ConnectionPool,
	builder RepositoryProviderBuilder[RepositoryProvider],
) UnitOfWork[RepositoryProvider] {
	return &unitOfWork[RepositoryProvider]{
		pool: sharedpool.NewPool[context.Context, *wrappedTransaction](
			func(ctx context.Context) (wt *wrappedTransaction, err error) {
				conn, err := pool.TransactionalConnection(ctx)
				if err != nil {
					return nil, err
				}

				defer func() {
					if err != nil {
						err = errors.Join(err, conn.Close())
					}
				}()

				transaction, err := conn.BeginTransaction(ctx, nil)
				if err != nil {
					return nil, err
				}

				wt = &wrappedTransaction{
					Transaction: transaction,
					state:       commit,
				}
				return wt, nil
			},
		),
		builder: builder,
	}
}

type unitOfWork[RepositoryProvider any] struct {
	pool    *sharedpool.Pool[context.Context, *wrappedTransaction]
	builder RepositoryProviderBuilder[RepositoryProvider]
}

func (uow unitOfWork[RepositoryProvider]) ExecuteWithUnitOfWork(ctx context.Context, callback func(provider RepositoryProvider) error) (err error) {
	sharedTransaction, err := uow.pool.Get(ctx)
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, sharedTransaction.Close())
	}()

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)

			defer func() {
				panic(r)
			}()
		}

		if err != nil {
			err = errors.Join(err, sharedTransaction.Value().Rollback())
		} else {
			err = errors.Join(err, sharedTransaction.Value().Commit())
		}
	}()

	err = callback(uow.builder(sharedTransaction.Value()))
	return err
}

const (
	commit = iota
	rollback
)

type wrappedTransaction struct {
	Transaction
	state int
}

func (wt *wrappedTransaction) Commit() error {
	return nil
}

func (wt *wrappedTransaction) Rollback() error {
	wt.state = rollback
	return nil
}

func (wt *wrappedTransaction) Close() error {
	var err error
	switch wt.state {
	case commit:
		err = wt.Transaction.Commit()
	case rollback:
		err = wt.Transaction.Rollback()
	}
	return err
}
