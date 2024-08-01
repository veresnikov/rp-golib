package mysql

import (
	"context"
	"fmt"

	"gitea.xscloud.ru/xscloud/golib/pkg/common/errors"
	"gitea.xscloud.ru/xscloud/golib/pkg/infrastructure/sharedpool"

	liberrors "github.com/pkg/errors"
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
		pool: sharedpool.NewPool[context.Context, Transaction](
			func(ctx context.Context) (Transaction, sharedpool.WrappedValueReleaseFunc, error) {
				conn, err := pool.TransactionalConnection(ctx)
				if err != nil {
					return nil, nil, err
				}

				var err2 error
				defer func() {
					if err2 != nil {
						err2 = errors.Join(err2, conn.Close())
					}
				}()

				transaction, err2 := conn.BeginTransaction(ctx, nil)
				if err2 != nil {
					return nil, nil, err2
				}

				wt := &wrappedTransaction{
					Transaction: transaction,
					state:       commit,
				}
				return wt, func() error {
					return errors.Join(wt.release(), conn.Close())
				}, nil
			},
		),
		builder: builder,
	}
}

type unitOfWork[RepositoryProvider any] struct {
	pool    *sharedpool.Pool[context.Context, Transaction]
	builder RepositoryProviderBuilder[RepositoryProvider]
}

func (uow unitOfWork[RepositoryProvider]) ExecuteWithUnitOfWork(ctx context.Context, callback func(provider RepositoryProvider) error) (err error) {
	sharedTransaction, err := uow.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, sharedTransaction.Release())
	}()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
		if err != nil {
			err = errors.Join(err, sharedTransaction.Value().Rollback())
			return
		}
		err = errors.Join(err, sharedTransaction.Value().Commit())
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

func (wt *wrappedTransaction) release() error {
	var err error
	switch wt.state {
	case commit:
		err = wt.Transaction.Commit()
	case rollback:
		err = wt.Transaction.Rollback()
	}
	return liberrors.WithStack(err)
}
