package mysql

import (
	"context"
	"fmt"

	commonerr "gitea.xscloud.ru/xscloud/golib/pkg/common/errors"
	"gitea.xscloud.ru/xscloud/golib/pkg/infrastructure/sharedpool"
)

type UnitOfWork interface {
	ExecuteWithUnitOfWork(ctx context.Context, callback func(client ClientContext) error) error
}

func NewUnitOfWork(connectionFactory ConnectionFactory) UnitOfWork {
	return &unitOfWork{
		pool: sharedpool.NewPool[context.Context, Transaction](
			func(key context.Context) (Transaction, sharedpool.WrappedValueReleaseFunc, error) {
				conn, err := connectionFactory.TransactionalConnection(key)
				if err != nil {
					return nil, nil, err
				}
				transaction, err := conn.BeginTransaction(key, nil)
				if err != nil {
					return nil, nil, err
				}

				wt := &wrappedTransaction{
					Transaction: transaction,
					state:       commit,
				}
				return wt, wt.Release, nil
			},
		),
	}
}

type unitOfWork struct {
	pool *sharedpool.Pool[context.Context, Transaction]
}

func (u unitOfWork) ExecuteWithUnitOfWork(ctx context.Context, callback func(client ClientContext) error) (err error) {
	transaction, err := u.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if r := recover(); r != nil {
			err = commonerr.Join(err, fmt.Errorf("panic: %v", r))
		}
		if err != nil {
			err = commonerr.Join(err, transaction.Value().Rollback())
		} else {
			err = transaction.Value().Commit()
		}
		err = commonerr.Join(err, transaction.Release())
	}()

	err = callback(transaction.Value())
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

func (w *wrappedTransaction) Commit() error {
	return nil
}

func (w *wrappedTransaction) Rollback() error {
	w.state = rollback
	return nil
}

func (w *wrappedTransaction) Release() error {
	switch w.state {
	case commit:
		return w.Transaction.Commit()
	case rollback:
		return w.Transaction.Rollback()
	}
	return nil
}
