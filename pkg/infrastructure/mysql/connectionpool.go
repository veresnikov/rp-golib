package mysql

import (
	"context"

	"gitea.xscloud.ru/xscloud/golib/pkg/infrastructure/sharedpool"

	"github.com/pkg/errors"
)

type ConnectionPool interface {
	TransactionalConnection(ctx context.Context) (TransactionalConnection, error)
}

func NewConnectionFactory(client TransactionalClient) ConnectionPool {
	return &connectionFactory{
		pool: sharedpool.NewPool[context.Context, TransactionalConnection](
			func(ctx context.Context) (TransactionalConnection, sharedpool.WrappedValueReleaseFunc, error) {
				conn, err := client.Connection(ctx)
				if err != nil {
					return nil, nil, errors.WithStack(err)
				}
				return conn, func() error {
					return errors.WithStack(conn.Close())
				}, nil
			},
		),
	}
}

type connectionFactory struct {
	pool *sharedpool.Pool[context.Context, TransactionalConnection]
}

func (f *connectionFactory) TransactionalConnection(ctx context.Context) (TransactionalConnection, error) {
	sharedConnection, err := f.pool.Get(ctx)
	if err != nil {
		return nil, err
	}

	return &wrappedTransactionalConnection{
		TransactionalConnection: sharedConnection.Value(),
		releaseFunc:             sharedConnection.Release,
	}, nil
}

type wrappedTransactionalConnection struct {
	TransactionalConnection
	releaseFunc func() error
}

func (conn *wrappedTransactionalConnection) Close() error {
	return conn.releaseFunc()
}
