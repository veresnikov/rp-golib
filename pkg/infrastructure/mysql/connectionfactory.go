package mysql

import (
	"context"

	"golib/pkg/infrastructure/sharedpool"
)

type ConnectionFactory interface {
	TransactionalConnection(ctx context.Context) (TransactionalConnection, error)
}

func NewConnectionFactory(client TransactionalClient) ConnectionFactory {
	return &connectionFactory{
		pool: sharedpool.NewPool[context.Context, TransactionalConnection](
			func(ctx context.Context) (TransactionalConnection, sharedpool.WrappedValueReleaseFunc, error) {
				conn, err := client.Connection(ctx)
				if err != nil {
					return nil, nil, err
				}
				return conn, conn.Close, nil
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
