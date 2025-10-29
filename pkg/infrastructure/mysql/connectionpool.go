package mysql

import (
	"context"

	"gitea.xscloud.ru/xscloud/golib/pkg/infrastructure/sharedpool"
)

type ConnectionPool interface {
	TransactionalConnection(ctx context.Context) (TransactionalConnection, error)
}

func NewConnectionPool(client TransactionalClient) ConnectionPool {
	return &connectionPool{
		pool: sharedpool.NewPool[context.Context, TransactionalConnection](client.Connection),
	}
}

type connectionPool struct {
	pool *sharedpool.Pool[context.Context, TransactionalConnection]
}

func (f *connectionPool) TransactionalConnection(ctx context.Context) (TransactionalConnection, error) {
	sharedConnection, err := f.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	return &wrappedTransactionalConnection{
		TransactionalConnection: sharedConnection.Value(),
		releaseFunc:             sharedConnection.Close,
	}, nil
}

type wrappedTransactionalConnection struct {
	TransactionalConnection
	releaseFunc func() error
}

func (conn *wrappedTransactionalConnection) Close() error {
	return conn.releaseFunc()
}
