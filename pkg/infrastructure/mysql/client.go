package mysql

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

type ClientContext interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)

	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

type Transaction interface {
	ClientContext
	Commit() error
	Rollback() error
}

type TransactionalConnection interface {
	ClientContext
	BeginTransaction(ctx context.Context, opts *sql.TxOptions) (Transaction, error)
	Close() error
}

type TransactionalClient interface {
	ClientContext
	BeginTransaction() (Transaction, error)
	Connection(ctx context.Context) (TransactionalConnection, error)
}

type transactionalClient struct {
	*sqlx.DB
}

func (client *transactionalClient) BeginTransaction() (Transaction, error) {
	return client.Beginx()
}

func (client *transactionalClient) Connection(ctx context.Context) (TransactionalConnection, error) {
	connx, err := client.Connx(ctx)
	if err != nil {
		return nil, err
	}
	return &transactionalConnection{Conn: connx}, nil
}

type transactionalConnection struct {
	*sqlx.Conn
}

func (conn *transactionalConnection) BeginTransaction(ctx context.Context, opts *sql.TxOptions) (Transaction, error) {
	return conn.BeginTxx(ctx, opts)
}
