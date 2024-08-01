package mysql

import (
	"errors"
	"time"

	// include mysql driver
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

func NewConnector() Connector {
	return &connector{}
}

type Connector interface {
	Open(dsn string, cfg Config) error
	Close() error

	TransactionalClient() TransactionalClient
}

type Config struct {
	MaxConnections        int
	ConnectionMaxLifeTime time.Duration
	ConnectionMaxIdleTime time.Duration
}

type connector struct {
	db *sqlx.DB
}

func (c *connector) Open(dsn string, cfg Config) error {
	var err error
	c.db, err = sqlx.Open("mysql", dsn)
	if err != nil {
		return err
	}

	c.db.SetMaxOpenConns(cfg.MaxConnections)
	c.db.SetConnMaxLifetime(cfg.ConnectionMaxLifeTime)
	c.db.SetConnMaxIdleTime(cfg.ConnectionMaxIdleTime)

	pingError := c.db.Ping()
	if pingError != nil {
		err = c.db.Close()
		if err != nil {
			return err
		}
		return pingError
	}

	return nil
}

func (c *connector) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return errors.New("db not initialized")
}

func (c *connector) TransactionalClient() TransactionalClient {
	return &transactionalClient{c.db}
}
