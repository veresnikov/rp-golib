package migrator

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"gitea.xscloud.ru/xscloud/golib/pkg/infrastructure/mysql"

	"github.com/pkg/errors"
)

func newStorage(
	tablePrefix string,
	client mysql.ClientContext,
) *storage {
	return &storage{
		tablePrefix: tablePrefix,
		client:      client,
	}
}

type storage struct {
	tablePrefix string
	client      mysql.ClientContext
}

func (storage *storage) Init(ctx context.Context) (err error) {
	const checkExistSQLQuery = `
		SELECT EXISTS(
    	   SELECT * FROM information_schema.tables 
    	   WHERE table_schema = DATABASE() 
    	   AND table_name = ?
		)
	`
	var exists bool
	err = storage.client.GetContext(ctx, &exists, checkExistSQLQuery, storage.tableName())
	if err != nil {
		err = errors.WithStack(err)
		return err
	}

	if exists {
		return nil
	}

	const createMigrationsTableSQLQuery = `
		CREATE TABLE %table_name%
		(
		    version     BIGINT   NOT NULL,
		    description TEXT     NOT NULL,
		    applied_at  DATETIME NOT NULL,
		    PRIMARY KEY (version)
		) 
    		ENGINE = InnoDB
    		CHARACTER SET = utf8mb4
    		COLLATE utf8mb4_unicode_ci
	`
	_, err = storage.client.ExecContext(ctx, prepareQuery(createMigrationsTableSQLQuery, storage.tableName()))
	err = errors.WithStack(err)
	return err
}

func (storage *storage) LastVersion(ctx context.Context) (int64, error) {
	const lastVersionSQLQuery = `SELECT MAX(version) FROM %table_name%`
	var version sql.NullInt64
	err := storage.client.GetContext(ctx, &version, prepareQuery(lastVersionSQLQuery, storage.tableName()))
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if !version.Valid {
		return 0, nil
	}
	return version.Int64, nil
}

func (storage *storage) Applied(ctx context.Context, version int64) (bool, error) {
	const appliedSQLQuery = `SELECT EXISTS(SELECT version FROM %table_name% WHERE version = ?)`
	var applied bool
	err := storage.client.GetContext(ctx, &applied, prepareQuery(appliedSQLQuery, storage.tableName()), version)
	return applied, errors.WithStack(err)
}

func (storage *storage) Store(ctx context.Context, migration Migration) error {
	const storeSQLQuery = `INSERT INTO %table_name% (version, description, applied_at) VALUES(?, ?, ?)`
	_, err := storage.client.ExecContext(ctx, prepareQuery(storeSQLQuery, storage.tableName()), migration.Version(), migration.Description(), time.Now())
	return errors.WithStack(err)
}

func (storage *storage) tableName() string {
	return storage.tablePrefix + "_migrations"
}

func prepareQuery(query, tableName string) string {
	return strings.ReplaceAll(query, "%table_name%", tableName)
}
