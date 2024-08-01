package migrator

import (
	"context"

	"gitea.xscloud.ru/xscloud/golib/pkg/application/logging"
	"gitea.xscloud.ru/xscloud/golib/pkg/infrastructure/mysql"

	"github.com/pkg/errors"
)

type Factory interface {
	NewMigrator(ctx context.Context, migrations ...Migration) (Migrator, error)
}

func NewMigratorFactory(tablePrefix string, client mysql.ClientContext, logger logging.Logger) Factory {
	return &migratorFactory{
		tablePrefix: tablePrefix,
		client:      client,
		logger:      logger,
	}
}

type migratorFactory struct {
	tablePrefix string
	client      mysql.ClientContext
	logger      logging.Logger
}

func (factory migratorFactory) NewMigrator(ctx context.Context, migrations ...Migration) (Migrator, error) {
	if len(migrations) == 0 {
		return nil, errors.New("migrations must not be empty")
	}
	migrator := NewMigrator(
		ctx,
		newStorage(factory.tablePrefix, factory.client),
		newLocker(factory.client),
		factory.logger,
		migrations,
	)
	return migrator, nil
}
