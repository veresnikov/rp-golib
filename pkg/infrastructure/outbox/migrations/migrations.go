package outboxmigrations

import (
	"context"
	"errors"
	"fmt"

	"gitea.xscloud.ru/xscloud/golib/pkg/application/logging"
	"gitea.xscloud.ru/xscloud/golib/pkg/common/io"
	libmigrator "gitea.xscloud.ru/xscloud/golib/pkg/infrastructure/migrator"
	"gitea.xscloud.ru/xscloud/golib/pkg/infrastructure/mysql"
)

func NewOutboxMigrator(
	ctx context.Context,
	pool mysql.ConnectionPool,
	logger logging.Logger,
	transport string,
) (migrator libmigrator.Migrator, release io.CloserFunc, err error) {
	if transport == "" {
		panic("transport cannot be empty")
	}

	conn, err2 := pool.TransactionalConnection(ctx)
	if err2 != nil {
		return nil, nil, err2
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, conn.Close())
		}
	}()

	tablePrefix := fmt.Sprintf("outbox_%s", transport)

	l := logger.WithField("migrator", tablePrefix)
	factory := libmigrator.NewMigratorFactory(tablePrefix, conn, l)

	migrations := make([]libmigrator.Migration, 0, len(builderFunctions))
	for _, builder := range builderFunctions {
		migrations = append(migrations, builder(conn))
	}

	migrator, err = factory.NewMigrator(ctx, migrations...)
	if err != nil {
		return nil, nil, err
	}
	return migrator, conn.Close, nil
}

var builderFunctions = []func(client mysql.ClientContext, transport string) libmigrator.Migration{
	newVersion1762198457,
}
