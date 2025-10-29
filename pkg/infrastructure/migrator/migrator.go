package migrator

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/pkg/errors"

	"gitea.xscloud.ru/xscloud/golib/pkg/application/logging"
	liberr "gitea.xscloud.ru/xscloud/golib/pkg/internal/errors"
)

type Migration interface {
	Version() int64
	Description() string
	Up(ctx context.Context) error
}

type Migrator interface {
	Migrate() error
}

func NewMigrator(
	ctx context.Context,
	storage *storage,
	locker *locker,
	logger logging.Logger,
	migrations []Migration,
) Migrator {
	slices.SortFunc(migrations, func(l, r Migration) int {
		return cmp.Compare(l.Version(), r.Version())
	})
	return &migrator{
		ctx:        ctx,
		storage:    storage,
		locker:     locker,
		logger:     logger,
		migrations: migrations,
	}
}

type migrator struct {
	ctx context.Context

	storage *storage
	locker  *locker
	logger  logging.Logger

	migrations []Migration
}

func (m migrator) Migrate() (err error) {
	err = m.locker.Lock(m.ctx)
	if err != nil {
		return err
	}
	defer func() {
		if r := recover(); r != nil {
			err = liberr.Join(err, fmt.Errorf("panic: %v", r))
		}
		err = liberr.Join(err, m.locker.Unlock())
	}()

	err = m.storage.Init(m.ctx)
	if err != nil {
		return err
	}
	lastVersion, err := m.storage.LastVersion(m.ctx)
	if err != nil {
		return err
	}

	for _, migration := range m.migrations {
		var applied bool
		applied, err = m.storage.Applied(m.ctx, migration.Version())
		if err != nil {
			return err
		}
		if applied {
			m.logger.Info(fmt.Sprintf("migration '%v' already applied", migration.Version()))
			continue
		}
		if migration.Version() < lastVersion {
			return errors.Errorf("migration version %v less then last applied %v", migration.Version(), lastVersion)
		}
		err = migration.Up(m.ctx)
		if err != nil {
			return err
		}
		m.logger.Info(fmt.Sprintf("migration '%v' successfully applied", migration.Version()))
		err = m.storage.Store(m.ctx, migration)
		if err != nil {
			return err
		}
	}
	return err
}
