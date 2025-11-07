package outboxmigrations

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"gitea.xscloud.ru/xscloud/golib/pkg/infrastructure/migrator"
	"gitea.xscloud.ru/xscloud/golib/pkg/infrastructure/mysql"
)

func newVersion1762551106(client mysql.ClientContext, transport string) migrator.Migration {
	return &version1762551106{
		client:    client,
		transport: transport,
	}
}

type version1762551106 struct {
	client    mysql.ClientContext
	transport string
}

func (v version1762551106) Version() int64 {
	return 1762551106
}

func (v version1762551106) Description() string {
	return fmt.Sprintf("Create 'outbox_%s_tracked_event' table", v.transport)
}

func (v version1762551106) Up(ctx context.Context) error {
	_, err := v.client.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE outbox_%s_tracked_event
		(
		    transport_name          VARBINARY(128)  NOT NULL,
		    last_tracked_event_id   BIGINT          NOT NULL,
		    PRIMARY KEY (transport_name)
		) 
		    ENGINE = InnoDB
		    CHARACTER SET = utf8mb4
		    COLLATE utf8mb4_unicode_ci
	`, v.transport))
	return errors.WithStack(err)
}
