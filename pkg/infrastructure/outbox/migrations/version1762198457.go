package outboxmigrations

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"gitea.xscloud.ru/xscloud/golib/pkg/infrastructure/migrator"
	"gitea.xscloud.ru/xscloud/golib/pkg/infrastructure/mysql"
)

func newVersion1762198457(client mysql.ClientContext, transport string) migrator.Migration {
	return &version1762198457{
		client:    client,
		transport: transport,
	}
}

type version1762198457 struct {
	client    mysql.ClientContext
	transport string
}

func (v version1762198457) Version() int64 {
	return 1762198457
}

func (v version1762198457) Description() string {
	return fmt.Sprintf("Create 'outbox_%s_event' table", v.transport)
}

func (v version1762198457) Up(ctx context.Context) error {
	_, err := v.client.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE outbox_%s_event
		(
		    event_id         BIGINT          NOT NULL AUTO_INCREMENT,
		    correlation_id   VARBINARY(128)  NOT NULL,
		    event_type       VARBINARY(128)  NOT NULL,
		    payload          TEXT            NOT NULL,
		    PRIMARY KEY (event_id)
		) 
		    ENGINE = InnoDB
		    CHARACTER SET = utf8mb4
		    COLLATE utf8mb4_unicode_ci
	`), v.transport)
	return errors.WithStack(err)
}
