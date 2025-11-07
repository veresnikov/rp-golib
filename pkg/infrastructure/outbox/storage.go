package outbox

import (
	"context"
	"fmt"

	"gitea.xscloud.ru/xscloud/golib/pkg/infrastructure/mysql"
)

type storedEvent struct {
	ID            uint64
	CorrelationID string
	EventType     string
	Payload       string
}

type eventStorage struct {
	uow       mysql.UnitOfWork
	transport string
}

func (s *eventStorage) append(ctx context.Context, event storedEvent) (err error) {
	return s.uow.ExecuteWithClientContext(ctx, func(client mysql.ClientContext) error {
		query := fmt.Sprintf(
			"INSERT INTO outbox_%s_event (correlation_id, event_type, payload) VALUES (?, ?, ?)",
			s.transport,
		)
		_, err = client.ExecContext(
			ctx,
			query,
			event.CorrelationID, event.EventType, event.Payload,
		)
		return err
	})
}
