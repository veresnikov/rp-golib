package outbox

import (
	"context"
	"time"

	"gitea.xscloud.ru/xscloud/golib/pkg/infrastructure/mysql"
)

type storedEvent struct {
	Destination   string
	ID            uint64
	CorrelationID string
	EventType     string
	Payload       string
}

type eventStorage struct {
	uow         mysql.UnitOfWork
	lockTimeout time.Duration
}

func (s *eventStorage) append(ctx context.Context, event storedEvent) (err error) {
	return s.uow.ExecuteWithClientContext(ctx, func(client mysql.ClientContext) error {
		_, err = client.ExecContext(
			ctx,
			"INSERT INTO outbox_event (destination, correlation_id, event_type, payload) VALUES (?, ?, ?, ?)",
			event.Destination, event.CorrelationID, event.EventType, event.Payload,
		)
		return err
	})
}
