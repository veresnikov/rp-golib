package outbox

import (
	"context"
	"database/sql"
	"fmt"
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
	uow         mysql.LockableUnitOfWork
	lockTimeout time.Duration
}

func (s *eventStorage) append(ctx context.Context, event storedEvent) (err error) {
	return s.uow.ExecuteWithClientContext(ctx, lockName(event.Destination), s.lockTimeout, func(client mysql.ClientContext) error {
		var currentID sql.NullInt64
		err = client.GetContext(
			ctx, &currentID,
			"SELECT MAX(event_id) FROM outbox_event WHERE destination = ?",
			event.Destination,
		)
		if err != nil {
			return err
		}
		var eventID uint64
		if currentID.Valid {
			eventID = uint64(currentID.Int64 + 1) // nolint gosec
		} else {
			eventID = 1
		}

		_, err = client.ExecContext(
			ctx,
			"INSERT INTO outbox_event (destination, event_id, correlation_id, event_type, payload) VALUES (?, ?, ?, ?, ?)",
			event.Destination, eventID, event.CorrelationID, event.EventType, event.Payload,
		)
		return err
	})
}

func lockName(destination string) string {
	return fmt.Sprintf("outbox_%s_lock", destination)
}
