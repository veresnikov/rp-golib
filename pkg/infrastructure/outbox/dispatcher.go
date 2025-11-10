package outbox

import (
	"context"
	"fmt"

	"gitea.xscloud.ru/xscloud/golib/pkg/application/outbox"
	"gitea.xscloud.ru/xscloud/golib/pkg/infrastructure/mysql"
)

func NewEventDispatcher[E outbox.Event](
	appID string,
	transportName string,
	serializer outbox.EventSerializer[E],
	uow mysql.UnitOfWork,
) outbox.EventDispatcher[E] {
	if transportName == "" {
		panic("transport name cannot be empty")
	}

	return &eventDispatcher[E]{
		appID:         appID,
		transportName: transportName,
		serializer:    serializer,
		uow:           uow,
	}
}

type eventDispatcher[E outbox.Event] struct {
	appID         string
	transportName string
	serializer    outbox.EventSerializer[E]
	uow           mysql.UnitOfWork
}

func (d *eventDispatcher[E]) Dispatch(ctx context.Context, event E) error {
	msg, err := d.serializer.Serialize(event)
	if err != nil {
		return err
	}

	correlationID, err := newCorrelationID(d.appID, msg)
	if err != nil {
		return err
	}

	return d.append(ctx, storedEvent{
		CorrelationID: correlationID,
		EventType:     event.Type(),
		Payload:       msg,
	})
}

func (d *eventDispatcher[E]) append(ctx context.Context, event storedEvent) (err error) {
	return d.uow.ExecuteWithClientContext(ctx, func(client mysql.ClientContext) error {
		query := fmt.Sprintf(
			"INSERT INTO outbox_%s_event (correlation_id, event_type, payload) VALUES (?, ?, ?)",
			d.transportName,
		)
		_, err = client.ExecContext(
			ctx,
			query,
			event.CorrelationID, event.EventType, event.Payload,
		)
		return err
	})
}
