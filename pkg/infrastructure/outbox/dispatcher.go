package outbox

import (
	"context"

	"gitea.xscloud.ru/xscloud/golib/pkg/application/outbox"
	"gitea.xscloud.ru/xscloud/golib/pkg/infrastructure/mysql"
)

func NewEventDispatcher[E outbox.Event](
	appID string,
	transport string,
	serializer outbox.EventSerializer[E],
	uow mysql.UnitOfWork,
) outbox.EventDispatcher[E] {
	if transport == "" {
		panic("transport cannot be empty")
	}

	return &eventDispatcher[E]{
		appID:      appID,
		serializer: serializer,
		storage: &eventStorage{
			uow:       uow,
			transport: transport,
		},
	}
}

type eventDispatcher[E outbox.Event] struct {
	appID      string
	serializer outbox.EventSerializer[E]

	storage *eventStorage
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

	return d.storage.append(ctx, storedEvent{
		CorrelationID: correlationID,
		EventType:     event.Type(),
		Payload:       msg,
	})
}
