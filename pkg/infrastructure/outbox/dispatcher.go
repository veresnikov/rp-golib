package outbox

import (
	"context"
	"time"

	"gitea.xscloud.ru/xscloud/golib/pkg/application/outbox"
	"gitea.xscloud.ru/xscloud/golib/pkg/infrastructure/mysql"
)

func NewEventDispatcher[E outbox.Event, RepositoryProvider any](
	appID string,
	serializer outbox.EventSerializer[E],
	uow mysql.LockableUnitOfWork[RepositoryProvider],
	lockTimeout time.Duration,
) outbox.EventDispatcher[E] {
	return &eventDispatcher[E, RepositoryProvider]{
		appID:      appID,
		serializer: serializer,
		storage: &eventStorage[RepositoryProvider]{
			uow:         uow,
			lockTimeout: lockTimeout,
		},
	}
}

type eventDispatcher[E outbox.Event, RepositoryProvider any] struct {
	appID      string
	serializer outbox.EventSerializer[E]

	storage *eventStorage[RepositoryProvider]
}

func (d *eventDispatcher[E, RepositoryProvider]) Dispatch(ctx context.Context, destination string, event E) error {
	msg, err := d.serializer.Serialize(event)
	if err != nil {
		return err
	}

	correlationID, err := newCorrelationID(d.appID, msg)
	if err != nil {
		return err
	}

	return d.storage.append(ctx, storedEvent{
		Destination:   destination,
		CorrelationID: correlationID,
		EventType:     event.Type(),
		Payload:       msg,
	})
}
