package outbox

import "context"

type Event interface {
	Type() string
}

type EventSerializer[E Event] interface {
	Serialize(event E) (string, error)
}

type EventDispatcher[E Event] interface {
	Dispatch(ctx context.Context, event E) error
}
