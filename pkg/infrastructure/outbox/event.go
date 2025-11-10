package outbox

type storedEvent struct {
	EventID       uint64 `db:"event_id"`
	CorrelationID string `db:"correlation_id"`
	EventType     string `db:"event_type"`
	Payload       string `db:"payload"`
}
