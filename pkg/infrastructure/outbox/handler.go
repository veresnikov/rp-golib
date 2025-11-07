package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"gitea.xscloud.ru/xscloud/golib/pkg/application/logging"
	"gitea.xscloud.ru/xscloud/golib/pkg/infrastructure/mysql"
	liberr "gitea.xscloud.ru/xscloud/golib/pkg/internal/errors"
)

type Event struct {
	CorrelationID string
	EventType     string
	Payload       string
}

type Transport interface {
	HandleEvents(ctx context.Context, events []Event) error
}

type Handler interface {
	Start(ctx context.Context) error
}

func NewEventHandler(
	transportName string,
	transport Transport,
	batchSize uint,
	sendInterval time.Duration,
	lockTimeout time.Duration,
	pool mysql.ConnectionPool,
	logger logging.Logger,
) Handler {
	return &handler{
		transportName: transportName,
		transport:     transport,
		batchSize:     batchSize,
		sendInterval:  sendInterval,
		pool:          pool,
		lockTimeout:   lockTimeout,
		locker:        mysql.NewLocker(pool),
		logger:        logger,
	}
}

type handler struct {
	transportName string
	transport     Transport
	batchSize     uint
	sendInterval  time.Duration

	pool        mysql.ConnectionPool
	lockTimeout time.Duration
	locker      mysql.Locker
	logger      logging.Logger
}

func (h handler) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(h.sendInterval):
		default:
		}

		err := h.sendEvents(context.Background())
		if err != nil {
			return err
		}
	}
}

func (h handler) sendEvents(ctx context.Context) error {
	return h.locker.ExecuteWithLock(ctx, h.lockName(), h.lockTimeout, func() error {
		conn, err := h.pool.TransactionalConnection(ctx)
		if err != nil {
			return err
		}

		lastTrackedEvent, err := h.lastTrackedEvent(ctx, conn)
		if err != nil {
			return err
		}

		commitedEvents, err := h.unhandledEvents(ctx, conn, lastTrackedEvent, false)
		if err != nil {
			return err
		}
		if len(commitedEvents) == 0 {
			return nil
		}

		uncommitedEvents, err := h.unhandledEvents(ctx, conn, lastTrackedEvent, true)
		if err != nil {
			return err
		}

		var events []Event
		lastHandledEvent := lastTrackedEvent
		for i := 0; i < len(commitedEvents); i++ {
			if uncommitedEvents[i].EventID != commitedEvents[i].EventID {
				break
			}

			events = append(events, Event{
				CorrelationID: commitedEvents[i].CorrelationID,
				EventType:     commitedEvents[i].EventType,
				Payload:       commitedEvents[i].Payload,
			})
			lastHandledEvent = commitedEvents[i].EventID
		}

		err2 := h.transport.HandleEvents(ctx, events)
		if err2 != nil {
			h.logger.Error(err2)
			return nil
		}

		return h.trackLastHandledEvent(ctx, conn, lastHandledEvent)
	})
}

func (h handler) lastTrackedEvent(ctx context.Context, client mysql.ClientContext) (uint64, error) {
	var lastEventID uint64
	err := client.GetContext(ctx, &lastEventID, fmt.Sprintf(`
		SELECT last_tracked_event_id FROM outbox_%s_tracked_event WHERE transport_name = ?
	`, h.transportName), h.transportName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return lastEventID, nil
}

func (h handler) unhandledEvents(ctx context.Context, conn mysql.TransactionalConnection, lastTracked uint64, includeUncommited bool) ([]storedEvent, error) {
	var (
		client mysql.ClientContext = conn
		err    error
	)
	if includeUncommited {
		tx, err := conn.BeginTransaction(ctx, &sql.TxOptions{
			Isolation: sql.LevelReadUncommitted,
			ReadOnly:  true,
		})
		if err != nil {
			return nil, err
		}
		defer func() {
			err = liberr.Join(err, tx.Rollback())
		}()
		client = tx
	}

	var events []storedEvent
	err = client.SelectContext(ctx, &events, fmt.Sprintf(`
		SELECT 
		    event_id,
		    correlation_id,
		    event_type,
		    payload
		FROM outbox_%s_event
		WHERE event_id > ?
		LIMIT %v
	`, h.transportName, h.batchSize), lastTracked)
	if err != nil {
		return nil, err
	}

	return events, nil
}

func (h handler) trackLastHandledEvent(ctx context.Context, client mysql.ClientContext, lastHandledEvent uint64) error {
	_, err := client.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO outbox_%s_tracked_event (transport_name, last_tracked_event_id) VALUES (?, ?)
		ON DUPLICATE KEY UPDATE
			transport_name = VALUES(transport_name),
			last_tracked_event_id = VALUES(last_tracked_event_id)
	`, h.transportName), h.transportName, lastHandledEvent)
	return err
}

func (h handler) lockName() string {
	return fmt.Sprintf("outbox_%s_handler", h.transportName)
}
