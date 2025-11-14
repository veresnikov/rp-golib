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
	"gitea.xscloud.ru/xscloud/golib/pkg/internal/helpers"
)

type Transport interface {
	HandleEvents(ctx context.Context, correlationID, eventType, payload string) error
}

type Handler interface {
	Start(ctx context.Context) error
}

type EventHandlerConfig struct {
	TransportName  string
	Transport      Transport
	ConnectionPool mysql.ConnectionPool
	Logger         logging.Logger
	BatchSize      *uint
	SendInterval   *time.Duration
	LockTimeout    *time.Duration
}

func NewEventHandler(config EventHandlerConfig) Handler {
	if config.TransportName == "" {
		panic("transport name cannot be empty")
	}
	if config.BatchSize == nil {
		config.BatchSize = helpers.ToPtr(uint(1000))
	}
	if config.SendInterval == nil {
		config.SendInterval = helpers.ToPtr(10 * time.Second)
	}
	if config.LockTimeout == nil {
		config.LockTimeout = helpers.ToPtr(time.Minute)
	}

	return &handler{
		transportName: config.TransportName,
		transport:     config.Transport,
		pool:          config.ConnectionPool,
		logger:        config.Logger,
		batchSize:     *config.BatchSize,
		sendInterval:  *config.SendInterval,
		lockTimeout:   *config.LockTimeout,
		locker:        mysql.NewLocker(config.ConnectionPool),
	}
}

type handler struct {
	transportName string
	transport     Transport
	batchSize     uint

	pool   mysql.ConnectionPool
	locker mysql.Locker
	logger logging.Logger

	sendInterval time.Duration
	lockTimeout  time.Duration
}

func (h handler) Start(ctx context.Context) error {
	needRetry := make(chan bool, 1)
	defer close(needRetry)

	select {
	case needRetry <- true:
	default:
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(h.sendInterval):
		case <-needRetry:
		}

		sendCtx, cancel := context.WithCancel(context.Background())
		err := h.sendEvents(sendCtx, needRetry)
		cancel()
		if err != nil {
			return err
		}
	}
}

func (h handler) sendEvents(ctx context.Context, needRetry chan bool) error {
	return h.locker.ExecuteWithLock(ctx, h.lockName(), h.lockTimeout, func() (err error) {
		conn, err := h.pool.TransactionalConnection(ctx)
		if err != nil {
			return err
		}
		defer func() {
			err = liberr.Join(err, conn.Close())
		}()

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

		select {
		case needRetry <- len(uncommitedEvents) > 0 || uint(len(commitedEvents)) == h.batchSize:
		default:
		}

		var handleErr error
		for i := 0; i < len(commitedEvents); i++ {
			if uncommitedEvents[i].EventID != commitedEvents[i].EventID {
				break
			}

			handleErr = h.transport.HandleEvents(
				ctx,
				commitedEvents[i].CorrelationID,
				commitedEvents[i].EventType,
				commitedEvents[i].Payload,
			)
			if handleErr != nil {
				h.logger.Error(handleErr)
				break
			}

			err = h.trackLastHandledEvent(ctx, conn, commitedEvents[i].EventID)
			if err != nil {
				return err
			}
		}

		return nil
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
