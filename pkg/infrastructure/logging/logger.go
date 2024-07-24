package logging

import (
	"os"
	"time"

	"golib/pkg/application/logging"

	"github.com/sirupsen/logrus"
)

const appNameKey = "app_name"

type Config struct {
	AppName string
}

func NewJSONLogger(config *Config) logging.MainLogger {
	impl := logrus.New()
	impl.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339Nano,
		FieldMap:        fieldMap,
	})
	impl.SetOutput(os.Stderr)
	impl.AddHook(NewStackTraceHook())
	return &loggerImpl{
		FieldLogger: impl.WithField(appNameKey, config.AppName),
	}
}

type loggerImpl struct {
	logrus.FieldLogger
}

func (l *loggerImpl) WithField(key string, value interface{}) logging.Logger {
	return &loggerImpl{l.FieldLogger.WithField(key, value)}
}

func (l *loggerImpl) WithFields(fields logging.Fields) logging.Logger {
	return &loggerImpl{l.FieldLogger.WithFields(logrus.Fields(fields))}
}

func (l *loggerImpl) Error(err error, args ...interface{}) {
	l.FieldLogger.WithError(err).Error(args...)
}

func (l *loggerImpl) Warning(err error, args ...interface{}) {
	l.FieldLogger.WithError(err).Warn(args...)
}

func (l *loggerImpl) FatalError(err error, args ...interface{}) {
	l.FieldLogger.WithError(err).Fatal(args...)
}

var fieldMap = logrus.FieldMap{
	logrus.FieldKeyTime: "@timestamp",
	logrus.FieldKeyMsg:  "message",
}
