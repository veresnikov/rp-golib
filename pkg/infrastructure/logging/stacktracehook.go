package logging

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const stackKey = "stack"

func NewStackTraceHook() logrus.Hook {
	return &stackTraceHook{}
}

type stackTraceHook struct{}

func (hook stackTraceHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (hook stackTraceHook) Fire(entry *logrus.Entry) error {
	val, ok := entry.Data[logrus.ErrorKey]
	if !ok {
		return nil
	}

	err, ok := val.(error)
	if !ok {
		return nil
	}

	if err == nil {
		delete(entry.Data, logrus.ErrorKey)
		return nil
	}

	if t, ok := err.(stackTracer); ok {
		entry.Data[stackKey] = strings.ReplaceAll(fmt.Sprintf("%+v", t.StackTrace()), " ", "\n")
	}
	entry.Data[logrus.ErrorKey] = err.Error()

	return nil
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}
