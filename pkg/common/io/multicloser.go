package io

import (
	"io"

	"gitea.xscloud.ru/xscloud/golib/pkg/common/errors"
)

type MultiCloser interface {
	io.Closer
	AddCloser(closer io.Closer)
}

func NewMultiCloser() MultiCloser {
	return &multiCloser{}
}

type multiCloser struct {
	closers []io.Closer
}

func (m *multiCloser) Close() error {
	var err error
	for _, closer := range m.closers {
		err = errors.Join(err, closer.Close())
	}
	return err
}

func (m *multiCloser) AddCloser(closer io.Closer) {
	m.closers = append(m.closers, closer)
}
