package outbox

import (
	"crypto/sha256"
	"encoding/base64"
	"strings"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

func newCorrelationID(appID, payload string) (string, error) {
	uid, err := uuid.NewV7()
	if err != nil {
		return "", errors.WithStack(err)
	}

	payloadHash := sha256.New()
	_, err = payloadHash.Write([]byte(payload))
	if err != nil {
		return "", errors.WithStack(err)
	}

	const separator = ":"
	return strings.Join(
		[]string{
			appID,
			base64.URLEncoding.EncodeToString(payloadHash.Sum(nil)),
			uid.String(),
		},
		separator,
	), nil
}
