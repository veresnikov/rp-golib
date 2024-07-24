package errors

import "errors"

func Join(errs ...error) error {
	var errSlice []error
	for _, err := range errs {
		if err != nil {
			errSlice = append(errSlice, err)
		}
	}
	return errors.Join(errSlice...)
}
