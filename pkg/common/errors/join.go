package errors

import "errors"

func Join(errs ...error) error {
	var errSlice []error
	for _, err := range errs {
		if err != nil {
			errSlice = append(errSlice, err)
		}
	}
	if len(errSlice) == 1 {
		return errSlice[0]
	}
	return errors.Join(errSlice...)
}
