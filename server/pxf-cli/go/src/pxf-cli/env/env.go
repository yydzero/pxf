package env

import (
	"errors"
	"os"
)

// Require returns the value of the requested environment variable, or an error if it is unset or blank.
func Require(varname string) (string, error) {
	value, isSet := os.LookupEnv(varname)

	switch {
	case !isSet:
		return "", errors.New(varname + " is not set")
	case value == "":
		return "", errors.New(varname + " is blank")
	default:
		return value, nil
	}
}
