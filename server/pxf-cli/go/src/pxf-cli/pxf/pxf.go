package pxf

import (
	"errors"
	"fmt"
	"os"
)

type CliInputs struct {
	Gphome  string
	Command string
}

func MakeValidClusterCommandInputs(subcmd string) (*CliInputs, error) {
	gphome, isGphomeSet := os.LookupEnv("GPHOME")
	if !isGphomeSet {
		return nil, errors.New("GPHOME is not set")
	}
	if gphome == "" {
		return nil, errors.New("GPHOME is blank")
	}
	switch subcmd {
	case "init", "start", "stop", "restart", "status":
		return &CliInputs{
			Gphome:  os.Getenv("GPHOME"),
			Command: subcmd,
		}, nil
	}
	// the 'flags' library validates the subcommand, so if we get here, it's programmer error.
	panic(fmt.Sprintf("invalid command passed to MakeValidClusterCommandInputs: %s", subcmd))
}

func RemoteCommandToRunOnSegments(inputs *CliInputs) []string {
	return []string{inputs.Gphome + "/pxf/bin/pxf", inputs.Command}
}
