package pxf

import (
	"fmt"
	"pxf-cli/env"
)

type ClusterCommandInputs struct {
	Gphome  string
	Command string
}

func MakeValidClusterCommandInputs(subcmd string) (*ClusterCommandInputs, error) {
	gphome, err := env.Require("GPHOME")
	if err != nil {
		return nil, err
	}

	switch subcmd {
	case "init", "start", "stop", "restart", "status":
		return &ClusterCommandInputs{
			Gphome:  gphome,
			Command: subcmd,
		}, nil
	}
	// the 'flags' library validates the subcommand, so if we get here, it's programmer error.
	panic(fmt.Sprintf("invalid command passed to MakeValidClusterCommandInputs: %s", subcmd))
}

func RemoteCommandToRunOnSegments(inputs *ClusterCommandInputs) []string {
	return []string{inputs.Gphome + "/pxf/bin/pxf", inputs.Command}
}
