package pxf

import (
	"errors"
	"fmt"
	"os"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
)

type CliInputs struct {
	Gphome  string
	PxfConf string
	Cmd     Command
}

type EnvVar string

const (
	Gphome  EnvVar = "GPHOME"
	PxfConf EnvVar = "PXF_CONF"
)

type Command string

const (
	Init  Command = "init"
	Start Command = "start"
	Stop  Command = "stop"
	Sync  Command = "sync"
)

var (
	SuccessMessage = map[Command]string{
		Init:  "PXF initialized successfully on %d out of %d hosts\n",
		Start: "PXF started successfully on %d out of %d hosts\n",
		Stop:  "PXF stopped successfully on %d out of %d hosts\n",
		Sync:  "PXF configs synced successfully on %d out of %d hosts\n",
	}

	ErrorMessage = map[Command]string{
		Init:  "PXF failed to initialize on %d out of %d hosts\n",
		Start: "PXF failed to start on %d out of %d hosts\n",
		Stop:  "PXF failed to stop on %d out of %d hosts\n",
		Sync:  "PXF configs failed to sync on %d out of %d hosts\n",
	}

	StatusMessage = map[Command]string{
		Init:  "Initializing PXF on master and %d segment hosts...\n",
		Start: "Starting PXF on %d segment hosts...\n",
		Stop:  "Stopping PXF on %d segment hosts...\n",
		Sync:  "Syncing PXF configuration files to %d hosts...\n",
	}
)

func makeValidCliInputs(cmd Command) (*CliInputs, error) {
	gphome, err := validateEnvVar(Gphome)
	if err != nil {
		return nil, err
	}
	pxfConf := ""
	if cmd == Init || cmd == Sync {
		pxfConf, err = validateEnvVar(PxfConf)
		if err != nil {
			return nil, err
		}
	}
	return &CliInputs{Cmd: cmd, Gphome: gphome, PxfConf: pxfConf}, nil
}

func validateEnvVar(envVar EnvVar) (string, error) {
	envVarValue, isEnvVarSet := os.LookupEnv(string(envVar))
	if !isEnvVarSet {
		return "", errors.New(string(envVar) + " must be set")
	}
	if envVarValue == "" {
		return "", errors.New(string(envVar) + " cannot be blank")
	}
	return envVarValue, nil
}

func CommandFunc(command Command, c *cluster.Cluster) (func(int) string, error) {
	inputs, err := makeValidCliInputs(command)
	if err != nil {
		return nil, err
	}
	switch command {
	case Start, Stop, Init:
		pxfCommand := ""
		if inputs.PxfConf != "" {
			pxfCommand += "PXF_CONF=" + inputs.PxfConf + " "
		}
		pxfCommand += inputs.Gphome + "/pxf/bin/pxf" + " " + string(inputs.Cmd)
		return func(_ int) string { return pxfCommand }, nil
	case Sync:
		return func(contentId int) string {
			return fmt.Sprintf(
				"rsync -az -e 'ssh -o StrictHostKeyChecking=no' '%s/conf' '%s/lib' '%s/servers' '%s:%s'",
				inputs.PxfConf,
				inputs.PxfConf,
				inputs.PxfConf,
				c.GetHostForContent(contentId),
				inputs.PxfConf)
		}, nil
	default:
		return nil, fmt.Errorf("%s is not a valid pxf command", command)
	}
}
