package pxf

import (
	"errors"
	"fmt"
	"os"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
)

type EnvVar string

const (
	Gphome  EnvVar = "GPHOME"
	PxfConf EnvVar = "PXF_CONF"
)

type MessageType int

const (
	Success MessageType = iota
	Status
	Error
)

type Command interface {
	WhereToRun() int
	Messages(MessageType) string
	GetFunctionToExecute() (func(int) string, error)
	RequiredEnvVars() []EnvVar
}

type SimpleCommand struct {
	commandName     string
	messages        map[MessageType]string
	whereToRun      int
	requiredEnvVars []EnvVar
}

type SyncCommand struct {
	commandName     string
	messages        map[MessageType]string
	whereToRun      int
	requiredEnvVars []EnvVar
	cluster         *cluster.Cluster
}

func (c *SimpleCommand) RequiredEnvVars() []EnvVar {
	return c.requiredEnvVars
}

func (c *SyncCommand) RequiredEnvVars() []EnvVar {
	return c.requiredEnvVars
}

func (c *SimpleCommand) WhereToRun() int {
	return c.whereToRun
}

func (c *SyncCommand) WhereToRun() int {
	return c.whereToRun
}

func (c *SimpleCommand) Messages(messageType MessageType) string {
	return c.messages[messageType]
}

func (c *SyncCommand) Messages(messageType MessageType) string {
	return c.messages[messageType]
}

func (c *SimpleCommand) GetFunctionToExecute() (func(int) string, error) {
	inputs, err := makeValidCliInputs(c)
	if err != nil {
		return nil, err
	}

	pxfCommand := ""
	if inputs[PxfConf] != "" {
		pxfCommand += "PXF_CONF=" + inputs[PxfConf] + " "
	}
	pxfCommand += inputs[Gphome] + "/pxf/bin/pxf" + " " + c.commandName
	return func(_ int) string { return pxfCommand }, nil
}

func SetCluster(c *cluster.Cluster) {
	Sync.cluster = c
}

func (c *SyncCommand) GetFunctionToExecute() (func(int) string, error) {
	if c.cluster == nil {
		return nil, errors.New("Cluster object must be set with SetCluster to use SyncCommand")
	}

	inputs, err := makeValidCliInputs(c)
	if err != nil {
		return nil, err
	}

	return func(contentId int) string {
		return fmt.Sprintf(
			"rsync -az -e 'ssh -o StrictHostKeyChecking=no' '%s/conf' '%s/lib' '%s/servers' '%s:%s'",
			inputs[PxfConf],
			inputs[PxfConf],
			inputs[PxfConf],
			c.cluster.GetHostForContent(contentId),
			inputs[PxfConf])
	}, nil
}

var (
	Init = SimpleCommand{
		commandName: "init",
		messages: map[MessageType]string{
			Success: "PXF initialized successfully on %d out of %d hosts\n",
			Status:  "Initializing PXF on master and %d segment hosts...\n",
			Error:   "PXF failed to initialize on %d out of %d hosts\n",
		},
		requiredEnvVars: []EnvVar{Gphome, PxfConf},
		whereToRun:      cluster.ON_HOSTS_AND_MASTER,
	}
	Start = SimpleCommand{
		commandName: "start",
		messages: map[MessageType]string{
			Success: "PXF started successfully on %d out of %d hosts\n",
			Status:  "Starting PXF on %d segment hosts...\n",
			Error:   "PXF failed to start on %d out of %d hosts\n",
		},
		requiredEnvVars: []EnvVar{Gphome},
		whereToRun:      cluster.ON_HOSTS,
	}
	Stop = SimpleCommand{
		commandName: "stop",
		messages: map[MessageType]string{
			Success: "PXF stopped successfully on %d out of %d hosts\n",
			Status:  "Stopping PXF on %d segment hosts...\n",
			Error:   "PXF failed to stop on %d out of %d hosts\n",
		},
		requiredEnvVars: []EnvVar{Gphome},
		whereToRun:      cluster.ON_HOSTS,
	}
	Sync = SyncCommand{
		commandName: "sync",
		messages: map[MessageType]string{
			Success: "PXF configs synced successfully on %d out of %d hosts\n",
			Status:  "Syncing PXF configuration files to %d hosts...\n",
			Error:   "PXF configs failed to sync on %d out of %d hosts\n",
		},
		requiredEnvVars: []EnvVar{PxfConf},
		whereToRun:      cluster.ON_MASTER_TO_HOSTS,
		cluster:         nil,
	}
)

func makeValidCliInputs(c Command) (map[EnvVar]string, error) {
	envVars := make(map[EnvVar]string)
	for _, e := range c.RequiredEnvVars() {
		val, err := validateEnvVar(e)
		if err != nil {
			return nil, err
		}
		envVars[e] = val
	}
	return envVars, nil
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
