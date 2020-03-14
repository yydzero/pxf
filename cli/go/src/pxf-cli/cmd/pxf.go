package cmd

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
)

type envVar string

const (
	gphome   envVar = "GPHOME"
	pxfConf  envVar = "PXF_CONF"
	javaHome envVar = "JAVA_HOME"
)

type messageType int

const (
	success messageType = iota
	status
	err
	warning
	standby
)

type command struct {
	name       commandName
	messages   map[messageType]string
	whereToRun cluster.Scope
	envVars    []envVar
	warn       bool // whether the command requires a warning/prompt
}

func (cmd *command) Warn(input io.Reader) error {
	if cmd.warn == false || promptUser(input, cmd.messages[warning]) {
		return nil
	}
	return fmt.Errorf("pxf %s cancelled", cmd.name)
}

func (cmd *command) GetFunctionToExecute() (func(string) string, error) {
	inputs, err := makeValidCliInputs(cmd)
	if err != nil {
		return nil, err
	}

	switch cmd.name {
	case sync:
		rsyncCommand := "rsync -az%s -e 'ssh -o StrictHostKeyChecking=no' '%s/conf' '%s/lib' '%s/servers' '%s:%s'"
		deleteString := ""
		if DeleteOnSync {
			deleteString = " --delete"
		}
		return func(hostname string) string {
			return fmt.Sprintf(
				rsyncCommand,
				deleteString,
				inputs[pxfConf],
				inputs[pxfConf],
				inputs[pxfConf],
				hostname,
				inputs[pxfConf])
		}, nil
	default:
		pxfCommand := ""
		if inputs[pxfConf] != "" {
			pxfCommand += "PXF_CONF=" + inputs[pxfConf] + " "
		}
		if inputs[javaHome] != "" {
			pxfCommand += "JAVA_HOME=" + inputs[javaHome] + " "
		}
		pxfCommand += inputs[gphome] + "/pxf/bin/pxf" + " " + string(cmd.name)
		if cmd.name == reset {
			pxfCommand += " --force" // there is a prompt for local reset as well
		}
		return func(_ string) string { return pxfCommand }, nil
	}
}

func promptUser(input io.Reader, prompt string) bool {
	reader := bufio.NewReader(input)
	fmt.Printf(prompt)
	text, _ := reader.ReadString('\n')
	text = strings.TrimRight(text, "\r\n")
	return strings.ToLower(text) == "y"
}

type commandName string

const (
	pxfInit  = "init"
	start    = "start"
	stop     = "stop"
	sync     = "sync"
	statuses = "status"
	reset    = "reset"
	restart  = "restart"
)

// The pxf cli commands, exported for testing
var (
	InitCommand = command{
		name: pxfInit,
		messages: map[messageType]string{
			success: "PXF initialized successfully on %d out of %d hosts\n",
			status:  "Initializing PXF on master host%s and %d segment hosts...\n",
			standby: ", standby master host,",
			err:     "PXF failed to initialize on %d out of %d hosts\n",
		},
		warn:       false,
		envVars:    []envVar{gphome, pxfConf, javaHome},
		whereToRun: cluster.ON_REMOTE | cluster.ON_HOSTS | cluster.INCLUDE_MASTER | cluster.INCLUDE_MIRRORS,
	}
	StartCommand = command{
		name: start,
		messages: map[messageType]string{
			success: "PXF started successfully on %d out of %d hosts\n",
			status:  "Starting PXF on %d segment hosts...\n",
			err:     "PXF failed to start on %d out of %d hosts\n",
		},
		warn:       false,
		envVars:    []envVar{gphome},
		whereToRun: cluster.ON_REMOTE | cluster.ON_HOSTS | cluster.EXCLUDE_MASTER | cluster.EXCLUDE_MIRRORS,
	}
	StopCommand = command{
		name: stop,
		messages: map[messageType]string{
			success: "PXF stopped successfully on %d out of %d hosts\n",
			status:  "Stopping PXF on %d segment hosts...\n",
			err:     "PXF failed to stop on %d out of %d hosts\n",
		},
		warn:       false,
		envVars:    []envVar{gphome},
		whereToRun: cluster.ON_REMOTE | cluster.ON_HOSTS | cluster.EXCLUDE_MASTER | cluster.EXCLUDE_MIRRORS,
	}
	SyncCommand = command{
		name: sync,
		messages: map[messageType]string{
			success: "PXF configs synced successfully on %d out of %d hosts\n",
			status:  "Syncing PXF configuration files from master host to%s %d segment hosts...\n",
			standby: " standby master host and",
			err:     "PXF configs failed to sync on %d out of %d hosts\n",
		},
		warn:    false,
		envVars: []envVar{pxfConf},
		// cluster.ON_LOCAL | cluster.ON_HOSTS: the command will target hosts, but be run from master
		// this is ideal for rsync from master to segment hosts. also exclude master but include standby master
		whereToRun: cluster.ON_LOCAL | cluster.ON_HOSTS | cluster.EXCLUDE_MASTER | cluster.INCLUDE_MIRRORS,
	}
	StatusCommand = command{
		name: statuses,
		messages: map[messageType]string{
			success: "PXF is running on %d out of %d hosts\n",
			status:  "Checking status of PXF servers on %d segment hosts...\n",
			err:     "PXF is not running on %d out of %d hosts\n",
		},
		warn:       false,
		envVars:    []envVar{gphome},
		whereToRun: cluster.ON_REMOTE | cluster.ON_HOSTS | cluster.EXCLUDE_MASTER | cluster.EXCLUDE_MIRRORS,
	}
	ResetCommand = command{
		name: reset,
		messages: map[messageType]string{
			success: "PXF has been reset on %d out of %d hosts\n",
			status:  "Resetting PXF on master host%s and %d segment hosts...\n",
			standby: ", standby master host,",
			err:     "Failed to reset PXF on %d out of %d hosts\n",
			warning: "Ensure your PXF cluster is stopped before continuing. " +
				"This is a destructive action. Press y to continue:\n",
		},
		warn:       true,
		envVars:    []envVar{gphome},
		whereToRun: cluster.ON_REMOTE | cluster.ON_HOSTS | cluster.INCLUDE_MASTER | cluster.INCLUDE_MIRRORS,
	}
	RestartCommand = command{
		name: restart,
		messages: map[messageType]string{
			success: "PXF has restarted successfully on %d out of %d hosts\n",
			status:  "Restarting PXF on %d segment hosts...\n",
			err:     "Failed to restart PXF on %d out of %d hosts\n",
		},
		warn:       false,
		envVars:    []envVar{gphome},
		whereToRun: cluster.ON_REMOTE | cluster.ON_HOSTS | cluster.EXCLUDE_MASTER | cluster.EXCLUDE_MIRRORS,
	}
)

func makeValidCliInputs(cmd *command) (map[envVar]string, error) {
	envVars := make(map[envVar]string)
	for _, e := range cmd.envVars {
		val, err := validateEnvVar(e)
		if err != nil {
			return nil, err
		}
		envVars[e] = val
	}
	return envVars, nil
}

func validateEnvVar(envVariable envVar) (string, error) {
	envVarValue, isEnvVarSet := os.LookupEnv(string(envVariable))
	if !isEnvVarSet {
		return "", errors.New(string(envVariable) + " must be set")
	}
	if envVarValue == "" {
		return "", errors.New(string(envVariable) + " cannot be blank")
	}
	return envVarValue, nil
}
