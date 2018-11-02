package cmds

import (
	"fmt"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"pxf-cli/gpssh"
	"pxf-cli/greenplum"
	"pxf-cli/pxf"
)

type start struct {
}

type stop struct {
}

type initialize struct {
}

type Cluster struct {
	Start start      `command:"start" description:"start the cluster"`
	Stop  stop       `command:"stop" description:"stop the cluster"`
	Init  initialize `command:"init" description:"initialize the cluster"`
}

func (c *Cluster) Execute(args []string) error {
	return nil
}

func (c *start) Execute(args []string) error {
	return doOnAllSegments("start")
}

func (c *stop) Execute(args []string) error {
	return doOnAllSegments("stop")
}

func (c *initialize) Execute(args []string) error {
	return doOnAllSegments("init")
}

func doOnAllSegments(subcmd string) error {
	gplog.InitializeLogging("pxf_cli", "")

	inputs, err := pxf.MakeValidClusterCommandInputs(subcmd)
	if err != nil {
		return err
	}

	segments, err := greenplum.GetSegmentHosts()
	if err != nil {
		return err
	}

	remoteCommand := pxf.RemoteCommandToRunOnSegments(inputs)
	out, err := gpssh.Command(segments, remoteCommand).CombinedOutput()
	fmt.Println(string(out))
	return err
}