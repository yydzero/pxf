package main

import (
	"github.com/jessevdk/go-flags"
	"os"
	"pxf-cli/cmds"
)

type commands struct {
	Help    cmds.Help    `command:"help" description:"display the help text"`
	Version cmds.Version `command:"version" description:"display the PXF version"`
	Cluster cmds.Cluster `command:"cluster" description:"perform actions on all segments"`
}

func main() {
	var c commands
	_, err := flags.Parse(&c) // invokes the command corresponding to the args
	if err != nil {
		os.Exit(1)
	}
}
