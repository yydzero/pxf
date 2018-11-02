package cmds

import (
	"fmt"
	"io/ioutil"
	"pxf-cli/env"
)

type Version struct {
}

func (c *Version) Execute(args []string) error {
	gphome, err := env.Require("GPHOME")
	if err != nil {
		return err
	}

	version, err := ioutil.ReadFile(gphome + "/pxf/version")
	if err != nil {
		return err
	}

	fmt.Println(string(version))
	return nil
}