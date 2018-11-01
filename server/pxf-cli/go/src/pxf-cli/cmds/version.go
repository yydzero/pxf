package cmds

import "fmt"

type Version struct {
}

func (c *Version) Execute(args []string) error {
	fmt.Println("version")
	return nil
}