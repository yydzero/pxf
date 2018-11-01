package cmds

import "fmt"

type Help struct {
}

func (c *Help) Execute(args []string) error {
	fmt.Println(`Usage: pxf [cluster] {start|stop|init|restart|status}
     | pxf {help|version}`)
	return nil
}
