package cmds

import "fmt"

type Help struct {
}

func (c *Help) Execute(args []string) error {
	fmt.Println(`Usage: pxf {start|stop|init|restart|status|help|version}
     | pxf cluster {start|stop|init}`)
	return nil
}
