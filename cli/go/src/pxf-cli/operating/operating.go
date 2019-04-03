package operating

import (
	"os"

	"github.com/greenplum-db/gp-common-go-libs/operating"
)

func SystemHostname() (string, error) {
	masterHostname := os.Getenv("PXF_MASTER_HOSTNAME")
	if masterHostname != "" {
		return masterHostname, nil
	}
	return operating.System.Hostname()
}
