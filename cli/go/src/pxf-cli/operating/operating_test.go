package operating_test

import (
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "pxf-cli/operating"

	"github.com/greenplum-db/gp-common-go-libs/operating"
)

var _ = Describe("Operating", func() {
	BeforeEach(func() {
		operating.System.Hostname = func() (string, error) {
			return "system_hostname", nil
		}
	})

	Context("SystemHostname", func() {
		Context("When PXF_MASTER_HOSTNAME env var is unset", func() {
			It("Reports the system hostname", func() {
				Expect(SystemHostname()).To(Equal("system_hostname"))
			})
		})

		Context("When PXF_MASTER_HOSTNAME env var is set to empty string", func() {
			BeforeEach(func() {
				_ = os.Setenv("PXF_MASTER_HOSTNAME", "")
			})
			It("Reports the system hostname", func() {
				Expect(SystemHostname()).To(Equal("system_hostname"))
			})
		})

		Context("When PXF_MASTER_HOSTNAME env var is set to something", func() {
			BeforeEach(func() {
				_ = os.Setenv("PXF_MASTER_HOSTNAME", "my-real-hostname")
			})
			It("Reports the hostname from PXF_MASTER_HOSTNAME", func() {
				Expect(SystemHostname()).To(Equal("my-real-hostname"))
			})
		})
	})
})
