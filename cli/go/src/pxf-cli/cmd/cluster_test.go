package cmd_test

import (
	"fmt"
	"pxf-cli/cmd"
	"pxf-cli/pxf"

	"github.com/greenplum-db/gp-common-go-libs/operating"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/pkg/errors"
)

var (
	clusterOutput *cluster.RemoteOutput
	configMaster  = cluster.SegConfig{ContentID: -1, Hostname: "mdw", DataDir: "/data/gpseg-1"}
	configSegOne  = cluster.SegConfig{ContentID: 0, Hostname: "sdw1", DataDir: "/data/gpseg0"}
	configSegTwo  = cluster.SegConfig{ContentID: 1, Hostname: "sdw2", DataDir: "/data/gpseg1"}
	globalCluster = cluster.NewCluster([]cluster.SegConfig{configMaster, configSegOne, configSegTwo})
)

var _ = Describe("GenerateHostList", func() {
	It("Returns the correct hostlist and master hostname", func() {
		operating.System.Hostname = func() (string, error) {
			return "mdw", nil
		}
		hostlist, err := cmd.GenerateHostList()
		Expect(err).To(BeNil())
		Expect(hostlist).To(Equal(map[string]int{"sdw1": 1, "sdw2": 1}))
	})
	It("Errors if not running from master host", func() {
		operating.System.Hostname = func() (string, error) {
			return "fake-host", nil
		}
		hostlist, err := cmd.GenerateHostList()
		Expect(err.Error()).To(Equal("ERROR: pxf cluster commands should only be run from Greenplum master"))
		Expect(hostlist).To(BeNil())
	})
})

var _ = Describe("GenerateOutput", func() {
	cmd.SetCluster(globalCluster)
	clusterOutput = &cluster.RemoteOutput{
		NumErrors: 0,
		Stderrs:   map[int]string{-1: "", 0: "", 1: ""},
		Stdouts:   map[int]string{-1: "everything fine", 0: "everything fine", 1: "everything fine"},
		Errors:    map[int]error{-1: nil, 0: nil, 1: nil},
	}
	Describe("Running supported commands", func() {
		Context("When all hosts are successful", func() {
			It("Reports all hosts initialized successfully", func() {
				cmd.SetCommand(&pxf.Init)
				_ = cmd.GenerateOutput(clusterOutput)
				Expect(testStdout).To(gbytes.Say(fmt.Sprintf(pxf.Init.Messages("success"), 3, 3)))
			})

			It("Reports all hosts started successfully", func() {
				cmd.SetCommand(&pxf.Start)
				_ = cmd.GenerateOutput(clusterOutput)
				Expect(testStdout).To(gbytes.Say(fmt.Sprintf(pxf.Start.Messages("success"), 3, 3)))
			})

			It("Reports all hosts stopped successfully", func() {
				cmd.SetCommand(&pxf.Stop)
				_ = cmd.GenerateOutput(clusterOutput)
				Expect(testStdout).To(gbytes.Say(fmt.Sprintf(pxf.Stop.Messages("success"), 3, 3)))
			})

			It("Reports all hosts synced successfully", func() {
				cmd.SetCommand(&pxf.Sync)
				_ = cmd.GenerateOutput(clusterOutput)
				Expect(testStdout).To(gbytes.Say(fmt.Sprintf(pxf.Sync.Messages("success"), 3, 3)))
			})
		})

		Context("When some hosts fail", func() {
			var expectedError string
			BeforeEach(func() {
				clusterOutput = &cluster.RemoteOutput{
					NumErrors: 1,
					Stderrs:   map[int]string{-1: "", 0: "", 1: "an error happened on sdw2"},
					Stdouts:   map[int]string{-1: "everything fine", 0: "everything fine", 1: "something wrong"},
					Errors:    map[int]error{-1: nil, 0: nil, 1: errors.New("some error")},
				}
				expectedError = "sdw2 ==> an error happened on sdw2"
			})
			It("Reports the number of hosts that failed to initialize", func() {
				cmd.SetCommand(&pxf.Init)
				_ = cmd.GenerateOutput(clusterOutput)
				Expect(testStdout).Should(gbytes.Say(fmt.Sprintf(pxf.Init.Messages("error"), 1, 3)))
				Expect(testStderr).Should(gbytes.Say(expectedError))
			})

			It("Reports the number of hosts that failed to start", func() {
				cmd.SetCommand(&pxf.Start)
				_ = cmd.GenerateOutput(clusterOutput)
				Expect(testStdout).Should(gbytes.Say(fmt.Sprintf(pxf.Start.Messages("error"), 1, 3)))
				Expect(testStderr).Should(gbytes.Say(expectedError))
			})

			It("Reports the number of hosts that failed to stop", func() {
				cmd.SetCommand(&pxf.Stop)
				_ = cmd.GenerateOutput(clusterOutput)
				Expect(testStdout).Should(gbytes.Say(fmt.Sprintf(pxf.Stop.Messages("error"), 1, 3)))
				Expect(testStderr).Should(gbytes.Say(expectedError))
			})

			It("Reports the number of hosts that failed to sync", func() {
				cmd.SetCommand(&pxf.Sync)
				_ = cmd.GenerateOutput(clusterOutput)
				Expect(testStdout).Should(gbytes.Say(fmt.Sprintf(pxf.Sync.Messages("error"), 1, 3)))
				Expect(testStderr).Should(gbytes.Say(expectedError))
			})
		})

		Context("Before the command returns", func() {
			cmd.SetCluster(globalCluster)
			hostList := map[string]int{"sdw1": 1, "sdw2": 2}
			cmd.SetHostList(hostList)
			It("Reports the number of hosts that will be initialized", func() {
				cmd.SetCommand(&pxf.Init)
				_ = cmd.GenerateStatusReport()
				Expect(testStdout).Should(gbytes.Say(fmt.Sprintf(pxf.Init.Messages("status"), 2)))
			})

			It("Reports the number of hosts will be started", func() {
				cmd.SetCommand(&pxf.Start)
				_ = cmd.GenerateStatusReport()
				Expect(testStdout).Should(gbytes.Say(fmt.Sprintf(pxf.Start.Messages("status"), 2)))
			})

			It("Reports the number of hosts that will be stopped", func() {
				cmd.SetCommand(&pxf.Stop)
				_ = cmd.GenerateStatusReport()
				Expect(testStdout).Should(gbytes.Say(fmt.Sprintf(pxf.Stop.Messages("status"), 2)))
			})

			It("Reports the number of hosts that will be synced", func() {
				cmd.SetCommand(&pxf.Sync)
				_ = cmd.GenerateStatusReport()
				Expect(testStdout).Should(gbytes.Say(fmt.Sprintf(pxf.Sync.Messages("status"), 2)))
			})
		})

		Context("When we see messages in Stderr, but NumErrors is 0", func() {
			It("Reports all hosts were successful", func() {
				clusterOutput = &cluster.RemoteOutput{
					NumErrors: 0,
					Stderrs:   map[int]string{-1: "typical stderr", 0: "typical stderr", 1: "typical stderr"},
					Stdouts:   map[int]string{-1: "typical stdout", 0: "typical stdout", 1: "typical stdout"},
					Errors:    map[int]error{-1: nil, 0: nil, 1: nil},
				}
				cmd.SetCommand(&pxf.Stop)
				_ = cmd.GenerateOutput(clusterOutput)
				Expect(testStdout).To(gbytes.Say("PXF stopped successfully on 3 out of 3 hosts"))
			})
		})

		Context("When a command fails, and output is multiline", func() {
			It("Truncates the output to two lines", func() {
				stderr := `stderr line one
stderr line two
stderr line three`
				clusterOutput = &cluster.RemoteOutput{
					NumErrors: 1,
					Stderrs:   map[int]string{-1: "", 0: "", 1: stderr},
					Stdouts:   map[int]string{-1: "everything fine", 0: "everything fine", 1: "everything not fine"},
					Errors:    map[int]error{-1: nil, 0: nil, 1: errors.New("some error")},
				}
				expectedError := `sdw2 ==> stderr line one
stderr line two...`
				cmd.SetCommand(&pxf.Stop)
				_ = cmd.GenerateOutput(clusterOutput)
				Expect(testStdout).Should(gbytes.Say(fmt.Sprintf(pxf.Stop.Messages("error"), 1, 3)))
				Expect(testStderr).Should(gbytes.Say(expectedError))
			})
		})

		Context("When NumErrors is non-zero, but Stderr is empty", func() {
			It("Reports Stdout in error message", func() {
				clusterOutput = &cluster.RemoteOutput{
					NumErrors: 1,
					Stderrs:   map[int]string{-1: "", 0: "", 1: ""},
					Stdouts:   map[int]string{-1: "everything fine", 0: "everything fine", 1: "something wrong on sdw2\nstderr line2\nstderr line3"},
					Errors:    map[int]error{-1: nil, 0: nil, 1: errors.New("some error")},
				}
				expectedError := "sdw2 ==> something wrong on sdw2\nstderr line2..."
				cmd.SetCommand(&pxf.Stop)
				_ = cmd.GenerateOutput(clusterOutput)
				Expect(testStdout).Should(gbytes.Say(fmt.Sprintf(pxf.Stop.Messages("error"), 1, 3)))
				Expect(testStderr).Should(gbytes.Say(expectedError))
			})
		})
	})
})
