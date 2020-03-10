package cmd_test

import (
	"fmt"
	"pxf-cli/cmd"

	"github.com/greenplum-db/gp-common-go-libs/cluster"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/pkg/errors"
)

var (
	configMaster = cluster.SegConfig{ContentID: -1, Hostname: "mdw", DataDir: "/data/gpseg-1"}
	configSegOne = cluster.SegConfig{ContentID: 0, Hostname: "sdw1", DataDir: "/data/gpseg0"}
	configSegTwo = cluster.SegConfig{ContentID: 1, Hostname: "sdw2", DataDir: "/data/gpseg1"}
	clusterData  = &cmd.ClusterData{
		Cluster:  cluster.NewCluster([]cluster.SegConfig{configMaster, configSegOne, configSegTwo}),
		NumHosts: 0,
		Output:   nil,
	}
)

var _ = Describe("GenerateOutput()", func() {
	BeforeEach(func() {
		clusterData.Output = &cluster.RemoteOutput{
			NumErrors: 0,
			FailedCommands: []*cluster.ShellCommand{
				nil,
				nil,
				nil,
			},
			Commands: []cluster.ShellCommand{
				{
					Host:   "mdw",
					Stdout: "everything fine",
					Stderr: "",
					Error:  nil,
				},
				{
					Host:   "sdw1",
					Stdout: "everything fine",
					Stderr: "",
					Error:  nil,
				},
				{
					Host:   "sdw2",
					Stdout: "everything fine",
					Stderr: "",
					Error:  nil,
				},
			},
		}
	})
	Context("when all hosts are successful", func() {
		It("reports all hosts initialized successfully", func() {
			_ = cmd.GenerateOutput(&cmd.InitCommand, clusterData)
			Expect(testStdout).To(gbytes.Say("PXF initialized successfully on 3 out of 3 hosts"))
		})

		It("reports all hosts started successfully", func() {
			_ = cmd.GenerateOutput(&cmd.StartCommand, clusterData)
			Expect(testStdout).To(gbytes.Say("PXF started successfully on 3 out of 3 hosts"))
		})

		It("reports all hosts stopped successfully", func() {
			_ = cmd.GenerateOutput(&cmd.StopCommand, clusterData)
			Expect(testStdout).To(gbytes.Say("PXF stopped successfully on 3 out of 3 hosts"))
		})

		It("reports all hosts synced successfully", func() {
			_ = cmd.GenerateOutput(&cmd.SyncCommand, clusterData)
			Expect(testStdout).To(gbytes.Say("PXF configs synced successfully on 3 out of 3 hosts"))
		})

		It("reports all hosts running", func() {
			_ = cmd.GenerateOutput(&cmd.StatusCommand, clusterData)
			Expect(testStdout).To(gbytes.Say("PXF is running on 3 out of 3 hosts"))
		})

		It("reports all hosts reset successfully", func() {
			_ = cmd.GenerateOutput(&cmd.ResetCommand, clusterData)
			Expect(testStdout).To(gbytes.Say("PXF has been reset on 3 out of 3 hosts"))
		})
	})

	Context("when some hosts fail", func() {
		BeforeEach(func() {
			failedCommand := cluster.ShellCommand{
				Host:   "sdw2",
				Stdout: "",
				Stderr: "an error happened on sdw2",
				Error:  errors.New("some error"),
			}
			clusterData.Output = &cluster.RemoteOutput{
				NumErrors: 1,
				FailedCommands: []*cluster.ShellCommand{
					&failedCommand,
				},
				Commands: []cluster.ShellCommand{
					{
						Host:   "mdw",
						Stdout: "everything fine",
						Stderr: "",
						Error:  nil,
					},
					{
						Host:   "sdw1",
						Stdout: "everything fine",
						Stderr: "",
						Error:  nil,
					},
					failedCommand,
				},
			}
		})
		It("reports the number of hosts that failed to initialize", func() {
			_ = cmd.GenerateOutput(&cmd.InitCommand, clusterData)
			Expect(testStdout).Should(gbytes.Say("PXF failed to initialize on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
		})

		It("reports the number of hosts that failed to start", func() {
			_ = cmd.GenerateOutput(&cmd.StartCommand, clusterData)
			Expect(testStdout).Should(gbytes.Say("PXF failed to start on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
		})

		It("reports the number of hosts that failed to stop", func() {
			_ = cmd.GenerateOutput(&cmd.StopCommand, clusterData)
			Expect(testStdout).Should(gbytes.Say("PXF failed to stop on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
		})

		It("reports the number of hosts that failed to sync", func() {
			_ = cmd.GenerateOutput(&cmd.SyncCommand, clusterData)
			Expect(testStdout).Should(gbytes.Say("PXF configs failed to sync on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
		})

		It("reports the number of hosts that aren't running", func() {
			_ = cmd.GenerateOutput(&cmd.StatusCommand, clusterData)
			Expect(testStdout).Should(gbytes.Say("PXF is not running on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
		})

		It("reports the number of hosts that failed to reset", func() {
			_ = cmd.GenerateOutput(&cmd.ResetCommand, clusterData)
			Expect(testStdout).Should(gbytes.Say("Failed to reset PXF on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> an error happened on sdw2"))
		})
	})

	Context("when we see messages in Stderr, but NumErrors is 0", func() {
		It("reports all hosts were successful", func() {
			clusterData.Output = &cluster.RemoteOutput{
				NumErrors: 0,
				Commands: []cluster.ShellCommand{
					{
						Host:   "mdw",
						Stdout: "typical stdout",
						Stderr: "typical stderr",
						Error:  nil,
					},
					{
						Host:   "sdw1",
						Stdout: "typical stdout",
						Stderr: "typical stderr",
						Error:  nil,
					},
					{
						Host:   "sdw2",
						Stdout: "typical stdout",
						Stderr: "typical stderr",
						Error:  nil,
					},
				},
			}
			_ = cmd.GenerateOutput(&cmd.StopCommand, clusterData)
			Expect(testStdout).To(gbytes.Say("PXF stopped successfully on 3 out of 3 hosts"))
		})
	})

	Context("when a command fails, and output is multiline", func() {
		It("truncates the output to two lines", func() {
			stderr := `stderr line one
stderr line two
stderr line three`
			failedCommand := cluster.ShellCommand{
				Host:   "sdw2",
				Stdout: "everything not fine",
				Stderr: stderr,
				Error:  errors.New("some error"),
			}
			clusterData.Output = &cluster.RemoteOutput{
				NumErrors: 1,
				FailedCommands: []*cluster.ShellCommand{
					&failedCommand,
				},
				Commands: []cluster.ShellCommand{
					{
						Host:   "mdw",
						Stdout: "everything fine",
						Stderr: "",
						Error:  nil,
					},
					{
						Host:   "sdw1",
						Stdout: "everything fine",
						Stderr: "",
						Error:  nil,
					},
					failedCommand,
				},
			}
			_ = cmd.GenerateOutput(&cmd.StopCommand, clusterData)
			Expect(testStdout).Should(gbytes.Say(fmt.Sprintf("PXF failed to stop on 1 out of 3 hosts")))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> stderr line one\nstderr line two..."))
		})
	})

	Context("when NumErrors is non-zero, but Stderr is empty", func() {
		It("reports Stdout in error message", func() {
			failedCommand := cluster.ShellCommand{
				Host:   "sdw2",
				Stdout: "something wrong on sdw2\nstderr line2\nstderr line3",
				Stderr: "",
				Error:  errors.New("some error"),
			}
			clusterData.Output = &cluster.RemoteOutput{
				NumErrors: 1,
				FailedCommands: []*cluster.ShellCommand{
					&failedCommand,
				},
				Commands: []cluster.ShellCommand{
					{
						Host:   "mdw",
						Stdout: "everything fine",
						Stderr: "",
						Error:  nil,
					},
					{
						Host:   "sdw1",
						Stdout: "everything fine",
						Stderr: "",
						Error:  nil,
					},
					failedCommand,
				},
			}
			_ = cmd.GenerateOutput(&cmd.StopCommand, clusterData)
			Expect(testStdout).Should(gbytes.Say("PXF failed to stop on 1 out of 3 hosts"))
			Expect(testStderr).Should(gbytes.Say("sdw2 ==> something wrong on sdw2\nstderr line2..."))
		})
	})
})
