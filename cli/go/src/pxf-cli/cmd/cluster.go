package cmd

import (
	"errors"
	"fmt"
	"os"
	"pxf-cli/pxf"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/operating"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/spf13/cobra"
)

type ClusterData struct {
	Cluster        *cluster.Cluster
	Output         *cluster.RemoteOutput
	NumHosts       int
	connectionPool *dbconn.DBConn
}

var (
	clusterCmd = &cobra.Command{
		Use:   "cluster",
		Short: "Perform <command> on each segment host in the cluster",
	}

	initCmd = &cobra.Command{
		Use:   "init",
		Short: "Initialize the local PXF server instance",
		Run: func(cmd *cobra.Command, args []string) {
			clusterData, err := doSetup()
			if err == nil {
				err = clusterRun(&pxf.Init, clusterData)
			}
			exitWithReturnCode(err)
		},
	}

	startCmd = &cobra.Command{
		Use:   "start",
		Short: "Start the local PXF server instance",
		Run: func(cmd *cobra.Command, args []string) {
			clusterData, err := doSetup()
			if err == nil {
				err = clusterRun(&pxf.Start, clusterData)
			}
			exitWithReturnCode(err)
		},
	}

	stopCmd = &cobra.Command{
		Use:   "stop",
		Short: "Stop the local PXF server instance",
		Run: func(cmd *cobra.Command, args []string) {
			clusterData, err := doSetup()
			if err == nil {
				err = clusterRun(&pxf.Stop, clusterData)
			}
			exitWithReturnCode(err)
		},
	}

	syncCmd = &cobra.Command{
		Use:   "sync",
		Short: "Sync PXF configs from master to all segment hosts",
		Run: func(cmd *cobra.Command, args []string) {
			clusterData, err := doSetup()
			if err == nil {
				err = clusterRun(&pxf.Sync, clusterData)
			}
			exitWithReturnCode(err)
		},
	}
)

func init() {
	rootCmd.AddCommand(clusterCmd)
	clusterCmd.AddCommand(initCmd)
	clusterCmd.AddCommand(startCmd)
	clusterCmd.AddCommand(stopCmd)
	clusterCmd.AddCommand(syncCmd)
}

func exitWithReturnCode(err error) {
	if err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

func (r *ClusterData) CountHostsAndValidateMaster() error {
	hostSegMap := make(map[string]int, 0)
	master, err := operating.System.Hostname()
	if err != nil {
		return err
	}
	for contentID, seg := range r.Cluster.Segments {
		if contentID == -1 {
			if seg.Hostname != master {
				r.NumHosts = -1
				return errors.New("ERROR: pxf cluster commands should only be run from Greenplum master")
			}
			continue
		}
		hostSegMap[seg.Hostname]++
	}
	r.NumHosts = len(hostSegMap)
	return nil
}

func GenerateStatusReport(command *pxf.Command, clusterData *ClusterData) string {
	cmdMsg := fmt.Sprintf(command.Messages(pxf.Status), clusterData.NumHosts)
	gplog.Info(cmdMsg)
	return cmdMsg
}

func GenerateOutput(command *pxf.Command, clusterData *ClusterData) error {
	numHosts := len(clusterData.Output.Stdouts)
	numErrors := clusterData.Output.NumErrors
	if numErrors == 0 {
		gplog.Info(command.Messages(pxf.Success), numHosts-numErrors, numHosts)
		return nil
	}
	response := ""
	for index, stderr := range clusterData.Output.Stderrs {
		if clusterData.Output.Errors[index] == nil {
			continue
		}
		host := clusterData.Cluster.Segments[index].Hostname
		errorMessage := stderr
		if len(errorMessage) == 0 {
			errorMessage = clusterData.Output.Stdouts[index]
		}
		lines := strings.Split(errorMessage, "\n")
		errorMessage = lines[0]
		if len(lines) > 1 {
			errorMessage += "\n" + lines[1]
		}
		if len(lines) > 2 {
			errorMessage += "..."
		}
		response += fmt.Sprintf("%s ==> %s\n", host, errorMessage)
	}
	gplog.Info("ERROR: "+command.Messages(pxf.Error), numErrors, numHosts)
	gplog.Error("%s", response)
	return errors.New(response)
}

func doSetup() (*ClusterData, error) {
	connectionPool := dbconn.NewDBConnFromEnvironment("postgres")
	err := connectionPool.Connect(1)
	if err != nil {
		gplog.Error(fmt.Sprintf("ERROR: Could not connect to GPDB.\n%s\n"+
			"Please make sure that your Greenplum database is running and you are on the master node.", err.Error()))
		return nil, err
	}
	segConfigs := cluster.MustGetSegmentConfiguration(connectionPool)
	clusterData := &ClusterData{Cluster: cluster.NewCluster(segConfigs), connectionPool: connectionPool}
	err = clusterData.CountHostsAndValidateMaster()
	if err != nil {
		gplog.Error(err.Error())
		return nil, err
	}
	return clusterData, nil
}

func adaptContentIDToHostname(cluster *cluster.Cluster, f func(string) string) func(int) string {
	return func(contentId int) string {
		return f(cluster.GetHostForContent(contentId))
	}
}

func clusterRun(command *pxf.Command, clusterData *ClusterData) error {
	defer clusterData.connectionPool.Close()
	f, err := command.GetFunctionToExecute()
	if err != nil {
		gplog.Error(fmt.Sprintf("Error: %s", err))
		return err
	}

	cmdMsg := GenerateStatusReport(command, clusterData)
	clusterData.Output = clusterData.Cluster.GenerateAndExecuteCommand(
		cmdMsg,
		adaptContentIDToHostname(clusterData.Cluster, f),
		command.WhereToRun(),
	)
	return GenerateOutput(command, clusterData)
}
