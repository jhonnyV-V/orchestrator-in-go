/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"log"

	"github.com/jhonnyV-V/orch-in-go/manager"
	"github.com/spf13/cobra"
)

// managerCmd represents the manager command
var managerCmd = &cobra.Command{
	Use:   "manager",
	Short: "Manager command to operate Cube manager node",
	Long: `cube manager command.

The manager controls the orchestration system and is reponsible for:
- Accepting tasks from users
- Scheduling tasks onto worker nodes
- Rescheduling tasks in the event of a node failure
- Periodically polling workers to get tasks updates`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("manager called")
		host, _ := cmd.Flags().GetString("host")
		port, _ := cmd.Flags().GetInt("port")
		workers, _ := cmd.Flags().GetStringSlice("workers")
		scheduler, _ := cmd.Flags().GetString("scheduler")
		dbtype, _ := cmd.Flags().GetString("dbtype")

		m := manager.New(workers, scheduler, dbtype)
		api := manager.Api{
			Address: host,
			Port:    port,
			Manager: m,
		}

		go m.ProcessTasks()
		go m.UpdateTasks()
		go m.DoHealthChecks()

		log.Printf("Starting manager API on http://%s:%d\n", host, port)
		api.Start()
	},
}

func init() {
	rootCmd.AddCommand(managerCmd)

	managerCmd.Flags().StringP("host", "H", "0.0.0.0", "Hostname or IP address")
	managerCmd.Flags().IntP("port", "p", 8099, "Port on which to listen")
	managerCmd.Flags().StringSliceP(
		"workers",
		"w",
		[]string{"0.0.0.0:8099"},
		"List of workers on which the manager will schedule tasks.",
	)
	managerCmd.Flags().StringP("scheduler", "s", "epvm", "Name of scheduler to use")
	managerCmd.Flags().StringP(
		"dbtype",
		"d",
		"memory",
		"Type of data store to use for tasks (\"memory\" or \"persistent\")",
	)
}
