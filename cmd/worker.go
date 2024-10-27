/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/jhonnyV-V/orch-in-go/worker"
	"github.com/spf13/cobra"
)

// workerCmd represents the worker command
var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Worker command to operate Cube node worker node.",
	Long: `cube worker command

The worker runs tasks and responds to the manager's requests about tasks state`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("worker called")
		host, _ := cmd.Flags().GetString("host")
		port, _ := cmd.Flags().GetInt("port")
		name, _ := cmd.Flags().GetString("name")
		dbtype, _ := cmd.Flags().GetString("dbtype")

		log.Printf("starting worker\n")

		w := worker.New(name, dbtype)
		api := worker.Api{
			Address: host,
			Port:    port,
			Worker:  w,
		}
		go w.RunTasks()
		go w.CollectStats()
		go w.UpdateTasks()
		log.Printf("starting worker %s API on http://%s:%d\n", name, host, port)
		api.Start()
	},
}

func init() {
	rootCmd.AddCommand(workerCmd)

	workerCmd.Flags().StringP("host", "H", "0.0.0.0", "Hostname or IP address")
	workerCmd.Flags().IntP("port", "p", 8089, "Port on which to listen")
	workerCmd.Flags().StringP(
		"name",
		"n",
		fmt.Sprintf("worker-%s", uuid.New().String()),
		"Name of the worker",
	)
	workerCmd.Flags().StringP(
		"dbtype",
		"d",
		"memory",
		"Type of data store to use for tasks (\"memory\" or \"persistent\")",
	)
}
