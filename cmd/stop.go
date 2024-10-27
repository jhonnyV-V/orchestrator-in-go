/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"log"
	"net/http"

	"github.com/spf13/cobra"
)

// stopCmd represents the stop command
var stopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stop running a task.",
	Long: `cube stop command.

The stop command stops a running task.`,
	Args: cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("stop called")
		manager, _ := cmd.Flags().GetString("manager")
		url := fmt.Sprintf("http://%s/tasks/%s", manager, args[0])

		client := &http.Client{}

		req, err := http.NewRequest("DELETE", url, nil)
		if err != nil {
			log.Printf("Error creating request %s %v\n", url, err)
			return
		}

		resp, err := client.Do(req)
		if err != nil {
			log.Printf("Error connecting to %s %v\n", url, err)
			return
		}

		if resp.StatusCode != http.StatusNoContent {
			log.Printf("Error sending request %v\n", resp.StatusCode)
			return
		}

		log.Printf("Task %v has been stopped\n", args[0])
	},
}

func init() {
	rootCmd.AddCommand(stopCmd)

	stopCmd.Flags().StringP("manager", "m", "0.0.0.0:8099", "Manager to talk to")
}
