/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"text/tabwriter"
	"time"

	"github.com/docker/go-units"
	"github.com/jhonnyV-V/orch-in-go/task"
	"github.com/spf13/cobra"
)

// statusCmd represents the status command
var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Status command to list tasks.",
	Long: `cube status command.

The cube command allows a user to get the status of tasks from the Cube manager`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("status called")
		manager, _ := cmd.Flags().GetString("manager")
		url := fmt.Sprintf("http://%s/tasks", manager)
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("Failed to get tasks from %s %v\n", url, err)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Fatal(err)
		}
		defer resp.Body.Close()

		var tasks []*task.Task
		err = json.Unmarshal(body, &tasks)
		if err != nil {
			log.Fatal(err)
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 5, ' ', tabwriter.TabIndent)

		fmt.Fprintln(w, "ID\tNAME\tCREATED\tSTATE\tCONAINERNAME\tIMAGE\t")
		for _, t := range tasks {
			var start string
			if t.StartTime.IsZero() {
				start = fmt.Sprintf(
					"%s ago",
					units.HumanDuration(time.Now().UTC().Sub(time.Now().UTC())),
				)
			} else {
				start = fmt.Sprintf(
					"%s ago",
					units.HumanDuration(time.Now().UTC().Sub(t.StartTime)),
				)
			}

			state := t.State.String()[t.State]

			fmt.Fprintf(
				w,
				"%s\t%s\t%s\t%s\t%s\t%s\t\n",
				t.ID,
				t.Name,
				start,
				state,
				t.Name,
				t.Image,
			)
		}
		w.Flush()

	},
}

func init() {
	rootCmd.AddCommand(statusCmd)

	statusCmd.Flags().StringP("manager", "m", "0.0.0.0:8099", "Manager to talk to")
}
