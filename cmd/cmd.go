package cmd

import (
	"github.com/spf13/cobra"
)

func ServerCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "server",
		Short: "Run as server",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return nil
		},
	}
	return c
}

func Root() *cobra.Command {
	c := &cobra.Command{
		Use:  "git-queue",
		Args: cobra.NoArgs,
	}
	c.AddCommand(ServerCmd())
	return c
}
