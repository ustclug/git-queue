package cmd

import (
	"github.com/spf13/cobra"
	"github.com/ustclug/git-queue/pkg/server"
)

func ServerCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "server",
		Short: "Run as server",
		Args:  cobra.NoArgs,
	}
	config := server.DefaultConfig()
	config.InstallFlags(c.Flags())
	c.RunE = func(cmd *cobra.Command, _ []string) error {
		s := server.NewServer(config)
		if err := s.Start(); err != nil {
			return err
		}
		<-make(chan struct{})
		return nil
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
