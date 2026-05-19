package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/ustclug/git-queue/pkg/server"
)

func ServerCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "server",
		Short: "Run as server",
		Args:  cobra.NoArgs,
	}
	configPath := server.DefaultConfigPath
	server.InstallConfigFlag(c.Flags(), &configPath)
	server.DefaultConfig().InstallFlags(c.Flags())
	c.RunE = func(cmd *cobra.Command, _ []string) error {
		config, err := server.LoadConfig(configPath, cmd.Flags())
		if err != nil {
			return err
		}
		s := server.NewServer(config)
		if err := s.Start(); err != nil {
			return err
		}
		<-make(chan struct{})
		return nil
	}
	return c
}

func ConnectionsCmd() *cobra.Command {
	c := &cobra.Command{
		Use:     "connections",
		Aliases: []string{"conn", "conns"},
		Short:   "Show active connections",
		Args:    cobra.NoArgs,
	}
	var withPort bool
	configPath := server.DefaultConfigPath
	server.InstallConfigFlag(c.Flags(), &configPath)
	server.DefaultConfig().InstallAdminFlags(c.Flags())
	c.Flags().BoolVarP(&withPort, "port", "p", false, "Show remote port in output")
	c.RunE = func(cmd *cobra.Command, _ []string) error {
		config, err := server.LoadAdminConfig(configPath, cmd.Flags())
		if err != nil {
			return fmt.Errorf("load config: %w", err)
		}
		infos, err := server.QueryConnections(config)
		if err != nil {
			return fmt.Errorf("query connections: %w", err)
		}
		return server.PrintConnections(os.Stdout, infos, withPort)
	}
	return c
}

func Root() *cobra.Command {
	c := &cobra.Command{
		Use:  "git-queue",
		Args: cobra.NoArgs,
	}
	c.AddCommand(ServerCmd())
	c.AddCommand(ConnectionsCmd())
	return c
}
