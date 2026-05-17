package server

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeTestConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	return path
}

func TestConnectionCounts(t *testing.T) {
	s := &Server{
		conns: map[uint64]ConnectionInfo{
			1: {Index: 1, QueuePos: 0},
			2: {Index: 2, QueuePos: 3},
			3: {Index: 3, QueuePos: 0},
			4: {Index: 4, QueuePos: 1},
		},
	}

	active, queued := s.connectionCounts()
	require.Equal(t, 2, active)
	assert.Equal(t, 2, queued)
}

func TestFormatTelegrafLine(t *testing.T) {
	line := formatTelegrafLine("host name,prod=1", 10, 15, time.Unix(0, 1778837100123456789))
	assert.Equal(t, "git-queue,host=host\\ name\\,prod\\=1 active=10i,queued=15i 1778837100123456789\n", line)
}

func TestPrintConnections_DefaultRemoteWithoutPort(t *testing.T) {
	var b bytes.Buffer
	err := PrintConnections(&b, []ConnectionInfo{{
		Index:      1,
		RemoteAddr: "192.0.2.12",
		RemotePort: "443",
		Path:       "/repo.git/git-upload-pack",
		Connected:  time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC),
	}}, false)
	require.NoError(t, err)

	out := b.String()
	assert.Contains(t, out, "192.0.2.12")
	assert.NotContains(t, out, "192.0.2.12:443")
}

func TestPrintConnections_WithPort(t *testing.T) {
	var b bytes.Buffer
	err := PrintConnections(&b, []ConnectionInfo{{
		Index:      1,
		RemoteAddr: "192.0.2.12",
		RemotePort: "443",
		Path:       "/repo.git/git-upload-pack",
		Connected:  time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC),
	}}, true)
	require.NoError(t, err)

	out := b.String()
	assert.True(t, strings.Contains(out, "192.0.2.12:443") || strings.Contains(out, "[192.0.2.12]:443"))
}

func TestShouldQueuePath(t *testing.T) {
	s := NewServer(Config{
		queueRepos: []string{"/queued", "/group/"},
	})

	assert.True(t, s.shouldQueuePath("/queued-repo.git/git-upload-pack"))
	assert.True(t, s.shouldQueuePath("/group/repo.git/git-receive-pack"))
	assert.False(t, s.shouldQueuePath("/other/repo.git/git-upload-pack"))
}

func TestShouldQueuePathWithoutPrefixesQueuesAll(t *testing.T) {
	s := NewServer(DefaultConfig())
	assert.True(t, s.shouldQueuePath("/other/repo.git/git-upload-pack"))
}

func TestDefaultConfigQueuesAllByDefault(t *testing.T) {
	config := DefaultConfig()
	require.Equal(t, []string{"/"}, config.queueRepos)
	assert.True(t, NewServer(config).shouldQueuePath("/any/repo.git/git-upload-pack"))
}

func TestLoadFile(t *testing.T) {
	path := writeTestConfig(t, strings.Join([]string{
		"listen = \"0.0.0.0:1234\"",
		"socket = \"/tmp/git-queue.sock\"",
		"max_active = 21",
		"max_queued = 34",
		"queue_repo_prefix = [\"/team/\", \"/ops/\"]",
	}, "\n"))

	config := DefaultConfig()
	require.NoError(t, config.LoadFile(path))

	assert.Equal(t, "0.0.0.0:1234", config.listenAddr)
	assert.Equal(t, "/tmp/git-queue.sock", config.adminSocket)
	assert.Equal(t, 21, config.maxActive)
	assert.Equal(t, 34, config.maxQueued)
	assert.Equal(t, []string{"/team/", "/ops/"}, config.queueRepos)
}

func TestLoadOptionalConfigMissingLogsAndKeepsDefaults(t *testing.T) {
	config := DefaultConfig()
	path := filepath.Join(t.TempDir(), "missing.toml")

	var b bytes.Buffer
	oldWriter := log.Writer()
	log.SetOutput(&b)
	t.Cleanup(func() {
		log.SetOutput(oldWriter)
	})

	require.NoError(t, LoadOptionalConfig(path, &config))
	assert.Contains(t, b.String(), "using defaults and flags")
	assert.Equal(t, DefaultConfig(), config)
}

func TestApplyServerFlagOverrides(t *testing.T) {
	config := DefaultConfig()
	path := writeTestConfig(t, strings.Join([]string{
		"listen = \"0.0.0.0:1234\"",
		"socket = \"/tmp/from-file.sock\"",
		"max_active = 21",
		"max_queued = 34",
		"queue_repo_prefix = [\"/team/\"]",
	}, "\n"))
	require.NoError(t, config.LoadFile(path))

	flagConfig := DefaultConfig()
	flags := pflag.NewFlagSet("server", pflag.ContinueOnError)
	flagConfig.InstallFlags(flags)
	require.NoError(t, flags.Parse([]string{
		"--listen=127.0.0.1:9999",
		"--max-active=7",
		"--queue-repo-prefix=/cli/",
	}))

	config.ApplyServerFlagOverrides(flagConfig, flags)

	assert.Equal(t, "127.0.0.1:9999", config.listenAddr)
	assert.Equal(t, "/tmp/from-file.sock", config.adminSocket)
	assert.Equal(t, 7, config.maxActive)
	assert.Equal(t, 34, config.maxQueued)
	assert.Equal(t, []string{"/cli/"}, config.queueRepos)
}

func TestApplyAdminFlagOverrides(t *testing.T) {
	config := DefaultConfig()
	path := writeTestConfig(t, "socket = \"/tmp/from-file.sock\"\n")
	require.NoError(t, config.LoadFile(path))

	flagConfig := DefaultConfig()
	flags := pflag.NewFlagSet("connections", pflag.ContinueOnError)
	flagConfig.InstallAdminFlags(flags)
	require.NoError(t, flags.Parse([]string{"--socket=/tmp/from-flag.sock"}))

	config.ApplyAdminFlagOverrides(flagConfig, flags)
	assert.Equal(t, "/tmp/from-flag.sock", config.adminSocket)
}
