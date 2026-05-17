package server

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
