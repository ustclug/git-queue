package server

import (
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
