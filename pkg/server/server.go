package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"github.com/ustclug/git-queue/pkg/queue"
)

type Config struct {
	listenAddr string
	maxActive  int
	maxQueued  int
}

func DefaultConfig() Config {
	return Config{
		listenAddr: ":9419",
		maxActive:  10,
		maxQueued:  1000,
	}
}

func (c *Config) InstallFlags(flagset *pflag.FlagSet) {
	flagset.StringVarP(&c.listenAddr, "listen", "l", c.listenAddr, "Address and port to listen on")
	flagset.IntVar(&c.maxActive, "max-active", c.maxActive, "Maximum number of active connections")
	flagset.IntVar(&c.maxQueued, "max-queued", c.maxQueued, "Maximum number of queued connections")
}

type Server struct {
	config Config

	l net.Listener
	q *queue.Queue
}

func NewServer(config Config) *Server {
	return &Server{
		config: config,
		q:      queue.New(config.maxActive, config.maxQueued),
	}
}

func (s *Server) handle(conn net.Conn) {
	defer conn.Close()

	attrs := make(map[string]string)
	r := bufio.NewScanner(conn)
	for r.Scan() {
		if r.Text() == "%" {
			break
		}
		if strings.TrimSpace(r.Text()) == "" {
			continue
		}
		parts := strings.SplitN(r.Text(), "=", 2)
		if len(parts) < 2 {
			log.Printf("Missing \"=\": %q", r.Text())
		}
		attrs[parts[0]] = parts[1]
	}

	h := s.q.Acquire()
	defer h.Release()

	status := <-h.C
	if status.Full {
		fmt.Fprintf(conn, "%d\n", -1)
		return
	}

	if !status.Ok {
		current := status.Index + 1
		if _, err := fmt.Fprintf(conn, "%d\n", current); err != nil {
			return
		}

		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

	queuing:
		for {
			select {
			case next := <-h.C:
				if next.Ok {
					break queuing
				}
				current = next.Index + 1
			case <-ticker.C:
				if _, err := fmt.Fprintf(conn, "%d\n", current); err != nil {
					return
				}
			}
		}
	}

	if _, err := fmt.Fprintf(conn, "%d\n", 0); err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, conn)
}

func (s *Server) Start() error {
	l, err := net.Listen("tcp", s.config.listenAddr)
	if err != nil {
		return err
	}
	s.l = l
	go s.acceptLoop()
	return nil
}

func (s *Server) Stop() error {
	err := s.l.Close()
	s.l = nil
	return err
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.l.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			log.Printf("Accept: %v", err)
			continue
		}
		go s.handle(conn)
	}
}
