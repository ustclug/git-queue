package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/tw"
	"github.com/spf13/pflag"
	"github.com/ustclug/git-queue/pkg/queue"
)

type Config struct {
	listenAddr  string
	adminSocket string
	maxActive   int
	maxQueued   int
}

func DefaultConfig() Config {
	return Config{
		listenAddr:  "127.0.0.1:9419",
		adminSocket: "/run/git-queue/git-queue.sock",
		maxActive:   10,
		maxQueued:   1000,
	}
}

func (c *Config) InstallFlags(flagset *pflag.FlagSet) {
	flagset.StringVarP(&c.listenAddr, "listen", "l", c.listenAddr, "Address and port to listen on")
	flagset.StringVar(&c.adminSocket, "socket", c.adminSocket, "Unix socket path for admin HTTP server")
	flagset.IntVar(&c.maxActive, "max-active", c.maxActive, "Maximum number of active connections")
	flagset.IntVar(&c.maxQueued, "max-queued", c.maxQueued, "Maximum number of queued connections")
}

func (c *Config) InstallAdminFlags(flagset *pflag.FlagSet) {
	flagset.StringVar(&c.adminSocket, "socket", c.adminSocket, "Unix socket path for admin HTTP server")
}

type ConnectionInfo struct {
	Index     uint64    `json:"index"`
	Remote    string    `json:"remote"`
	Path      string    `json:"path"`
	Connected time.Time `json:"connected"`
	QueuePos  int       `json:"queue_pos"`
}

type Server struct {
	config Config

	l      net.Listener
	adminL net.Listener
	q      *queue.Queue

	nextConnIndex atomic.Uint64

	connsMu sync.Mutex
	conns   map[uint64]ConnectionInfo
}

func NewServer(config Config) *Server {
	return &Server{
		config: config,
		q:      queue.New(config.maxActive, config.maxQueued),
		conns:  make(map[uint64]ConnectionInfo),
	}
}

func (s *Server) registerConnection(connected time.Time) ConnectionInfo {
	info := ConnectionInfo{
		Index:     s.nextConnIndex.Add(1),
		Remote:    "",
		Path:      "",
		Connected: connected,
		QueuePos:  0,
	}
	s.connsMu.Lock()
	s.conns[info.Index] = info
	s.connsMu.Unlock()
	return info
}

func (s *Server) updateConnection(info ConnectionInfo) {
	s.connsMu.Lock()
	s.conns[info.Index] = info
	s.connsMu.Unlock()
}

func (s *Server) unregisterConnection(index uint64) {
	s.connsMu.Lock()
	delete(s.conns, index)
	s.connsMu.Unlock()
}

func (s *Server) snapshotConnections() []ConnectionInfo {
	s.connsMu.Lock()
	out := make([]ConnectionInfo, 0, len(s.conns))
	for _, info := range s.conns {
		out = append(out, info)
	}
	s.connsMu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		return out[i].Index < out[j].Index
	})
	return out
}

func formatRemote(attrs map[string]string, fallback string) string {
	host := attrs["REMOTE_ADDR"]
	port := attrs["REMOTE_PORT"]
	if host != "" || port != "" {
		if port == "" {
			return host
		}
		if host == "" {
			return ":" + port
		}
		return net.JoinHostPort(host, port)
	}
	return fallback
}

func (s *Server) handle(conn net.Conn, connected time.Time) {
	defer conn.Close()

	info := s.registerConnection(connected)
	defer s.unregisterConnection(info.Index)

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

	chClosed := make(chan struct{})
	go func() {
		io.Copy(io.Discard, conn)
		close(chClosed)
	}()

	info.Remote = formatRemote(attrs, "<unknown>")
	info.Path = attrs["PATH_INFO"]
	s.updateConnection(info)

	h := s.q.Acquire()
	defer h.Release()

	status := <-h.C
	if status.Full {
		fmt.Fprintf(conn, "%d\n", -1)
		return
	}

	if !status.Ok {
		current := status.Index + 1
		info.QueuePos = current
		s.updateConnection(info)
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
					info.QueuePos = 0
					s.updateConnection(info)
					break queuing
				}
				current = next.Index + 1
				info.QueuePos = current
				s.updateConnection(info)
			case <-ticker.C:
				if _, err := fmt.Fprintf(conn, "%d\n", current); err != nil {
					return
				}
			case <-chClosed:
				return
			}
		}
	}

	info.QueuePos = 0
	s.updateConnection(info)

	if _, err := fmt.Fprintf(conn, "%d\n", 0); err != nil {
		return
	}
	<-chClosed
}

func (s *Server) Start() error {
	l, err := net.Listen("tcp", s.config.listenAddr)
	if err != nil {
		return err
	}
	adminL, err := net.Listen("unix", s.config.adminSocket)
	if err != nil {
		_ = l.Close()
		return err
	}
	if err := os.Chmod(s.config.adminSocket, 0o660); err != nil {
		_ = l.Close()
		_ = adminL.Close()
		return err
	}
	s.l = l
	s.adminL = adminL
	go s.acceptLoop()
	go s.adminServeLoop()
	return nil
}

func (s *Server) Stop() error {
	err := s.l.Close()
	s.l = nil
	if s.adminL != nil {
		if e := s.adminL.Close(); err == nil {
			err = e
		}
		s.adminL = nil
	}
	if e := os.Remove(s.config.adminSocket); err == nil && e != nil && !errors.Is(e, os.ErrNotExist) {
		err = e
	}
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
		go s.handle(conn, time.Now())
	}
}

func (s *Server) adminServeLoop() {
	mux := http.NewServeMux()
	mux.HandleFunc("/connections", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(s.snapshotConnections()); err != nil {
			log.Printf("Admin write: %v", err)
		}
	})
	if err := http.Serve(s.adminL, mux); err != nil && !errors.Is(err, net.ErrClosed) {
		log.Printf("Admin serve: %v", err)
	}
}

func QueryConnections(config Config) ([]ConnectionInfo, error) {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", config.adminSocket)
		},
	}
	client := &http.Client{Transport: transport}

	resp, err := client.Get("http://unix/connections")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	defer transport.CloseIdleConnections()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("admin request failed: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}

	var infos []ConnectionInfo
	if err := json.NewDecoder(resp.Body).Decode(&infos); err != nil {
		return nil, err
	}
	return infos, nil
}

func PrintConnections(w io.Writer, infos []ConnectionInfo) error {
	table := tablewriter.NewTable(
		w,
		tablewriter.WithRendition(tw.Rendition{
			Borders: tw.BorderNone,
			Settings: tw.Settings{
				Lines:      tw.LinesNone,
				Separators: tw.SeparatorsNone,
			},
		}),
		tablewriter.WithPadding(tw.Padding{
			Right:     "  ",
			Overwrite: true,
		}),
		tablewriter.WithHeaderAutoFormat(tw.Off),
		tablewriter.WithAlignment(tw.Alignment{
			tw.AlignRight,   // Index
			tw.AlignDefault, // Remote
			tw.AlignDefault, // Path
			tw.AlignDefault, // Status
			tw.AlignDefault, // Connected
		}),
	)
	table.Header("Index", "Remote", "Path", "Status", "Connected")
	for _, info := range infos {
		status := "Active"
		if info.QueuePos > 0 {
			status = fmt.Sprintf("Queued (%d)", info.QueuePos)
		}
		table.Append([]string{
			strconv.FormatUint(info.Index, 10),
			info.Remote,
			strings.TrimSuffix(info.Path, "/git-upload-pack"),
			status,
			info.Connected.Format(time.DateTime),
		})
	}
	return table.Render()
}
