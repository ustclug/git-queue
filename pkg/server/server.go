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

	"github.com/BurntSushi/toml"
	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/tw"
	"github.com/spf13/pflag"

	"github.com/ustclug/git-queue/pkg/queue"
)

const UnknownFallback = "<unknown>"
const DefaultConfigPath = "/etc/git-queue/config.toml"
const DefaultAccessLogPath = "/var/log/git-queue/access.log"

type Config struct {
	listenAddr  string
	adminSocket string
	maxActive   int
	maxQueued   int
	queueRepos  []string
}

func DefaultConfig() Config {
	return Config{
		listenAddr:  "127.0.0.1:9419",
		adminSocket: "/run/git-queue/git-queue.sock",
		maxActive:   10,
		maxQueued:   1000,
		queueRepos:  []string{"/"},
	}
}

type fileConfig struct {
	Listen          string   `toml:"listen"`
	Socket          string   `toml:"socket"`
	MaxActive       int      `toml:"max_active"`
	MaxQueued       int      `toml:"max_queued"`
	QueueRepoPrefix []string `toml:"queue_repo_prefix"`
}

func InstallConfigFlag(flagset *pflag.FlagSet, path *string) {
	flagset.StringVar(path, "config", DefaultConfigPath, "Path to TOML config file")
}

func (c *Config) InstallFlags(flagset *pflag.FlagSet) {
	flagset.StringVarP(&c.listenAddr, "listen", "l", c.listenAddr, "Address and port to listen on")
	flagset.StringVar(&c.adminSocket, "socket", c.adminSocket, "Unix socket path for admin HTTP server")
	flagset.IntVar(&c.maxActive, "max-active", c.maxActive, "Maximum number of active connections")
	flagset.IntVar(&c.maxQueued, "max-queued", c.maxQueued, "Maximum number of queued connections")
	flagset.StringArrayVar(&c.queueRepos, "queue-repo-prefix", c.queueRepos, "Only queue repositories whose path starts with this prefix; repeatable")
}

func (c *Config) InstallAdminFlags(flagset *pflag.FlagSet) {
	flagset.StringVar(&c.adminSocket, "socket", c.adminSocket, "Unix socket path for admin HTTP server")
}

func (c *Config) LoadFile(path string) error {
	var fc fileConfig
	md, err := toml.DecodeFile(path, &fc)
	if err != nil {
		return err
	}
	if md.IsDefined("listen") {
		c.listenAddr = fc.Listen
	}
	if md.IsDefined("socket") {
		c.adminSocket = fc.Socket
	}
	if md.IsDefined("max_active") {
		c.maxActive = fc.MaxActive
	}
	if md.IsDefined("max_queued") {
		c.maxQueued = fc.MaxQueued
	}
	if md.IsDefined("queue_repo_prefix") {
		c.queueRepos = fc.QueueRepoPrefix
	}
	return nil
}

func (c *Config) ApplyServerFlagOverrides(overrides Config, flagset *pflag.FlagSet) {
	if flagset.Changed("listen") {
		c.listenAddr = overrides.listenAddr
	}
	if flagset.Changed("socket") {
		c.adminSocket = overrides.adminSocket
	}
	if flagset.Changed("max-active") {
		c.maxActive = overrides.maxActive
	}
	if flagset.Changed("max-queued") {
		c.maxQueued = overrides.maxQueued
	}
	if flagset.Changed("queue-repo-prefix") {
		c.queueRepos = overrides.queueRepos
	}
}

func (c *Config) ApplyAdminFlagOverrides(overrides Config, flagset *pflag.FlagSet) {
	if flagset.Changed("socket") {
		c.adminSocket = overrides.adminSocket
	}
}

func LoadOptionalConfig(path string, config *Config) error {
	if err := config.LoadFile(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			log.Printf("Config file %q not found, using defaults and flags", path)
			return nil
		}
		return fmt.Errorf("load config %q: %w", path, err)
	}
	return nil
}

type ConnectionInfo struct {
	Index      uint64    `json:"index"`
	RemoteAddr string    `json:"remote_addr"`
	RemotePort string    `json:"remote_port"`
	Path       string    `json:"path"`
	Connected  time.Time `json:"connected"`
	QueuePos   int       `json:"queue_pos"`
}

type Server struct {
	config Config

	l      net.Listener
	adminL net.Listener
	access *os.File
	logger *log.Logger
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
		Index:      s.nextConnIndex.Add(1),
		RemoteAddr: "",
		RemotePort: "",
		Path:       "",
		Connected:  connected,
		QueuePos:   0,
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

func (s *Server) connectionCounts() (active, queued int) {
	for _, info := range s.snapshotConnections() {
		if info.QueuePos > 0 {
			queued++
			continue
		}
		active++
	}
	return active, queued
}

func escapeInfluxTagValue(value string) string {
	replacer := strings.NewReplacer(",", `\,`, " ", `\ `, "=", `\=`)
	return replacer.Replace(value)
}

func formatTelegrafLine(host string, active, queued int, timestamp time.Time) string {
	return fmt.Sprintf("git-queue,host=%s active=%di,queued=%di %d\n",
		escapeInfluxTagValue(host), active, queued, timestamp.UnixNano())
}

func (s *Server) telegrafMetrics() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = UnknownFallback
	}
	active, queued := s.connectionCounts()
	return formatTelegrafLine(host, active, queued, time.Now())
}

func remoteParts(attrs map[string]string, fallback net.Addr) (string, string) {
	host := attrs["REMOTE_ADDR"]
	port := attrs["REMOTE_PORT"]
	if host != "" || port != "" {
		return host, port
	}
	if fallback == nil {
		return UnknownFallback, ""
	}
	fallbackHost, fallbackPort, err := net.SplitHostPort(fallback.String())
	if err == nil {
		return fallbackHost, fallbackPort
	}
	return fallback.String(), ""
}

func assembleRemote(addr, port string) string {
	if port == "" {
		if addr == "" {
			return UnknownFallback
		}
		return addr
	}
	if addr == "" {
		return ":" + port
	}
	return net.JoinHostPort(addr, port)
}

func accessLogRemote(info ConnectionInfo) string {
	return assembleRemote(info.RemoteAddr, info.RemotePort)
}

func formatAccessLog(info ConnectionInfo, event string, kv ...string) string {
	parts := []string{
		fmt.Sprintf("remote=%q", accessLogRemote(info)),
		fmt.Sprintf("path=%q", info.Path),
		"event=" + event,
	}
	parts = append(parts, kv...)
	return strings.Join(parts, " ")
}

func formatDurationField(key string, d time.Duration) string {
	return fmt.Sprintf("%s=%s", key, d.Round(time.Millisecond))
}

func (s *Server) logAccess(info ConnectionInfo, event string, kv ...string) {
	if s.logger == nil {
		return
	}
	s.logger.Print(formatAccessLog(info, event, kv...))
}

func (s *Server) shouldQueuePath(path string) bool {
	if len(s.config.queueRepos) == 0 {
		return true
	}
	for _, prefix := range s.config.queueRepos {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

func (s *Server) handle(conn net.Conn, connected time.Time) {
	defer conn.Close()

	info := s.registerConnection(connected)
	defer s.unregisterConnection(info.Index)

	finalEvent := "finished"
	finalFields := []string{formatDurationField("total_duration", time.Since(connected))}
	defer func() {
		finalFields[0] = formatDurationField("total_duration", time.Since(connected))
		s.logAccess(info, finalEvent, finalFields...)
	}()

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
		_, _ = io.Copy(io.Discard, conn)
		close(chClosed)
	}()

	info.RemoteAddr, info.RemotePort = remoteParts(attrs, conn.RemoteAddr())
	info.Path = attrs["PATH_INFO"]
	s.updateConnection(info)
	if !s.shouldQueuePath(info.Path) {
		if _, err := fmt.Fprintf(conn, "%d\n", 0); err != nil {
			return
		}
		<-chClosed
		return
	}

	h := s.q.Acquire()
	defer h.Release()

	status := <-h.C
	if status.Full {
		finalEvent = "rejected"
		finalFields = append(finalFields, "reason=queue_full")
		fmt.Fprintf(conn, "%d\n", -1)
		return
	}

	if !status.Ok {
		queueStarted := time.Now()
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
					s.logAccess(info, "queue_done", formatDurationField("queue_duration", time.Since(queueStarted)))
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
	access, err := os.OpenFile(DefaultAccessLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o640)
	if err != nil {
		return fmt.Errorf("open access log %q: %w", DefaultAccessLogPath, err)
	}
	l, err := net.Listen("tcp", s.config.listenAddr)
	if err != nil {
		_ = access.Close()
		return err
	}
	adminL, err := net.Listen("unix", s.config.adminSocket)
	if err != nil {
		_ = access.Close()
		_ = l.Close()
		return err
	}
	if err := os.Chmod(s.config.adminSocket, 0o660); err != nil {
		_ = access.Close()
		_ = l.Close()
		_ = adminL.Close()
		return err
	}
	s.l = l
	s.adminL = adminL
	s.access = access
	s.logger = log.New(access, "", log.LstdFlags)
	log.Printf("Access log: %s", DefaultAccessLogPath)
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
	if s.access != nil {
		if e := s.access.Close(); err == nil {
			err = e
		}
		s.access = nil
		s.logger = nil
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
	mux.HandleFunc("GET /connections", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(s.snapshotConnections()); err != nil {
			log.Printf("Admin write: %v", err)
		}
	})
	mux.HandleFunc("GET /telegraf", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, s.telegrafMetrics())
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

func PrintConnections(w io.Writer, infos []ConnectionInfo, withPort bool) error {
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
		remote := info.RemoteAddr
		if remote == "" {
			remote = UnknownFallback
		}
		if withPort {
			remote = assembleRemote(info.RemoteAddr, info.RemotePort)
		}

		status := "Active"
		if info.QueuePos > 0 {
			status = fmt.Sprintf("Queued (%d)", info.QueuePos)
		}
		if err := table.Append([]string{
			strconv.FormatUint(info.Index, 10),
			remote,
			strings.TrimSuffix(info.Path, "/git-upload-pack"),
			status,
			info.Connected.Format(time.DateTime),
		}); err != nil {
			return fmt.Errorf("table.Append: %w", err)
		}
	}
	return table.Render()
}
