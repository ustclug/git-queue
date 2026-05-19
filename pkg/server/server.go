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
	"github.com/spf13/viper"

	"github.com/ustclug/git-queue/pkg/queue"
)

const UnknownFallback = "<unknown>"
const DefaultConfigPath = "/etc/git-queue/config.toml"
const DefaultAccessLogPath = "/var/log/git-queue/access.log"

type Config struct {
	ListenAddr  string   `mapstructure:"listen"`
	AdminSocket string   `mapstructure:"socket"`
	MaxActive   int      `mapstructure:"max_active"`
	MaxQueued   int      `mapstructure:"max_queued"`
	QueueRepos  []string `mapstructure:"queue_repo_prefix"`
}

func DefaultConfig() Config {
	return Config{
		ListenAddr:  "127.0.0.1:9419",
		AdminSocket: "/run/git-queue/git-queue.sock",
		MaxActive:   10,
		MaxQueued:   1000,
		QueueRepos:  []string{"/"},
	}
}

func InstallConfigFlag(flagset *pflag.FlagSet, path *string) {
	flagset.StringVar(path, "config", DefaultConfigPath, "Path to TOML config file")
}

func (c Config) InstallFlags(flagset *pflag.FlagSet) {
	flagset.StringP("listen", "l", c.ListenAddr, "Address and port to listen on")
	flagset.String("socket", c.AdminSocket, "Unix socket path for admin HTTP server")
	flagset.Int("max-active", c.MaxActive, "Maximum number of active connections")
	flagset.Int("max-queued", c.MaxQueued, "Maximum number of queued connections")
	flagset.StringArray("queue-repo-prefix", c.QueueRepos, "Only queue repositories whose path starts with this prefix; repeatable")
}

func (c Config) InstallAdminFlags(flagset *pflag.FlagSet) {
	flagset.String("socket", c.AdminSocket, "Unix socket path for admin HTTP server")
}

func (c Config) applyDefaults(v *viper.Viper) {
	v.SetDefault("listen", c.ListenAddr)
	v.SetDefault("socket", c.AdminSocket)
	v.SetDefault("max_active", c.MaxActive)
	v.SetDefault("max_queued", c.MaxQueued)
	v.SetDefault("queue_repo_prefix", c.QueueRepos)
}

func bindConfigFlag(v *viper.Viper, flagset *pflag.FlagSet, key, name string) error {
	flag := flagset.Lookup(name)
	if flag == nil {
		return nil
	}
	if err := v.BindPFlag(key, flag); err != nil {
		return fmt.Errorf("bind flag %q: %w", name, err)
	}
	return nil
}

func bindConfigFlags(v *viper.Viper, flagset *pflag.FlagSet) error {
	if flagset == nil {
		return nil
	}
	for key, name := range map[string]string{
		"listen":            "listen",
		"socket":            "socket",
		"max_active":        "max-active",
		"max_queued":        "max-queued",
		"queue_repo_prefix": "queue-repo-prefix",
	} {
		if err := bindConfigFlag(v, flagset, key, name); err != nil {
			return err
		}
	}
	return nil
}

func bindAdminConfigFlags(v *viper.Viper, flagset *pflag.FlagSet) error {
	return bindConfigFlag(v, flagset, "socket", "socket")
}

func LoadConfig(path string, flagset *pflag.FlagSet) (Config, error) {
	return loadConfig(path, flagset, bindConfigFlags)
}

func LoadAdminConfig(path string, flagset *pflag.FlagSet) (Config, error) {
	return loadConfig(path, flagset, bindAdminConfigFlags)
}

func loadConfig(path string, flagset *pflag.FlagSet, bindFlags func(*viper.Viper, *pflag.FlagSet) error) (Config, error) {
	config := DefaultConfig()
	v := viper.New()
	config.applyDefaults(v)
	if err := bindFlags(v, flagset); err != nil {
		return Config{}, err
	}
	if path != "" {
		v.SetConfigFile(path)
		v.SetConfigType("toml")
		if err := v.ReadInConfig(); err != nil {
			var configFileNotFound viper.ConfigFileNotFoundError
			if errors.As(err, &configFileNotFound) || errors.Is(err, os.ErrNotExist) {
				log.Printf("Config file %q not found, using defaults and flags", path)
			} else {
				return Config{}, fmt.Errorf("load config %q: %w", path, err)
			}
		}
	}
	if err := v.Unmarshal(&config); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	return config, nil
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
		q:      queue.New(config.MaxActive, config.MaxQueued),
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
	parts := make([]string, 0, 3+len(kv))
	parts = append(parts,
		fmt.Sprintf("remote=%q", accessLogRemote(info)),
		fmt.Sprintf("path=%q", info.Path),
		"event="+event,
	)
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
	if len(s.config.QueueRepos) == 0 {
		return true
	}
	for _, prefix := range s.config.QueueRepos {
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
	l, err := net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		_ = access.Close()
		return err
	}
	adminL, err := net.Listen("unix", s.config.AdminSocket)
	if err != nil {
		_ = access.Close()
		_ = l.Close()
		return err
	}
	if err := os.Chmod(s.config.AdminSocket, 0o660); err != nil {
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
	if e := os.Remove(s.config.AdminSocket); err == nil && e != nil && !errors.Is(e, os.ErrNotExist) {
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
			return d.DialContext(ctx, "unix", config.AdminSocket)
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
