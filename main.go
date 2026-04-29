package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"

	"github.com/spf13/pflag"
)

const ScriptFilename = "/usr/lib/git-core/git-http-backend"

var (
	listenAddr  string
	projectRoot string
	gitPath     string
)

func handleHTTP(w http.ResponseWriter, r *http.Request) {
	// Print request header to stdout for debugging
	fmt.Printf("%s %s %s\n", r.Method, r.URL.Path, r.Proto)
	for name, values := range r.Header {
		for _, value := range values {
			fmt.Println(name+":", value)
		}
	}
	fmt.Println()

	cmd := exec.Command(gitPath)
	cmd.Env = append(cmd.Env,
		// Standard HTTP CGI environment variables
		fmt.Sprintf("REQUEST_METHOD=%s", r.Method),
		fmt.Sprintf("QUERY_STRING=%s", r.URL.RawQuery),
		fmt.Sprintf("CONTENT_TYPE=%s", r.Header.Get("Content-Type")),
		fmt.Sprintf("CONTENT_LENGTH=%d", r.ContentLength),

		// Git-specific environment variables
		fmt.Sprintf("GIT_PROJECT_ROOT=%s", projectRoot),
		fmt.Sprintf("PATH_INFO=%s", r.URL.Path),
		fmt.Sprintf("GIT_PROTOCOL=%s", r.Header.Get("Git-Protocol")),
	)
	cmd.Stdin = r.Body
	defer r.Body.Close()
	out := new(bytes.Buffer)
	cmd.Stdout = out
	cmd.Stderr = out

	if err := cmd.Run(); err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		fmt.Println("Error executing git-http-backend:", err)
		return
	}
	fmt.Println("Git backend OK")

	// parse HTTP headers from CGI output
	for {
		line, err := out.ReadString('\n')
		if err != nil {
			break
		}
		line = strings.TrimSpace(line)
		if line == "" {
			break // end of headers
		}
		parts := strings.SplitN(line, ": ", 2)
		if len(parts) == 2 {
			fmt.Println(line)
			w.Header().Set(parts[0], parts[1])
		} else {
			fmt.Println("Invalid header line:", line)
		}
	}
	w.WriteHeader(http.StatusOK)
	io.CopyN(w, io.TeeReader(out, os.Stdout), 100)
	io.Copy(w, out)
	fmt.Println("\n")
}

func main() {
	pflag.StringVarP(&gitPath, "git-path", "g", ScriptFilename, "Path to git-http-backend")
	pflag.StringVarP(&listenAddr, "listen", "l", ":8080", "Address to listen on")
	pflag.StringVarP(&projectRoot, "root", "r", "/srv/git", "Project root directory")
	pflag.Parse()

	s := &http.Server{
		Addr:    listenAddr,
		Handler: http.HandlerFunc(handleHTTP),
	}

	if err := s.ListenAndServe(); err != nil {
		panic(err)
	}
}
