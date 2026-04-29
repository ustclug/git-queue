package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strings"

	"github.com/spf13/pflag"
)

func handle(conn net.Conn) {
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
	fmt.Fprintf(conn, "%d\n", 0)
	io.Copy(io.Discard, conn)
}

func main() {
	var listenAddr string
	pflag.StringVarP(&listenAddr, "listen", "l", ":9419", "Address and port to listen on")
	pflag.Parse()

	l, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("Accept: %v", err)
			continue
		}
		go handle(conn)
	}
}
