package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/herabel/pulse-sync/pkg/engine"
)

func main() {
	listenAddr := flag.String("listen", "", "TCP address to listen on (e.g. :9000 or 0.0.0.0:9000)")
	targetAddr := flag.String("target", "", "Remote TCP receiver address to send to (e.g. 192.168.1.50:9000)")
	filePath := flag.String("file", "", "Path to local file to send")
	outPath := flag.String("out", "", "Destination path for received file (optional)")

	flag.Parse()

	if *listenAddr == "" && *targetAddr == "" {
		fmt.Println("pulse-sync: fast differential file synchronization daemon")
		fmt.Println("\nUsage:")
		fmt.Println("  Receive mode : pulse-sync -listen :9000 [-out /path/to/target.bin]")
		fmt.Println("  Send mode    : pulse-sync -target 192.168.1.50:9000 -file /path/to/source.bin")
		os.Exit(1)
	}

	if *listenAddr != "" {
		runReceiver(*listenAddr, *outPath)
		return
	}

	if *targetAddr != "" {
		if *filePath == "" {
			fmt.Fprintln(os.Stderr, "error: -file is required in sender mode")
			os.Exit(1)
		}
		runSender(*targetAddr, *filePath)
		return
	}
}

func runReceiver(addr string, outPath string) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to listen on %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer ln.Close()

	fmt.Printf("[pulse-sync] listening on %s (waiting for incoming peers)...\n", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "accept error: %v\n", err)
			continue
		}

		remote := conn.RemoteAddr().String()
		fmt.Printf("[pulse-sync] accepted connection from %s\n", remote)

		start := time.Now()
		err = engine.ReceiveFile(conn, outPath)
		conn.Close()

		if err != nil {
			fmt.Fprintf(os.Stderr, "[pulse-sync] sync failed from %s: %v\n", remote, err)
		} else {
			elapsed := time.Since(start)
			fmt.Printf("[pulse-sync] sync completed from %s in %s\n", remote, elapsed.Round(time.Millisecond))
		}
	}
}

func runSender(target string, filePath string) {
	fi, err := os.Stat(filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open %s: %v\n", filePath, err)
		os.Exit(1)
	}

	fmt.Printf("[pulse-sync] connecting to %s...\n", target)
	start := time.Now()

	conn, err := net.Dial("tcp", target)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connection failed to %s: %v\n", target, err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Printf("[pulse-sync] sending %s (%s)...\n", filePath, formatBytes(fi.Size()))

	err = engine.SendFile(conn, filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[pulse-sync] send error: %v\n", err)
		os.Exit(1)
	}

	elapsed := time.Since(start)
	speedMB := float64(fi.Size()) / (1024 * 1024) / elapsed.Seconds()

	fmt.Printf("[pulse-sync] finished in %s (effective throughput: %.2f MB/s)\n",
		elapsed.Round(time.Millisecond), speedMB)
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
