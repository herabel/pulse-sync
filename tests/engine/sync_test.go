package engine_test

import (
	"bytes"
	"crypto/rand"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/herabel/pulse-sync/pkg/engine"
)

func TestSync_FreshFile(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.bin")
	targetPath := filepath.Join(dir, "target.bin")

	data := make([]byte, 2*1024*1024) // 2 MB
	_, _ = rand.Read(data)
	if err := os.WriteFile(sourcePath, data, 0644); err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	var wg sync.WaitGroup
	var serverErr, clientErr error

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := listener.Accept()
		if err != nil {
			serverErr = err
			return
		}
		defer conn.Close()
		serverErr = engine.ReceiveFile(conn, targetPath)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			clientErr = err
			return
		}
		defer conn.Close()
		clientErr = engine.SendFile(conn, sourcePath)
	}()

	wg.Wait()

	if serverErr != nil {
		t.Fatalf("Server ReceiveFile error: %v", serverErr)
	}
	if clientErr != nil {
		t.Fatalf("Client SendFile error: %v", clientErr)
	}

	receivedData, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("Failed to read received target: %v", err)
	}

	if !bytes.Equal(data, receivedData) {
		t.Fatalf("Data mismatch: received len %d != source len %d", len(receivedData), len(data))
	}
}

func TestSync_DifferentialUpdate(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.bin")
	targetPath := filepath.Join(dir, "target.bin")

	// 1. Изначальный файл (4 МБ)
	initialData := make([]byte, 4*1024*1024)
	_, _ = rand.Read(initialData)
	if err := os.WriteFile(targetPath, initialData, 0644); err != nil {
		t.Fatal(err)
	}

	// 2. Модифицированный файл на источнике (меняем середину)
	sourceData := make([]byte, len(initialData))
	copy(sourceData, initialData)
	for i := 1024 * 1024; i < 1024*1024+32768; i++ {
		sourceData[i] ^= 0xEE
	}
	if err := os.WriteFile(sourcePath, sourceData, 0644); err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	var wg sync.WaitGroup
	var serverErr, clientErr error

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := listener.Accept()
		if err != nil {
			serverErr = err
			return
		}
		defer conn.Close()
		serverErr = engine.ReceiveFile(conn, targetPath)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			clientErr = err
			return
		}
		defer conn.Close()
		clientErr = engine.SendFile(conn, sourcePath)
	}()

	wg.Wait()

	if serverErr != nil {
		t.Fatalf("Server ReceiveFile error: %v", serverErr)
	}
	if clientErr != nil {
		t.Fatalf("Client SendFile error: %v", clientErr)
	}

	updatedData, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("Failed to read updated target: %v", err)
	}

	if !bytes.Equal(sourceData, updatedData) {
		t.Fatalf("Data mismatch after differential sync")
	}
}

func TestSync_IdenticalFile(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.bin")
	targetPath := filepath.Join(dir, "target.bin")

	data := make([]byte, 1024*1024)
	_, _ = rand.Read(data)
	if err := os.WriteFile(sourcePath, data, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(targetPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	var wg sync.WaitGroup
	var serverErr, clientErr error

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := listener.Accept()
		if err != nil {
			serverErr = err
			return
		}
		defer conn.Close()
		serverErr = engine.ReceiveFile(conn, targetPath)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			clientErr = err
			return
		}
		defer conn.Close()
		clientErr = engine.SendFile(conn, sourcePath)
	}()

	wg.Wait()

	if serverErr != nil {
		t.Fatalf("Server ReceiveFile error: %v", serverErr)
	}
	if clientErr != nil {
		t.Fatalf("Client SendFile error: %v", clientErr)
	}
}
