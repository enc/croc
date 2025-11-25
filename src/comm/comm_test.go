package comm

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestComm(t *testing.T) {
	token := make([]byte, 2048)
	_, err := rand.Read(token)
	require.NoError(t, err)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	serverErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()

		c := New(conn)
		if err := c.Send([]byte("hello, world")); err != nil {
			serverErr <- err
			return
		}
		msg, err := c.Receive()
		if err != nil {
			serverErr <- err
			return
		}
		if !bytes.Equal(msg, []byte("hello, computer")) {
			serverErr <- fmt.Errorf("server expected hello, computer, got %q", string(msg))
			return
		}
		msg, err = c.Receive()
		if err != nil {
			serverErr <- err
			return
		}
		if !bytes.Equal(msg, []byte{'\x00'}) {
			serverErr <- fmt.Errorf("server expected single null byte, got %x", msg)
			return
		}
		msg, err = c.Receive()
		if err != nil {
			serverErr <- err
			return
		}
		if !bytes.Equal(msg, token) {
			serverErr <- fmt.Errorf("server expected token of length %d, got %d", len(token), len(msg))
			return
		}
		serverErr <- nil
	}()

	client, err := NewConnection(listener.Addr().String(), 5*time.Second)
	require.NoError(t, err)

	msg, err := client.Receive()
	require.NoError(t, err)
	require.Equal(t, []byte("hello, world"), msg)

	require.NoError(t, client.Send([]byte("hello, computer")))
	require.NoError(t, client.Send([]byte{'\x00'}))
	require.NoError(t, client.Send(token))

	require.NoError(t, <-serverErr)

	client.Close()
	require.Error(t, client.Send(token))
	_, err = client.Write(token)
	require.Error(t, err)
}

func TestReadRejectsOversizedFrame(t *testing.T) {
	reader, writer := net.Pipe()
	defer reader.Close()
	defer writer.Close()

	// craft a frame larger than allowed (5 MiB) so it should be rejected
	payloadSize := 5 * 1024 * 1024

	errCh := make(chan error, 1)
	go func() {
		buf := new(bytes.Buffer)
		buf.Write(MAGIC_BYTES)
		if err := binary.Write(buf, binary.LittleEndian, uint32(payloadSize)); err != nil {
			errCh <- err
			return
		}
		_, err := writer.Write(buf.Bytes())
		errCh <- err
	}()

	c := New(reader)
	_, _, _, err := c.Read()
	if err == nil {
		t.Fatalf("expected oversized frame to be rejected")
	}
	if !strings.Contains(err.Error(), "too large") {
		t.Fatalf("expected error about frame size, got: %v", err)
	}

	if writeErr := <-errCh; writeErr != nil {
		t.Fatalf("writer error: %v", writeErr)
	}
}
