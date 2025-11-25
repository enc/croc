package comm

import (
	"bytes"
	"encoding/binary"
	"net"
	"strings"
	"testing"
)

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
