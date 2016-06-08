package spade

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCompression(t *testing.T) {
	testEvent := NewEvent(time.Now(), net.IPv4(10, 0, 0, 0), "xForwardedFor", "uuid", "data")
	assert.NotNil(t, testEvent)

	b, err := Compress(testEvent)
	assert.NoError(t, err)
	assert.NotEmpty(t, b)

	newEvent, err := Decompress(b)
	assert.NoError(t, err)
	assert.NotNil(t, newEvent)

	assert.Equal(t, testEvent, newEvent)
}

func TestWrongVersion(t *testing.T) {
	var err error
	fakeData := []byte{COMPRESSION_VERSION + 1}
	_, err = Decompress(fakeData)
	assert.Contains(t, err.Error(), "Unknown version")
}
