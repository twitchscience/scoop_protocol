package spade

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDetermineBase64Encoding(t *testing.T) {
	assert.Equal(t, base64.StdEncoding, DetermineBase64Encoding([]byte("wacn2oi3f90n2j238j+/")))
	assert.Equal(t, base64.StdEncoding, DetermineBase64Encoding([]byte("wacn2oi3f90n2j238j")))
	assert.Equal(t, base64.URLEncoding, DetermineBase64Encoding([]byte("wacn2oi3f90n2j2_8j")))
	assert.Equal(t, base64.URLEncoding, DetermineBase64Encoding([]byte("-acn2oi3f90n2j238j")))
	assert.Equal(t, base64.URLEncoding, DetermineBase64Encoding([]byte("acn2oi3f90n2j238j_")))
	assert.Equal(t, SpaceEncoding, DetermineBase64Encoding([]byte("wacn2oi3f90n2j238j /")))
	assert.Equal(t, SpaceEncoding, DetermineBase64Encoding([]byte(" acn2oi3f90n2j238j /")))
	assert.Equal(t, SpaceEncoding, DetermineBase64Encoding([]byte("acn2oi3f90n2j238j/ ")))
}
