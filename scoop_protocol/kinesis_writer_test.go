package scoop_protocol

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var FirehoseRedshiftStreamTestConfig = []byte(`
    {
        "StreamName": "spade-processed-integration-jackgao-coview-redshift-test",
        "StreamType": "firehose",
        "Compress": false,
        "FirehoseRedshiftStream": true,
        "Events": {
            "minute-watched": {
                "Fields": [
                    "country",
                    "device_id"
                ]
            }
        },
        "BufferSize": 1024,
        "MaxAttemptsPerRecord": 10,
        "RetryDelay": "1s",
        "Globber": {
            "MaxSize": 990000,
            "MaxAge": "1s",
            "BufferLength": 1024
        },
        "Batcher": {
            "MaxSize": 990000,
            "MaxEntries": 500,
            "MaxAge": "1s",
            "BufferLength": 1024
        }
    }
  `)

func TestConfigValidation(t *testing.T) {
	config := KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	assert.Nil(t, config.Validate(), "config could not be validated")
}

func TestRedshiftStreamAndCompressValidation(t *testing.T) {
	config := KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	config.Compress = true

	// firehose->redshift streaming cannot be used with compress mode
	assert.NotNil(t, config.Validate(), "redshift streaming and compress cannot both be on")
}

func TestRedshiftStreamAndStreamValidation(t *testing.T) {
	config := KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	config.StreamType = "stream"

	// firehose->redshift streaming can only be used with firehose
	assert.NotNil(t, config.Validate(), "redshift streaming can only be used with firehose")
}

func TestFieldRenaming(t *testing.T) {
	config := KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	config.Events["minute-watched"].FieldRenames = map[string]string{
		"country": "renamed_country",
	}

	require.Nil(t, config.Validate(), "config could not be validated")
	assert.Equal(t, map[string]string{"country": "renamed_country", "device_id": "device_id"},
		config.Events["minute-watched"].FullFieldMap)
}
