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
            },
            "pageview": {
                "Fields": [
                    "login"
                ],
                "Filter": "isOneOf",
                "FilterParameters": [{
                    "Field": "login",
                    "Values": ["test_login"],
                    "Operator": "in_set"
                }]
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
	assert.NoError(t, config.Validate(), "config could not be validated")
}

func TestRedshiftStreamAndCompressValidation(t *testing.T) {
	config := KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	config.Compress = true

	// firehose->redshift streaming cannot be used with compress mode
	assert.Error(t, config.Validate(), "redshift streaming and compress cannot both be on")
}

func TestRedshiftStreamAndStreamValidation(t *testing.T) {
	config := KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	config.StreamType = "stream"

	// firehose->redshift streaming can only be used with firehose
	assert.Error(t, config.Validate(), "redshift streaming can only be used with firehose")
}

func TestFieldRenaming(t *testing.T) {
	config := KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	config.Events["minute-watched"].FieldRenames = map[string]string{
		"country": "renamed_country",
	}

	require.NoError(t, config.Validate(), "config could not be validated")
	assert.Equal(t, map[string]string{"country": "renamed_country", "device_id": "device_id"},
		config.Events["minute-watched"].FullFieldMap)
}

func TestRegionValidation(t *testing.T) {
	config := KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	config.StreamRegion = "us-west-2"
	assert.NoError(t, config.Validate(), "valid region didn't work")
	config.StreamRegion = "us-west-3"
	assert.Error(t, config.Validate(), "invalid region worked")
}

func TestFilterFuncValidation(t *testing.T) {
	config := KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	config.Events["pageview"].Filter = "invalid_filter"
	assert.Error(t, config.Validate(), "invalid filter func worked")
}

func TestNoFilterFuncParametersValidation(t *testing.T) {
	config := KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	config.Events["pageview"].FilterParameters = nil
	assert.Error(t, config.Validate(), "Providing filter with no parameters worked")
}

func TestInvalidFilterFuncParametersValidation(t *testing.T) {
	testCases := []struct {
		fieldValue string
		values     []string
		op         FilterOperator
		msg        string
	}{
		{"", []string{"b", "a"}, IN_SET, "Providing empty filter field worked"},
		{"a", nil, IN_SET, "Providing nil filter values worked"},
		{"a", []string{}, IN_SET, "Providing no filter values worked"},
		{"a", []string{"b", "a"}, "bad", "Providing invalid filter operator worked"},
		{"a", []string{"b", "a"}, "", "Providing empty filter operator worked"},
	}
	config := KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	for _, tc := range testCases {
		config.Events["pageview"].FilterParameters = []*KinesisEventFilterConfig{
			&KinesisEventFilterConfig{tc.fieldValue, tc.values, tc.op},
		}
		assert.Error(t, config.Validate(), tc.msg)
	}
}

func TestFilterFuncs(t *testing.T) {
	testCases := []struct {
		filterName string
		event      map[string]string
		result     bool
	}{
		{
			"isAGSEvent",
			map[string]string{"adg_product_id": "600505cc-de2f-4b99-9960-c47ee5d23f04"},
			true,
		},
		{"isAGSEvent", map[string]string{"adg_product_id": ""}, false},
		{"isAGSEvent", map[string]string{"time": ""}, false},
		{"isChannelIDSet", map[string]string{"channel_id": "xxx"}, true},
		{"isChannelIDSet", map[string]string{"channel_id": ""}, false},
		{"isChannelIDSet", map[string]string{"time": ""}, false},
		{"isUserIDSet", map[string]string{"user_id": "xxx"}, true},
		{"isUserIDSet", map[string]string{"user_id": ""}, false},
		{"isUserIDSet", map[string]string{"time": ""}, false},
		{"isVod", map[string]string{"vod_id": "xx", "vod_type": "archive"}, true},
		{"isVod", map[string]string{"vod_id": "xx", "vod_type": "clip"}, false},
		{"isVod", map[string]string{"vod_id": "", "vod_type": "archive"}, false},
		{"isVod", map[string]string{"vod_id": "xx"}, true},
		{"isVod", map[string]string{"vod_id": "xx", "vod_type": ""}, true},
		{"isVod", map[string]string{"time": ""}, false},
		{"isLiveClipContent", map[string]string{"time": ""}, false},
		{"isLiveClipContent", map[string]string{"source_content_type": "other"}, false},
		{"isLiveClipContent", map[string]string{"source_content_type": "live"}, true},
		{"isTwilightApp", map[string]string{"time": ""}, false},
		{"isTwilightApp", map[string]string{"client_app": "non-twilight"}, false},
		{"isTwilightApp", map[string]string{"client_app": "twilight"}, true},
	}
	for _, tc := range testCases {
		assert.Equal(t, tc.result, filterFuncs[tc.filterName](tc.event),
			"filter: %v; map: %v", tc.filterName, tc.event)
	}
}

func TestEventFilterMatch(t *testing.T) {
	testCases := []struct {
		filterValue  string
		filterConfig *KinesisEventFilterConfig
		result       bool
	}{
		{"a", &KinesisEventFilterConfig{"x", []string{"a"}, IN_SET}, true},
		{"a", &KinesisEventFilterConfig{"x", []string{"b", "a"}, IN_SET}, true},
		{"a", &KinesisEventFilterConfig{"x", []string{"b", "c"}, IN_SET}, false},
		{"a", &KinesisEventFilterConfig{"x", []string{""}, IN_SET}, false},
		{"", &KinesisEventFilterConfig{"x", []string{"b", "c"}, IN_SET}, false},
		{"", &KinesisEventFilterConfig{"x", []string{""}, IN_SET}, true},
		{"a", &KinesisEventFilterConfig{"x", []string{"a"}, NOT_IN_SET}, false},
		{"a", &KinesisEventFilterConfig{"x", []string{"b", "a"}, NOT_IN_SET}, false},
		{"a", &KinesisEventFilterConfig{"x", []string{"b", "c"}, NOT_IN_SET}, true},
		{"a", &KinesisEventFilterConfig{"x", []string{""}, NOT_IN_SET}, true},
		{"", &KinesisEventFilterConfig{"x", []string{"b", "c"}, NOT_IN_SET}, true},
		{"", &KinesisEventFilterConfig{"x", []string{""}, NOT_IN_SET}, false},
	}
	for _, tc := range testCases {
		assert.Equal(t, tc.result, tc.filterConfig.Match(tc.filterValue),
			"value: %v; config: %v", tc.filterValue, tc.filterConfig)
	}

}

func TestFilterFuncGenerators(t *testing.T) {
	testCases := []struct {
		filterName string
		event      map[string]string
		filters    []*KinesisEventFilterConfig
		result     bool
	}{
		{
			"isOneOf",
			map[string]string{"f1": "a", "f2": "b"},
			[]*KinesisEventFilterConfig{
				&KinesisEventFilterConfig{"f1", []string{"b", "a"}, IN_SET},
				&KinesisEventFilterConfig{"f2", []string{"c", "d"}, NOT_IN_SET},
				&KinesisEventFilterConfig{"f3", []string{""}, IN_SET},
			},
			true,
		},
		{
			"isOneOf",
			map[string]string{"f1": "a", "f2": "b"},
			[]*KinesisEventFilterConfig{
				&KinesisEventFilterConfig{"f1", []string{"b", "a"}, IN_SET},
				&KinesisEventFilterConfig{"f2", []string{"c", "d"}, NOT_IN_SET},
				&KinesisEventFilterConfig{"f3", []string{""}, IN_SET},
				&KinesisEventFilterConfig{"f3", []string{""}, NOT_IN_SET},
			},
			false,
		},
		{
			"isOneOf",
			map[string]string{"f1": "a", "f2": "b"},
			[]*KinesisEventFilterConfig{
				&KinesisEventFilterConfig{"f1", []string{"b", "a"}, IN_SET},
				&KinesisEventFilterConfig{"f2", []string{"c", "d"}, NOT_IN_SET},
				&KinesisEventFilterConfig{"f3", []string{""}, IN_SET},
				&KinesisEventFilterConfig{"f4", []string{""}, NOT_IN_SET},
			},
			false,
		},
	}
	for _, tc := range testCases {
		filterFunc := filterFuncGenerators[tc.filterName](tc.filters)
		assert.Equal(t, tc.result, filterFunc(tc.event),
			"filter: %v; map: %v; filters: %v", tc.filterName, tc.event, tc.filters)
	}
}
