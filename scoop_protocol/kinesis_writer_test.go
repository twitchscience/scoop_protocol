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
	assert.NoError(t, config.Validate(nil), "config could not be validated")
}

func TestRedshiftStreamAndCompressValidation(t *testing.T) {
	config := KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	config.Compress = true

	// firehose->redshift streaming cannot be used with compress mode
	assert.Error(t, config.Validate(nil), "redshift streaming and compress cannot both be on")
}

func TestRedshiftStreamAndStreamValidation(t *testing.T) {
	config := KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	config.StreamType = "stream"

	// firehose->redshift streaming can only be used with firehose
	assert.Error(t, config.Validate(nil), "redshift streaming can only be used with firehose")
}

func TestFieldRenaming(t *testing.T) {
	config := KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	config.Events["minute-watched"].FieldRenames = map[string]string{
		"country": "renamed_country",
	}

	require.NoError(t, config.Validate(nil), "config could not be validated")
	assert.Equal(t, map[string]string{"country": "renamed_country", "device_id": "device_id"},
		config.Events["minute-watched"].FullFieldMap)
}

func TestRegionValidation(t *testing.T) {
	config := KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	config.StreamRegion = "us-west-2"
	assert.NoError(t, config.Validate(nil), "valid region didn't work")
	config.StreamRegion = "us-west-3"
	assert.Error(t, config.Validate(nil), "invalid region worked")
}

func TestFilterFuncValidation(t *testing.T) {
	config := KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	commonFilters := map[string]EventFilterFunc{"valid_filter": NoopFilter}
	config.Events["pageview"].Filter = "invalid_filter"
	assert.Error(t, config.Validate(commonFilters), "invalid filter func worked")
	config.Events["pageview"].Filter = "valid_filter"
	assert.NoError(t, config.Validate(commonFilters), "valid filter func did not work")
}

func TestNoFilterFuncParametersValidation(t *testing.T) {
	config := KinesisWriterConfig{}
	_ = json.Unmarshal(FirehoseRedshiftStreamTestConfig, &config)
	config.Events["pageview"].FilterParameters = nil
	assert.Error(t, config.Validate(nil), "Providing filter with no parameters worked")
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
			{tc.fieldValue, tc.values, tc.op},
		}
		assert.Error(t, config.Validate(nil), tc.msg)
	}
}

func TestTestableKinesisEventFilter(t *testing.T) {
	testCases := []struct {
		name              string
		config            []*KinesisEventFilterConfig
		matchingEvents    []map[string]string
		nonMatchingEvents []map[string]string
	}{
		{
			name: "inSet",
			config: []*KinesisEventFilterConfig{{
				Field:    "propa",
				Values:   []string{"a", "b"},
				Operator: IN_SET,
			}, {
				Field:    "propb",
				Values:   []string{"b", ""},
				Operator: IN_SET,
			}},
			matchingEvents: []map[string]string{
				{"propa": "a"},
				{"propa": "a", "propb": "b"},
				{"propa": "b", "propb": ""},
			},
			nonMatchingEvents: []map[string]string{
				{},
				{"propb": "b"},
				{"propa": "b", "propb": "c"},
				{"propa": "c", "propb": "b"},
			},
		},
		{
			name: "notInSet",
			config: []*KinesisEventFilterConfig{{
				Field:    "propa",
				Values:   []string{"a", "b"},
				Operator: NOT_IN_SET,
			}, {
				Field:    "propb",
				Values:   []string{"b", "c", ""},
				Operator: NOT_IN_SET,
			}},
			matchingEvents: []map[string]string{
				{"propb": "a"},
				{"propa": "c", "propb": "a"},
			},
			nonMatchingEvents: []map[string]string{
				{},
				{"propa": "a"},
				{"propb": "b"},
				{"propa": "a", "propb": "a"},
				{"propa": "c", "propb": "b"},
				{"propa": "a", "propb": "b"},
			},
		},
		{
			name: "mixed",
			config: []*KinesisEventFilterConfig{{
				Field:    "propa",
				Values:   []string{"a", "b"},
				Operator: IN_SET,
			}, {
				Field:    "propb",
				Values:   []string{"b", "c"},
				Operator: NOT_IN_SET,
			}},
			matchingEvents: []map[string]string{
				{"propa": "a"},
				{"propa": "a", "propb": "a"},
			},
			nonMatchingEvents: []map[string]string{
				{},
				{"propa": "c"},
				{"propb": "a"},
				{"propa": "a", "propb": "b"},
				{"propa": "c", "propb": "a"},
			},
		},
	}
	for _, tc := range testCases {
		tkef := TestableKinesisEventFilter{
			Config:            tc.config,
			MatchingEvents:    tc.matchingEvents,
			NonMatchingEvents: tc.nonMatchingEvents,
		}
		_, err := tkef.Build()
		assert.NoError(t, err, "%s ok", tc.name)
		for _, me := range tc.matchingEvents {
			tkef = TestableKinesisEventFilter{
				Config:            tc.config,
				NonMatchingEvents: []map[string]string{me},
			}
			_, err = tkef.Build()
			assert.Error(t, err, "%s matching", tc.name)
		}
		for _, nme := range tc.nonMatchingEvents {
			tkef = TestableKinesisEventFilter{
				Config:         tc.config,
				MatchingEvents: []map[string]string{nme},
			}
			_, err = tkef.Build()
			assert.Error(t, err, "%s not matching", tc.name)
		}
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
				{"f1", []string{"b", "a"}, IN_SET},
				{"f2", []string{"c", "d"}, NOT_IN_SET},
				{"f3", []string{""}, IN_SET},
			},
			true,
		},
		{
			"isOneOf",
			map[string]string{"f1": "a", "f2": "b"},
			[]*KinesisEventFilterConfig{
				{"f1", []string{"b", "a"}, IN_SET},
				{"f2", []string{"c", "d"}, NOT_IN_SET},
				{"f3", []string{""}, IN_SET},
				{"f3", []string{""}, NOT_IN_SET},
			},
			false,
		},
		{
			"isOneOf",
			map[string]string{"f1": "a", "f2": "b"},
			[]*KinesisEventFilterConfig{
				{"f1", []string{"b", "a"}, IN_SET},
				{"f2", []string{"c", "d"}, NOT_IN_SET},
				{"f3", []string{""}, IN_SET},
				{"f4", []string{""}, NOT_IN_SET},
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
