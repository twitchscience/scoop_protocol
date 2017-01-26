package scoop_protocol

import (
	"bytes"
	"fmt"
	"testing"
)

func TestConfig(t *testing.T) {
	s := GetScoopSigner()
	testConfig := Config{
		"test",
		[]ColumnDefinition{
			{"", "test1", "int", "options?"},
			{"", "test2", "int", "options?"},
			{"", "test3", "int", "options?"},
		},
		0,
	}
	b, erro := s.SignJsonBody(testConfig)
	if erro != nil {
		t.Log(erro)
		t.Fail()
	}
	c, err := s.GetConfig(bytes.NewReader(b))
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	if fmt.Sprintf("%v", *c) != fmt.Sprintf("%v", testConfig) {
		t.Logf("Expected %v got %v\n", testConfig, c)
		t.Fail()
	}
}

func TestEmptyConfig(t *testing.T) {
	s := GetScoopSigner()
	testConfig := Config{
		"test",
		[]ColumnDefinition{
			{"", "test1", "int", "options?"},
			{"", "test2", "int", "options?"},
			{},
		},
		0,
	}
	b, erro := s.SignJsonBody(testConfig)
	if erro != nil {
		t.Log(erro)
		t.Fail()
	}
	c, err := s.GetConfig(bytes.NewReader(b))
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	if fmt.Sprintf("%v", *c) != fmt.Sprintf("%v", testConfig) {
		t.Logf("Expected %v got %v\n", testConfig, c)
		t.Fail()
	}
}

func TestRowCopyRequest(t *testing.T) {
	s := GetScoopSigner()
	testConfig := RowCopyRequest{
		"key",
		"table",
		0,
	}
	b, erro := s.SignJsonBody(testConfig)
	if erro != nil {
		t.Log(erro)
		t.Fail()
	}
	c, err := s.GetRowCopyRequest(bytes.NewReader(b))
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	if fmt.Sprintf("%v", *c) != fmt.Sprintf("%v", testConfig) {
		t.Logf("Expected %v got %v\n", testConfig, c)
		t.Fail()
	}
}
