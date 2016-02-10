package schema

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type Migrator interface {
	Validate() (*Event, error)
}

type Event struct {
	EventName       string
	Version         int
	Columns         []ColumnDefinition
	ParentMigration Migration
	TableOption     TableOption
}

type ColumnDefinition struct {
	InboundName           string
	OutboundName          string
	Transformer           string
	ColumnCreationOptions string
}

type Migration struct {
	TableOperation   string
	Name             string
	ColumnOperations []ColumnOperation
	TableOption      TableOption
}

type ColumnOperation struct {
	Operation           string
	InboundName         string
	OutboundName        string
	NewColumnDefinition ColumnDefinition
}

type TableOption struct {
	DistKey []string
	SortKey []string
}

func MakeNewEvent(eventName string, eventVersion int) Event {
	return Event{
		EventName: eventName,
		Version:   eventVersion,
	}
}

func (s *Event) IsEmpty() bool {
	if len(s.Columns) == 0 {
		return true
	}
	return false
}

func (to *TableOption) IsEmpty() bool {
	if len(to.DistKey) == 0 && len(to.SortKey) == 0 {
		return true
	}
	return false
}

func (e *Event) AddColumn(ColumnOperation ColumnOperation) error {

	// column operation is add
	if ColumnOperation.Operation != "add" {
		return errors.New("Column Operation is not 'add'")
	}

	//contains valid transformer
	if !TransformList.Contains(ColumnOperation.NewColumnDefinition.Transformer) {
		return errors.New("Add Column operation transformer is invalid: " + ColumnOperation.NewColumnDefinition.Transformer)
	}

	//checks if varchar byte limit is exceeded
	if ColumnOperation.NewColumnDefinition.Transformer == "varchar" {
		temp := ColumnOperation.NewColumnDefinition.ColumnCreationOptions
		varcharLen, err := strconv.Atoi(string(temp[1 : len(temp)-1]))
		if err != nil {
			return errors.New("Column creation options does not contain a number")
		}
		if varcharLen > 65535 {
			return errors.New("max size of varchar is 65535 (64k-1)")
		}
	}

	if !IsValidIdentifier(ColumnOperation.OutboundName) {
		return errors.New(fmt.Sprintf("%s is not a valid identifier for a column outbound name", ColumnOperation.OutboundName))
	}

	// Check for column name collision, and add column, return error if there is one
	for _, column := range e.Columns {
		if column.OutboundName == ColumnOperation.OutboundName {
			return errors.New("Column with same Outbound name already exists in table")
		}
	}
	e.Columns = append(e.Columns, ColumnOperation.NewColumnDefinition)
	return nil
}

func (e *Event) RemoveColumn(ColumnOperation ColumnOperation) error {
	//column operation is remove
	if ColumnOperation.Operation != "remove" {
		return errors.New("Column Operation is not 'remove'")
	}

	//finds index in list which corresponds to column that needs to be removed
	i := -1
	for index, column := range e.Columns {
		if column.OutboundName == ColumnOperation.OutboundName {
			i = index
			break
		}
	}

	//checks to see if column event existed to begin with
	if i == -1 {
		return errors.New("Column cannot be removed if it does not exist")
	}

	for _, key := range e.TableOption.DistKey {
		if key == ColumnOperation.OutboundName {
			return errors.New("Cannot remove columns that are the DistKey")
		}
	}

	e.Columns = append(e.Columns[:i], e.Columns[i+1:]...)
	return nil
}

func (e *Event) UpdateColumn(ColumnOperation ColumnOperation) error {
	//column operation is update
	if ColumnOperation.Operation != "update" {
		return errors.New("Column Operation is not 'update'")
	}

	//Check if transformer is valid
	if !TransformList.Contains(ColumnOperation.NewColumnDefinition.Transformer) {
		return errors.New("Update Column operation transformer is invalid: " + ColumnOperation.NewColumnDefinition.Transformer)
	}

	//checks if varchar byte limit is exceeded
	if ColumnOperation.NewColumnDefinition.Transformer == "varchar" {
		temp := ColumnOperation.NewColumnDefinition.ColumnCreationOptions
		varcharLen, err := strconv.Atoi(string(temp[1 : len(temp)-1]))
		if err != nil {
			return errors.New("Column creation options does not contain a number")
		}
		if varcharLen > 65535 {
			return errors.New("max size of varchar is 65535 (64k-1)")
		}
	}

	if !IsValidIdentifier(ColumnOperation.NewColumnDefinition.OutboundName) {
		return errors.New(fmt.Sprintf("%s is not a valid identifier for a column outbound name", ColumnOperation.OutboundName))
	}

	for _, key := range e.TableOption.DistKey {
		if key == ColumnOperation.OutboundName {
			return errors.New("Cannot update columns that are the DistKey")
		}
	}

	//finds index in list which corresponds to column that needs to be updated
	i := -1
	outboundHashSet := make(HashSet)

	for index, column := range e.Columns {
		if column.OutboundName == ColumnOperation.OutboundName {
			i = index
		} else {
			outboundHashSet[column.OutboundName] = HashMember{}
		}
	}

	//checks to see if column event existed to begin with
	if i == -1 {
		return errors.New("Column cannot be updated if it does not exist")
	}

	//outbound name change, check for collision for column rename reasons.
	if outboundHashSet.Contains(ColumnOperation.NewColumnDefinition.OutboundName) {
		return errors.New("New outbound name in update column operation already exists in table")
	}

	e.Columns[i] = ColumnOperation.NewColumnDefinition
	return nil
}

type HashSet map[string]HashMember

type HashMember struct{}

func (hs HashSet) Contains(val string) bool {
	_, ok := hs[val]
	return ok
}

func (hs HashSet) Delete(val string) {
	delete(hs, val)
}

//hash member struct
var TransformList = HashSet{
	"bigint":             HashMember{},
	"float":              HashMember{},
	"varchar":            HashMember{},
	"ipAsnInteger":       HashMember{},
	"int":                HashMember{},
	"bool":               HashMember{},
	"ipCity":             HashMember{},
	"ipCountry":          HashMember{},
	"ipRegion":           HashMember{},
	"ipAsn":              HashMember{},
	"stringToIntegerMD5": HashMember{},
	"f@timestamp@unix":   HashMember{},
}

func IsStandardIdentifier(identifier string) bool {
	validChar := func(r rune) bool {
		return !((r > 'A' && r < 'Z') || (r > 'a' && r < 'z') || (r > '0' && r < '9') || r == '$' || r == '_' || r == '-')
	}

	validFirstChar := func(r rune) bool {
		return !((r > 'A' && r < 'Z') || (r > 'a' && r < 'z') || r == '_' || r == '-')
	}

	if len(identifier) > 127 || len(identifier) < 1 {
		return false
	}

	if strings.IndexFunc(identifier, validChar) != -1 {
		return false
	}

	if strings.IndexFunc(string(identifier[0]), validFirstChar) != -1 {
		return false
	}

	return true
}

func IsValidIdentifier(identifier string) bool {
	if len(identifier) > 127 || len(identifier) < 1 {
		return false
	}

	if bytes.Index([]byte(identifier), []byte("\x00")) != -1 {
		return false
	}

	return true
}

func (m *Migration) CreateOutboundColsHashSet() HashSet {
	outboundColNames := make(HashSet)
	for _, operation := range m.ColumnOperations {
		outboundColNames[operation.OutboundName] = HashMember{}
	}
	return outboundColNames
}
