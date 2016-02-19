package schema

import (
	"strconv"
	"strings"
)

const add string = "add"
const update string = "update"
const remove string = "remove"
const maxVarcharLen int = 65535

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

func NewEvent(eventName string, eventVersion int) Event {
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
	if ColumnOperation.Operation != add {
		return &ColumnError{ErrColumnOpNotAdd}
	}

	//contains valid transformer
	if !TransformList.Contains(ColumnOperation.NewColumnDefinition.Transformer) {
		return &ColumnError{ErrInvalidTransformer}
	}

	//checks if varchar byte limit is exceeded
	if ColumnOperation.NewColumnDefinition.Transformer == "varchar" {
		temp := ColumnOperation.NewColumnDefinition.ColumnCreationOptions
		varcharLen, err := strconv.Atoi(string(temp[1 : len(temp)-1]))
		if err != nil {
			return &ColumnError{ErrVarCharNotInt}
		}
		if varcharLen > maxVarcharLen {
			return &ColumnError{ErrVarCharBytesMax}
		}
	}

	//checks if outbound column name is valid identifier
	if !IsValidIdentifier(ColumnOperation.OutboundName) {
		return &ColumnError{ErrInvalidIdentifier}
	}

	// Check for column name collision, and add column, return error if there is one
	for _, column := range e.Columns {
		if column.OutboundName == ColumnOperation.OutboundName {
			return &ColumnError{ErrOutboundNameCollision}
		}
	}
	e.Columns = append(e.Columns, ColumnOperation.NewColumnDefinition)
	return nil
}

func (e *Event) RemoveColumn(ColumnOperation ColumnOperation) error {
	//column operation is remove
	if ColumnOperation.Operation != remove {
		return &ColumnError{ErrColumnOpNotRemove}
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
		return &ColumnError{ErrRemoveColNonExistingCol}
	}

	//cannot remove columns that are distkey
	for _, key := range e.TableOption.DistKey {
		if key == ColumnOperation.OutboundName {
			return &ColumnError{ErrRemoveColisDistKey}
		}
	}

	e.Columns = append(e.Columns[:i], e.Columns[i+1:]...)
	return nil
}

func (e *Event) UpdateColumn(ColumnOperation ColumnOperation) error {
	//column operation is update
	if ColumnOperation.Operation != update {
		return &ColumnError{ErrColumnOpNotUpdate}
	}

	//Check if transformer is valid
	if !TransformList.Contains(ColumnOperation.NewColumnDefinition.Transformer) {
		return &ColumnError{ErrInvalidTransformer}
	}

	//checks if varchar byte limit is exceeded
	if ColumnOperation.NewColumnDefinition.Transformer == "varchar" {
		temp := ColumnOperation.NewColumnDefinition.ColumnCreationOptions
		varcharLen, err := strconv.Atoi(string(temp[1 : len(temp)-1]))
		if err != nil {
			return &ColumnError{ErrVarCharNotInt}
		}
		if varcharLen > maxVarcharLen {
			return &ColumnError{ErrVarCharBytesMax}
		}
	}

	//checks if outbound col name is valid identifier
	if !IsValidIdentifier(ColumnOperation.NewColumnDefinition.OutboundName) {
		return &ColumnError{ErrInvalidIdentifier}
	}

	//cannot update columns that are distkey
	for _, key := range e.TableOption.DistKey {
		if key == ColumnOperation.OutboundName {
			return &ColumnError{ErrUpdateColisDistKey}
		}
	}

	//finds index in list which corresponds to column that needs to be updated
	i := -1

	for index, column := range e.Columns {
		if column.OutboundName == ColumnOperation.OutboundName {
			i = index
		} else {
			if column.OutboundName == ColumnOperation.NewColumnDefinition.OutboundName {
				return &ColumnError{ErrOutboundNameCollision}
			}
		}
	}

	//checks to see if column event existed to begin with
	if i == -1 {
		return &ColumnError{ErrUpdateColNonExistingCol}
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

func IsValidIdentifier(identifier string) bool {
	if len(identifier) > 127 || len(identifier) < 1 {
		return false
	}

	if strings.ContainsAny(identifier, "\x00") {
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
