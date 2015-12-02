package schema

import "errors"

type Migrator interface {
	Validate() (*Event, error)
}

type Event struct {
	Name            string
	Version         int
	ColumnSchema    ColumnSchema
	ParentMigration Migration
}

type ColumnSchema struct {
	TableOption TableOption
	Columns     []ColumnDefinition
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

func (s *ColumnSchema) IsEmpty() bool {
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
	for _, column := range e.ColumnSchema.Columns {
		if column.OutboundName == ColumnOperation.OutboundName {
			return errors.New("Column with same Outbound name already exists in table")
		}
	}
	e.ColumnSchema.Columns = append(e.ColumnSchema.Columns, ColumnOperation.NewColumnDefinition)
	return nil
}

func (e *Event) RemoveColumn(ColumnOperation ColumnOperation) error {
	//finds index in list which corresponds to column that needs to be removed
	i := -1
	for index, column := range e.ColumnSchema.Columns {
		if column.OutboundName == ColumnOperation.OutboundName {
			i = index
			break
		}
	}

	//checks to see if column event existed to begin with
	if i == -1 {
		return errors.New("Column cannot be removed if it does not exist")
	}

	e.ColumnSchema.Columns = append(e.ColumnSchema.Columns[:i], e.ColumnSchema.Columns[i+1:]...)
	return nil
}

func (e *Event) UpdateColumn(ColumnOperation ColumnOperation) error {
	//finds index in list which corresponds to column that needs to be updated
	i := -1
	for index, column := range e.ColumnSchema.Columns {
		if column.OutboundName == ColumnOperation.OutboundName {
			i = index
			break
		}
	}

	//checks to see if column event existed to begin with
	if i == -1 {
		return errors.New("Column cannot be removed if it does not exist")
	}

	return nil
}

type HashSet map[string]HashMember

type HashMember struct{}

func (hs HashSet) Contains(val string) bool {
	_, ok := hs[val]
	return ok
}

//hash member struct
var transformList = HashSet{
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
