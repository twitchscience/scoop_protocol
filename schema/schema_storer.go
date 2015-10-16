package schema

import "errors"

type TableSchema struct {
	TableName     string
	Version       int
	ColumnSchema  Schema
	CurrMigration Migration
}

type Schema struct {
	Option  TableOption
	Columns []ColumnDefinition
}

type ColumnDefinition struct {
	InboundName           string
	OutboundName          string
	Transformer           string
	ColumnCreationOptions string
}

type Migration struct {
	TableOp   string
	TableName string
	ColumnOps []ColumnOperation
	Option    TableOption
}

type ColumnOperation struct {
	Operation             string
	InboundName           string
	OutboundName          string
	Transformer           string
	ColumnCreationOptions string
}

type TableOption struct {
	DistKey []string
	SortKey []string
}

func (m *Migration) ValidateMigration(tableSchema *TableSchema) error {
	switch {
	case m.TableOp == "add":
		return m.validateMigrationAddTable(tableSchema)
	case m.TableOp == "remove":
		return m.validateMigrationRemoveTable(tableSchema)
	case m.TableOp == "update":
		return m.validateMigrationUpdateTable(tableSchema)
	}
	return errors.New("Not a valid table operation")
}

func (m *Migration) validateMigrationAddTable(tableSchema *TableSchema) error {
	if tableSchema != nil {
		if tableSchema.ColumnSchema.Columns == nil {
			return nil
		} else {
			return errors.New("Table already exists.  Cannot add table that already exists.")
		}
	}

	return nil
}

func (m *Migration) validateMigrationRemoveTable(tableSchema *TableSchema) error {
	return nil
}

func (m *Migration) validateMigrationUpdateTable(tableSchema *TableSchema) error {
	return nil
}
