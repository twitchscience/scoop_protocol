package schema

import (
	"errors"
	"reflect"
)

type MigratorBackend struct {
	possibleMigration *Migration
	currentEvent      *Event
}

func NewMigratorBackend(newMigration Migration, currentEvent Event) MigratorBackend {
	return MigratorBackend{
		possibleMigration: &newMigration,
		currentEvent:      &currentEvent,
	}
}

func (m *MigratorBackend) ApplyMigration() (*Event, error) {

	switch m.possibleMigration.TableOperation {
	case add:
		return m.addTable()
	case remove:
		return m.removeTable()
	case update:
		return m.updateTable()
	default:
		return nil, &TableError{ErrInvalidTableOperation}
	}
}

func (m *MigratorBackend) addTable() (*Event, error) {
	//checks to see if table already exists.
	if !m.currentEvent.IsEmpty() {
		return nil, &TableError{ErrAddTableOnExistingTable}
	}
	//checks for existance of atleast single distKey
	if len(m.possibleMigration.TableOption.DistKey) < 1 {
		return nil, &TableError{ErrMustContainDistKey}
	}

	//checks if distkey and sortkey are actually in the columns

	outboundCols := m.possibleMigration.CreateOutboundColsHashSet()
	for _, distKey := range m.possibleMigration.TableOption.DistKey {
		if !outboundCols.Contains(distKey) {
			return nil, &TableError{ErrDistKeyNotInCols}
		}
	}
	for _, sortKey := range m.possibleMigration.TableOption.SortKey {
		if !outboundCols.Contains(sortKey) {
			return nil, &TableError{ErrSortKeyNotInCols}
		}
	}

	if !IsValidIdentifier(m.possibleMigration.Name) {
		return nil, &TableError{ErrInvalidIdentifier}
	}

	if len(m.possibleMigration.ColumnOperations) > 300 {
		return nil, &TableError{ErrTooManyColumns}
	}

	//in the process of adding columns, validate add columns as well.
	for _, ColumnOperation := range m.possibleMigration.ColumnOperations {

		err := m.currentEvent.AddColumn(ColumnOperation)

		if err != nil {
			return nil, errors.New("Adding Table failed: " + err.Error())
		}
	}

	//add table options to new event
	m.currentEvent.TableOption = m.possibleMigration.TableOption

	//increment version number
	m.currentEvent.Version++
	m.currentEvent.ParentMigration = *m.possibleMigration
	return m.currentEvent, nil
}

func (m *MigratorBackend) removeTable() (*Event, error) {
	//checks to see if table is already empty.
	if m.currentEvent.IsEmpty() {
		return nil, &TableError{ErrRemoveTableOnNonExistingTable}
	}

	m.currentEvent.Columns = []ColumnDefinition{}
	m.currentEvent.TableOption = TableOption{}

	m.currentEvent.Version++
	m.currentEvent.ParentMigration = *m.possibleMigration
	return m.currentEvent, nil
}

func (m *MigratorBackend) updateTable() (*Event, error) {
	//checks to see if table is already empty.
	if m.currentEvent.IsEmpty() {
		return nil, &TableError{ErrUpdateTableonNonExistingTable}
	}

	if !reflect.DeepEqual(m.possibleMigration.TableOption, m.currentEvent.TableOption) {
		return nil, &TableError{ErrDifferentTableOptions}
	}

	for _, ColumnOperation := range m.possibleMigration.ColumnOperations {

		var err error

		switch ColumnOperation.Operation {
		case add:
			err = m.currentEvent.AddColumn(ColumnOperation)
		case remove:
			err = m.currentEvent.RemoveColumn(ColumnOperation)
		case update:
			err = m.currentEvent.UpdateColumn(ColumnOperation)
		default:
			err = &TableError{ErrInvalidColumnOperation} //in case column operation string is mangled.
		}

		if err != nil {
			return nil, errors.New("Updating Table failed: " + err.Error())
		}
	}

	if len(m.currentEvent.Columns) > 300 {
		return nil, &TableError{ErrTooManyColumns}
	}

	if len(m.possibleMigration.TableOption.DistKey) < 1 {
		return nil, &TableError{ErrMustContainDistKey}
	}

	//table Option check? before or after migration? Still too consider.

	m.currentEvent.Version++
	m.currentEvent.ParentMigration = *m.possibleMigration
	return m.currentEvent, nil
}
