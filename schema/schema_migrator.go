package schema

import (
	"errors"
	"fmt"
	"reflect"
)

type MigratorBackend struct {
	possibleMigration *Migration
	currentEvent      *Event
}

func BuildMigratorBackend(newMigration Migration, currentEvent Event) MigratorBackend {
	return MigratorBackend{
		possibleMigration: &newMigration,
		currentEvent:      &currentEvent,
	}
}

func (m *MigratorBackend) ApplyMigration() (*Event, error) {

	switch m.possibleMigration.TableOperation {
	case "add":
		return m.addTable()
	case "remove":
		return m.removeTable()
	case "update":
		return m.updateTable()
	default:
		return nil, errors.New("Not a valid table operation")
	}
}

func (m *MigratorBackend) addTable() (*Event, error) {
	//checks to see if table already exists.
	if !m.currentEvent.IsEmpty() {
		return nil, errors.New("Cannot add table that already exists")
	}
	//checks for existance of atleast single distKey
	if len(m.possibleMigration.TableOption.DistKey) < 1 {
		return nil, errors.New("Tableoption must contain at least 1 distkey")
	}

	//checks if distkey and sortkey are actually in the columns

	outboundCols := m.possibleMigration.CreateOutboundColsHashSet()
	for _, distKey := range m.possibleMigration.TableOption.DistKey {
		if !outboundCols.Contains(distKey) {
			return nil, errors.New("TableOption DistKeys does not contain a outbound col name")
		}
	}
	for _, sortKey := range m.possibleMigration.TableOption.SortKey {
		if !outboundCols.Contains(sortKey) {
			return nil, errors.New("TableOption SortKeys does not contain a outbound col name")
		}
	}

	if !IsValidIdentifier(m.possibleMigration.Name) {
		return nil, errors.New(fmt.Sprintf("%s is not a valid identifier for a table", m.possibleMigration.Name))
	}

	if len(m.possibleMigration.ColumnOperations) > 300 {
		return nil, errors.New("tables with more than 300 columns slow redshift immensely")
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
		return nil, errors.New("Cannot remove table that is already empty")
	}

	m.currentEvent.Columns = nil

	m.currentEvent.Version++
	m.currentEvent.ParentMigration = *m.possibleMigration
	return m.currentEvent, nil
}

func (m *MigratorBackend) updateTable() (*Event, error) {
	//checks to see if table is already empty.
	if m.currentEvent.IsEmpty() {
		return nil, errors.New("Cannot update table that is already empty, add table first")
	}

	if !reflect.DeepEqual(m.possibleMigration.TableOption, m.currentEvent.TableOption) {
		return nil, errors.New("Cannot change table options on update")
	}

	for _, ColumnOperation := range m.possibleMigration.ColumnOperations {

		var err error

		switch ColumnOperation.Operation {
		case "add":
			err = m.currentEvent.AddColumn(ColumnOperation)
		case "remove":
			err = m.currentEvent.RemoveColumn(ColumnOperation)
		case "update":
			err = m.currentEvent.UpdateColumn(ColumnOperation)
		default:
			err = errors.New("Not a valid Column Operation") //in case column operation string is mangled.
		}

		if err != nil {
			return nil, errors.New("Updating Table failed: " + err.Error())
		}
	}

	if len(m.currentEvent.Columns) > 300 {
		return nil, errors.New("tables with more than 300 columns slow redshift immensely")
	}

	if len(m.possibleMigration.TableOption.DistKey) < 1 {
		return nil, errors.New("Tableoption must contain at least 1 distkey")
	}

	//table Option check? before or after migration? Still too consider.

	m.currentEvent.Version++
	m.currentEvent.ParentMigration = *m.possibleMigration
	return m.currentEvent, nil
}
