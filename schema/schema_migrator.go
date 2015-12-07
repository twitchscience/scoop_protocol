package schema

import "errors"

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
	if !m.currentEvent.ColumnSchema.IsEmpty() {
		return nil, errors.New("Cannot add table that already exists")
	}
	//checks for existance of atleast single distKey
	if len(m.possibleMigration.TableOption.DistKey) < 1 {
		return nil, errors.New("Tableoption must contain at least 1 distkey")
	}

	//in the process of adding columns, validate add columns as well.
	for _, ColumnOperation := range m.possibleMigration.ColumnOperations {

		err := m.currentEvent.AddColumn(ColumnOperation)

		if err != nil {
			return nil, errors.New("Adding Table failed: " + err.Error())
		}
	}

	//add table options to new event
	m.currentEvent.ColumnSchema.TableOption = m.possibleMigration.TableOption

	//increment version number
	m.currentEvent.Version++
	m.currentEvent.ParentMigration = *m.possibleMigration
	return m.currentEvent, nil
}

func (m *MigratorBackend) removeTable() (*Event, error) {
	//checks to see if table is already empty.
	if m.currentEvent.ColumnSchema.IsEmpty() {
		return nil, errors.New("Cannot remove table that is already empty")
	}

	m.currentEvent.ColumnSchema = ColumnSchema{}

	m.currentEvent.Version++
	m.currentEvent.ParentMigration = *m.possibleMigration
	return m.currentEvent, nil
}

func (m *MigratorBackend) updateTable() (*Event, error) {
	//checks to see if table is already empty.
	if m.currentEvent.ColumnSchema.IsEmpty() {
		return nil, errors.New("Cannot update table that is already empty, add table first")
	}

	//table Option check? before or after migration? Still too consider.

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

	if len(m.possibleMigration.TableOption.DistKey) < 1 {
		return nil, errors.New("Tableoption must contain at least 1 distkey")
	}

	m.currentEvent.Version++
	m.currentEvent.ParentMigration = *m.possibleMigration
	return m.currentEvent, nil
}
