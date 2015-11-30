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
	}
	return nil, errors.New("Not a valid table operation")
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

		err := m.addColumn(ColumnOperation)

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
	//checks to see if table already exists.
	if !m.currentEvent.ColumnSchema.IsEmpty() {
		return nil, errors.New("Cannot add table that already exists")
	}

	m.currentEvent.ColumnSchema = ColumnSchema{}

	m.currentEvent.Version++
	m.currentEvent.ParentMigration = *m.possibleMigration
	return m.currentEvent, nil
}

func (m *MigratorBackend) updateTable() (*Event, error) {
	m.currentEvent.Version++
	m.currentEvent.ParentMigration = *m.possibleMigration
	return m.currentEvent, nil
}

func (m *MigratorBackend) addColumn(ColumnOperation ColumnOperation) error {
	// column operation is add
	if ColumnOperation.Operation != "add" {
		return errors.New("Column Operation is not 'add'")
	}

	//contains valid transformer
	if !transformList.Contains(ColumnOperation.NewColumnDefinition.Transformer) {
		return errors.New("Add Column operation transformer is invalid: " + ColumnOperation.NewColumnDefinition.Transformer)
	}

	//is not a column that already exists in the table // MODIFY TO ONLY CHECK OUTBOUND!!!!!!! ENCAPSULATE INTO METHOD
	for _, column := range m.currentEvent.ColumnSchema.Columns {
		if column.InboundName == ColumnOperation.InboundName && column.OutboundName == ColumnOperation.OutboundName {
			return errors.New("Column with same Inbound and Outbound name already exists in table")
		}
	}

	//adds column to table
	m.currentEvent.ColumnSchema.Columns = append(m.currentEvent.ColumnSchema.Columns, ColumnOperation.NewColumnDefinition)
	return nil
}
