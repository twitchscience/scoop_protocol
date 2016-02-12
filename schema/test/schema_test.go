package test

import (
	"fmt"
	"reflect"
	"testing"

	s "github.com/twitchscience/scoop_protocol/schema"
)

func TestAddTable(t *testing.T) {
	AddTableIsColumnSchemaEmpty(t)
	AddTableHasValidTableOptions(t)
	AddTableIsValidEventIdentifier(t)
	AddTableAddColumnOnly(t)
	AddTableAddColumnNoTransformerJunk(t)
	AddTableAddColumnVarCharLessThan64k(t)
	AddTableAddColumnOutboundNameCollision(t)
	AddTableAddColumnColumnNameLen(t)
	AddTableAddColumnColumnsMoreThan300(t)
	//test working schema here

	t.Logf("Testing working Add Table now")
	testEvent := SimEvent1Version1()
	testMigration := SimEvent1Migration1()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", *newEvent)
	}

	if !reflect.DeepEqual(*newEvent, SimEvent1Version2()) {
		t.Errorf("expected: %+v recieved %+v", SimEvent1Version2(), *newEvent)
	}
}

func AddTableIsColumnSchemaEmpty(t *testing.T) {
	t.Log("Testing if the Column Schema is already empty when adding table")
	testEvent := EventTest1()
	testMigration := Migration1OnEvent1()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as original schema had existing event")
		t.Logf("%+v", newEvent)
	}

	testEvent = EventTest1Empty()
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}
}

func AddTableHasValidTableOptions(t *testing.T) {

	t.Log("Testing to see if the migration has valid table options")
	testMigration := Migration1OnEvent1()
	testEvent := EventTest1Empty()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.TableOption.DistKey = []string{"not_passing_tableoption_distkey"}
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as migration disykey not contained in any outbound col names")
		t.Logf("%+v", newEvent)
	}

	testMigration.TableOption.DistKey = []string{}
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as migration distkey does not contain any values")
		t.Logf("%+v", newEvent)
	}
}

func AddTableIsValidEventIdentifier(t *testing.T) {
	t.Log("Testing to see if the event name is a valid identifier")
	testMigration := Migration1OnEvent1()
	testEvent := EventTest1Empty()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.Name = ""
	testEvent.EventName = ""
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as Event Name is less than 1 char")
		t.Logf("%+v", newEvent)
	}

	testMigration.Name = "abcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyz"
	testEvent.EventName = "abcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyz"
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as Event Name is more than 127 chars")
		t.Logf("%+v", newEvent)
	}
}

func AddTableAddColumnOnly(t *testing.T) {
	t.Log("Testing to see if add table migration only has add column operations")

	testMigration := Migration1OnEvent1()
	testEvent := EventTest1Empty()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.ColumnOperations[0].Operation = "update"
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as first column operation is an update")
		t.Logf("%+v", newEvent)
	}

	testMigration.ColumnOperations[0].Operation = "remove"
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as first column operation is a remove")
		t.Logf("%+v", newEvent)
	}
}

func AddTableAddColumnNoTransformerJunk(t *testing.T) {
	t.Log("Testing to see column transformer in migration is valid")

	testMigration := Migration1OnEvent1()
	testEvent := EventTest1Empty()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.ColumnOperations[0].NewColumnDefinition.Transformer = "NotRealTransformer"
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as first column operation transformer is not valid")
		t.Logf("%+v", newEvent)
	}
}

func AddTableAddColumnVarCharLessThan64k(t *testing.T) {
	t.Log("Testing to see if column value varchar is less than 64k")

	testMigration := Migration1OnEvent1()
	testEvent := EventTest1Empty()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.ColumnOperations[0].NewColumnDefinition.ColumnCreationOptions = "(65536)"
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as first column operation Column creation is higher than varchar byte max")
		t.Logf("%+v", newEvent)
	}

	testMigration.ColumnOperations[0].NewColumnDefinition.ColumnCreationOptions = "(65535)"
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}
}

func AddTableAddColumnOutboundNameCollision(t *testing.T) {
	t.Log("Testing to see if new column creates name collision with existing column")

	testMigration := Migration1OnEvent1()
	testEvent := EventTest1Empty()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.ColumnOperations[1].OutboundName = testMigration.ColumnOperations[0].OutboundName
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as second column operation outbound name collides with first")
		t.Logf("%+v", newEvent)
	}
}

func AddTableAddColumnColumnNameLen(t *testing.T) {
	t.Log("Testing to make sure column name len is less than redshift limit")
	testMigration := Migration1OnEvent1()
	testEvent := EventTest1Empty()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.ColumnOperations[0].OutboundName = "abcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyz"
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as first column outbout name is longer than 127 chars")
		t.Logf("%+v", newEvent)
	}
}

func AddTableAddColumnColumnsMoreThan300(t *testing.T) {
	t.Log("Testing if there are more than 300 columns")

	testMigration := Migration1OnEvent1()
	testEvent := EventTest1Empty()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	for i := 5; i < 310; i++ {
		testMigration.ColumnOperations = append(testMigration.ColumnOperations,
			s.ColumnOperation{
				Operation:    "add",
				InboundName:  fmt.Sprintf("test_event_1_new_inbound_col_%d", i),
				OutboundName: fmt.Sprintf("test_event_1_new_inbound_col_%d", i),
				NewColumnDefinition: s.ColumnDefinition{
					Transformer:           "varchar",
					InboundName:           fmt.Sprintf("test_event_1_new_inbound_col_%d", i),
					OutboundName:          fmt.Sprintf("test_event_1_new_inbound_col_%d", i),
					ColumnCreationOptions: "(500)",
				},
			})
	}
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as there are more than 300 columns being added")
		t.Logf("%+v", newEvent)
	}
}

func TestRemoveTable(t *testing.T) {
	RemoveTableIsColumnSchemaFull(t)
	//test working schema here
}

func RemoveTableIsColumnSchemaFull(t *testing.T) {
	t.Log("Testing if the table is actually occupied")

	testMigration := Migration2OnEvent1()
	testEvent := EventTest1()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testEvent = EventTest1Empty()
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as event is already empty")
		t.Logf("%+v", newEvent)
	}
}

func TestUpdateTable(t *testing.T) {
	UpdateTableIsColumnSchemaEmpty(t)
	UpdateTableHasValidTableOptions(t)

	UpdateTableAddColumnNoTransformerJunk(t)
	UpdateTableUpdateColumnNoTransformerJunk(t)

	UpdateTableAddColumnVarCharLessThan64k(t)
	UpdateTableUpdateColumnVarCharLessThan64k(t)

	UpdateTableAddColumnOutboundNameCollision(t)
	UpdateTableUpdateColumnOutboundNameCollision(t)

	UpdateTableAddColumnColumnNameLen(t)
	UpdateTableUpdateColumnColumnNameLen(t)

	UpdateTableAddColumnColumnsMoreThan300(t)

	UpdateTableUpdateColumnNewDistSortKey(t)
	UpdateTableUpdateColumnDoesNotExist(t)

	UpdateTableRemoveColumnDoesNotExist(t)
	UpdateTableRemoveColumnIsDistKey(t)
	//test working schema here

	t.Logf("Testing working update Table now")
	testEvent := SimEvent1Version2()
	testMigration := SimEvent1Migration2()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", *newEvent)
	}

	if !reflect.DeepEqual(*newEvent, SimEvent1Version3()) {
		t.Errorf("expected: %+v recieved %+v", SimEvent1Version3(), *newEvent)
	}

	testEvent = SimEvent1Version3()
	testMigration = SimEvent1Migration3()
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", *newEvent)
	}

	if !reflect.DeepEqual(*newEvent, SimEvent1Version4()) {
		t.Errorf("expected: %+v recieved %+v", SimEvent1Version4(), *newEvent)
	}

	testEvent = SimEvent1Version4()
	testMigration = SimEvent1Migration4()
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", *newEvent)
	}

	if !reflect.DeepEqual(*newEvent, SimEvent1Version5()) {
		t.Errorf("expected: %+v recieved %+v", SimEvent1Version5(), *newEvent)
	}

}

func UpdateTableIsColumnSchemaEmpty(t *testing.T) {
	t.Log("Testing to see if schema is already empty")

	testEvent := EventTest1()
	testMigration := Migration3OnEvent1()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testEvent = EventTest1Empty()
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as original schema had existing event")
		t.Logf("%+v", newEvent)
	}
}
func UpdateTableHasValidTableOptions(t *testing.T) {
	t.Log("Testing to make sure table options are correct")

	testEvent := EventTest1()
	testMigration := Migration3OnEvent1()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.TableOption = s.TableOption{
		DistKey: []string{"not_original_distkey"},
	}
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as Migration table options did not match event's table options")
		t.Logf("%+v", newEvent)
	}
}

func UpdateTableAddColumnNoTransformerJunk(t *testing.T) {
	t.Log("Testing to make sure added columns have no transformer junk")

	testEvent := EventTest1()
	testMigration := Migration3OnEvent1()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.ColumnOperations[0].NewColumnDefinition.Transformer = "NOT_VALID_TRANSFORMER"
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as add column migration has a bad transformer")
		t.Logf("%+v", newEvent)
	}
}
func UpdateTableUpdateColumnNoTransformerJunk(t *testing.T) {
	t.Log("Testing to make sure that updated columns have no transformer junk")

	testEvent := EventTest1()
	testMigration := Migration3OnEvent1()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.ColumnOperations[1].NewColumnDefinition.Transformer = "NOT_VALID_TRANSFORMER"
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as add column migration has a bad transformer")
		t.Logf("%+v", newEvent)
	}
}

func UpdateTableAddColumnVarCharLessThan64k(t *testing.T) {
	t.Log("Making sure that added columns don't have too large varchar settings")

	testEvent := EventTest1()
	testMigration := Migration3OnEvent1()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.ColumnOperations[0].NewColumnDefinition.ColumnCreationOptions = "(65536)"
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as add column column creation options is bigger that 65535 bytes (64k-1)")
		t.Logf("%+v", newEvent)
	}

	testMigration.ColumnOperations[0].NewColumnDefinition.ColumnCreationOptions = "(65535)"
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}
}
func UpdateTableUpdateColumnVarCharLessThan64k(t *testing.T) {
	t.Log("making sure that updated columns don't have too large carchar settings")

	testEvent := EventTest1()
	testMigration := Migration3OnEvent1()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.ColumnOperations[1].NewColumnDefinition.ColumnCreationOptions = "(65536)"
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as add column column creation options is bigger that 65535 bytes (64k-1)")
		t.Logf("%+v", newEvent)
	}

	testMigration.ColumnOperations[1].NewColumnDefinition.ColumnCreationOptions = "(65535)"
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}
}

func UpdateTableAddColumnOutboundNameCollision(t *testing.T) {
	t.Log("Making sure that added columns don't already exist")

	testEvent := EventTest1()
	testMigration := Migration3OnEvent1()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.ColumnOperations[0].OutboundName = testEvent.Columns[0].OutboundName
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as add column operation outbound name collides with first existing column")
		t.Logf("%+v", newEvent)
	}
}
func UpdateTableUpdateColumnOutboundNameCollision(t *testing.T) {
	t.Log("Making sure that updated columns don't already exist")

	testEvent := EventTest1()
	testMigration := Migration3OnEvent1()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.ColumnOperations[1].NewColumnDefinition.OutboundName = testEvent.Columns[0].OutboundName
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as add column operation outbound name collides with first existing column")
		t.Logf("%+v", newEvent)
	}
}

func UpdateTableAddColumnColumnNameLen(t *testing.T) {
	t.Log("Making sure that added column name len is under 127 chars")

	testEvent := EventTest1()
	testMigration := Migration3OnEvent1()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.ColumnOperations[0].OutboundName = "abcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyz"
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as add column operation outbound name collides with first existing column")
		t.Logf("%+v", newEvent)
	}
}
func UpdateTableUpdateColumnColumnNameLen(t *testing.T) {
	t.Log("Making sure that updated column name len is under 127 chars")

	testEvent := EventTest1()
	testMigration := Migration3OnEvent1()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.ColumnOperations[1].NewColumnDefinition.OutboundName = "abcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyzabcdefghijklmnopqrstuvwzyz"
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as add column operation outbound name collides with first existing column")
		t.Logf("%+v", newEvent)
	}
}

func UpdateTableAddColumnColumnsMoreThan300(t *testing.T) {
	t.Log("Making sure total column number is not > 300")

	testEvent := EventTest1()
	testMigration := Migration3OnEvent1()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	for i := 5; i < 310; i++ {
		testMigration.ColumnOperations = append(testMigration.ColumnOperations,
			s.ColumnOperation{
				Operation:    "add",
				InboundName:  fmt.Sprintf("test_event_1_new_inbound_col_%d", i),
				OutboundName: fmt.Sprintf("test_event_1_new_inbound_col_%d", i),
				NewColumnDefinition: s.ColumnDefinition{
					Transformer:           "varchar",
					InboundName:           fmt.Sprintf("test_event_1_new_inbound_col_%d", i),
					OutboundName:          fmt.Sprintf("test_event_1_new_inbound_col_%d", i),
					ColumnCreationOptions: "(500)",
				},
			})
	}
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as add column operation outbound name collides with first existing column")
		t.Logf("%+v", newEvent)
	}
}

func UpdateTableUpdateColumnNewDistSortKey(t *testing.T) {
	t.Log("Make sure that updated columns are not the distkey/sortkey")

	testEvent := EventTest1()
	testMigration := Migration3OnEvent1()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.ColumnOperations[1].OutboundName = "test_event_1_new_inbound_col_1"
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as updated column is distkey")
		t.Logf("%+v", newEvent)
	}
}
func UpdateTableUpdateColumnDoesNotExist(t *testing.T) {
	t.Log("Updated column does not exist in schema")

	testEvent := EventTest1()
	testMigration := Migration3OnEvent1()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.ColumnOperations[1].OutboundName = "column_that_does_not_exist"
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as column trying to be updated does not exist")
		t.Logf("%+v", newEvent)
	}
}

func UpdateTableRemoveColumnDoesNotExist(t *testing.T) {
	t.Log("Removed column already doesn't exist")

	testEvent := EventTest1()
	testMigration := Migration3OnEvent1()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.ColumnOperations[2].OutboundName = "column_that_does_not_exist"
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as column trying to be updated does not exist")
		t.Logf("%+v", newEvent)
	}
}
func UpdateTableRemoveColumnIsDistKey(t *testing.T) {
	t.Log("Cannot remove column because it's a distkey")

	testEvent := EventTest1()
	testMigration := Migration3OnEvent1()
	migrator := s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Errorf("Expected passed migration, instead errored: %s", err)
	} else {
		t.Logf("Passed migration successfully: %+v", newEvent)
	}

	testMigration.ColumnOperations[2].OutboundName = "test_event_1_new_inbound_col_1"
	migrator = s.BuildMigratorBackend(testMigration, testEvent)
	newEvent, err = migrator.ApplyMigration()
	if err == nil {
		t.Error("Test should have failed as column trying to be updated does not exist")
		t.Logf("%+v", newEvent)
	}
}
