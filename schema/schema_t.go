package schema

import (
	"fmt"
	"testing"
)

func buildEvent(name string, version int, columnSchema ColumnSchema, migration Migration) Event {
	return Event{
		Name:            name,
		Version:         version,
		ColumnSchema:    columnSchema,
		ParentMigration: migration,
	}
}

func buildColumnDefinition(inboundName, outboundName, transformer, columnCreationOptions string) ColumnDefinition {
	return ColumnDefinition{
		InboundName:           inboundName,
		OutboundName:          outboundName,
		Transformer:           transformer,
		ColumnCreationOptions: columnCreationOptions,
	}
}

func buildColumnDefinitionBatch(num int, inboundName, outboundName, transformer, columnCreationOptions string) ColumnDefinition {
	return ColumnDefinition{
		InboundName:           fmt.Sprintf(inboundName+"%d", num),
		OutboundName:          fmt.Sprintf(outboundName+"%d", num),
		Transformer:           fmt.Sprintf(transformer+"%d", num),
		ColumnCreationOptions: fmt.Sprintf(columnCreationOptions+"%d", num),
	}
}

func buildColumns(num int, inboundName, outboundName, transformer, columnCreationOptions string) []ColumnDefinition {
	var columns []ColumnDefinition
	for i := 0; i < num; i++ {
		columns = append(columns, buildColumnDefinitionBatch(i, inboundName, outboundName, transformer, columnCreationOptions))
	}
	return columns
}

func buildColumnOperationBatch(num int, operation, inboundNameKey, outboundNameKey, inboundName, outboundName, transformer, columnCreationOptions string) ColumnOperation {
	return ColumnOperation{
		Operation:    operation,
		InboundName:  fmt.Sprintf(inboundNameKey+"%d", num),
		OutboundName: fmt.Sprintf(outboundNameKey+"%d", num),
		NewColumnDefinition: ColumnDefinition{
			InboundName:           fmt.Sprintf(inboundName+"%d", num),
			OutboundName:          fmt.Sprintf(outboundName+"%d", num),
			Transformer:           fmt.Sprintf(transformer+"%d", num),
			ColumnCreationOptions: fmt.Sprintf(columnCreationOptions+"%d", num),
		},
	}
}

func buildColumnOperations(num int, operation, inboundNameKey, outboundNameKey, inboundName, outboundName, transformer, columnCreationOptions string) []ColumnOperation {
	var columnOperations []ColumnOperation
	for i := 0; i < num; i++ {
		columnOperations = append(columnOperations, buildColumnOperationBatch(i, operation, inboundNameKey, outboundNameKey, inboundName, outboundName, transformer, columnCreationOptions))
	}
	return columnOperations
}

func buildTableOption(distKey, sortKey []string) TableOption {
	return TableOption{
		DistKey: distKey,
		SortKey: sortKey,
	}
}

func buildStringList(list ...string) []string {
	if list == nil {
		return []string{}
	}
	return list
}

func buildColumnSchema(tableOption TableOption, columns []ColumnDefinition) ColumnSchema {
	return ColumnSchema{
		TableOption: tableOption,
		Columns:     columns,
	}
}

func buildMigration(tableOperation, name string, columnOperations []ColumnOperation, tableOption TableOption) Migration {
	return Migration{
		TableOperation:   tableOperation,
		Name:             name,
		ColumnOperations: columnOperations,
		TableOption:      tableOption,
	}
}

// func TestSimpleConfig(t *testing.T) {
// 	simpleColumns := buildColumns(5, "simpleIn", "simpleOut", "int", "")
// 	simpleTableOption := buildTableOption(buildStringList(simpleColumns[0].OutboundName), buildStringList())
// 	simpleColumnSchema := buildColumnSchema(simpleTableOption, simpleColumns)
// 	simpleEvent := buildEvent("simple_Table", 1, simpleColumnSchema)

// 	err := nil

// 	if err != nil {
// 		t.Log("err was incorrect")
// 		t.Fail()
// 	}
// }

func TestAddTable(t *testing.T) {
	emptyEvent := Event{
		Name:    "empty",
		Version: 1,
	}

	simpleColumnOperations := buildColumnOperations(3, "add", "simpleColIn", "simpleColOut", "simpleColIn", "simpleColOut", "varchar", "(30)")
	simpleTableOption := buildTableOption(buildStringList(simpleColumnOperations[0].NewColumnDefinition.OutboundName), buildStringList())
	simpleMigration := buildMigration("add", "empty", simpleColumnOperations, simpleTableOption)

	simpleColumns := buildColumns(3, "simpleColIn", "simpleColOut", "varchar", "(30)")
	simpleColumnSchema := buildColumnSchema(simpleTableOption, simpleColumns)
	expectedEvent := buildEvent("empty", 2, simpleColumnSchema, simpleMigration)

	migrator := BuildMigratorBackend(simpleMigration, emptyEvent)

	newEvent, err := migrator.ApplyMigration()
	if err != nil {
		t.Log("err was incorrect, should have passed")
		t.Fail()
	}

	t.Log(expectedEvent)
	t.Log(*newEvent)
}
