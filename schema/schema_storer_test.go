package schema

import "testing"

func buildSimpleMigration() (migration *Migration) {
	return &Migration{
		TableOp:   "update",
		TableName: "simple",
		Option:    TableOption{},
		ColumnOps: []ColumnOperation{
			ColumnOperation{
				Operation:             "add",
				InboundName:           "simpleInbound2",
				OutboundName:          "simpleOutbound2",
				Transformer:           "varchar",
				ColumnCreationOptions: "(16)",
			},
		},
	}
}

func buildSimpleTableSchema() (tableSchema *TableSchema) {
	return &TableSchema{
		TableName: "simple",
		Version:   1,
		ColumnSchema: Schema{
			Option: TableOption{},
			Columns: []ColumnDefinition{
				ColumnDefinition{
					InboundName:           "simpleInbound1",
					OutboundName:          "simpleOutbound1",
					Transformer:           "varchar",
					ColumnCreationOptions: "(16)",
				},
			},
		},
		CurrMigration: Migration{},
	}
}

func TestSimpleConfig(t *testing.T) {
	migration := buildSimpleMigration()
	tableSchema := buildSimpleTableSchema()
	err := migration.ValidateMigration(tableSchema)
	if err != nil {
		t.Log("err was incorrect")
		t.Fail()
	}
}
