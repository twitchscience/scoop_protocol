package dynamo_connector

import "github.com/twitchscience/scoop_protocol/schema_storer"

type TestDynamoConnector struct {
}

func BuildTestDynamoConnector() *TestDynamoConnector {
	return &TestDynamoConnector{}
}

func (c *TestDynamoConnector) GetTables() []string {
	temp := []string{"table1", "table2", "table3", "table4", "table5"}
	return temp
}

func (c *TestDynamoConnector) GetSchema(tableName string) (*schema_storer.TableSchema, error) {
	return &schema_storer.TableSchema{}, nil
}

func (c *TestDynamoConnector) DoesExist() bool {
	return false
}

func (c *TestDynamoConnector) UpdateTable(schema_storer.TableSchema) error {
	return nil
}
