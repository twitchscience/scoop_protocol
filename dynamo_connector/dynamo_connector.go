package dynamo_connector

import (
	"github.com/twitchscience/scoop_protocol/schema_storer"
)

type DynamoConnector interface {
	GetTables() []string
	GetSchema(tableName string) (*schema_storer.TableSchema, error)
	DoesExist() bool
	UpdateTable(schema_storer.TableSchema) error
}
