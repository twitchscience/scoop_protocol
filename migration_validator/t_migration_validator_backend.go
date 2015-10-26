package migration_validator

import (
	"github.com/twitchscience/scoop_protocol/dynamo_connector"
	"github.com/twitchscience/scoop_protocol/migration_ops"
	"github.com/twitchscience/scoop_protocol/schema_storer"
)

type TestMigrationValidator struct {
	possibleMigration  schema_storer.Migration
	currentTableSchema schema_storer.TableSchema
	migrationOperator  migration_ops.TestMigrationOps
	dynamo             dynamo_connector.TestDynamoConnector
}

func BuildTestMigrationValidator() *TestMigrationValidator {
	return &TestMigrationValidator{}
}

func (m *TestMigrationValidator) Validate() error {
	return nil
}
