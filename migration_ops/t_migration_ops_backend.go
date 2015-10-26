package migration_ops

import "github.com/twitchscience/scoop_protocol/schema_storer"

type TestMigrationOps struct {
	newMigration schema_storer.Migration
}

func BuidTestMigrationOps(newMigration schema_storer.Migration) *TestMigrationOps {
	return &TestMigrationOps{newMigration: newMigration}
}

func (m *TestMigrationOps) AddTable() (*schema_storer.TableSchema, error) {
	return &schema_storer.TableSchema{}, nil
}

func (m *TestMigrationOps) RemoveTable() (*schema_storer.TableSchema, error) {
	return &schema_storer.TableSchema{}, nil
}

func (m *TestMigrationOps) UpdateTable() (*schema_storer.TableSchema, error) {
	return &schema_storer.TableSchema{}, nil
}

func (m *TestMigrationOps) AddColumn() (*schema_storer.TableSchema, error) {
	return &schema_storer.TableSchema{}, nil
}

func (m *TestMigrationOps) RemoveColumn() (*schema_storer.TableSchema, error) {
	return &schema_storer.TableSchema{}, nil
}

func (m *TestMigrationOps) UpdateColumn() (*schema_storer.TableSchema, error) {
	return &schema_storer.TableSchema{}, nil
}
