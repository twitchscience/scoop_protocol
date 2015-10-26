package migration_validator

type MigrationValidator interface {
	Validate() error
}
