package migration_ops

type MigrationOps interface {
	AddTable()
	UpdateTable()
	RemoveTable()
	AddColumn()
	UpdateColumn()
	RemoveColumn()
}
