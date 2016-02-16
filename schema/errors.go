package schema

var (
	ErrInvalidTableOperation         = "Not one of the valid table operations (add, remove, update)"
	ErrInvalidColumnOperation        = "Not one of the valid column operations (add, remove, update)"
	ErrAddTableOnExistingTable       = "Cannot Add table that already exists"
	ErrRemoveTableOnNonExistingTable = "Cannot Remove Table that doesn't exist"
	ErrUpdateTableonNonExistingTable = "Cannot Update Table that doesn't exist"
	ErrMustContainDistKey            = "Cannot proceed without having atleast a single DistKey"
	ErrDistKeyNotInCols              = "DistKey must be present in outbound col names"
	ErrSortKeyNotInCols              = "SortKey must be present in outbound col names"
	ErrDifferentTableOptions         = "Cannot change Table Options on update"
	ErrRemoveColisDistKey            = "Remove Column operation is on DistKey"
	ErrUpdateColisDistKey            = "Update Column operation is on DistKey"
	ErrUpdateColNonExistingCol       = "Cannot Update column that does not exist"
	ErrRemoveColNonExistingCol       = "Cannot Remove column that does not exist"
	ErrColumnOpNotAdd                = "Columnn Operation should be Add"
	ErrColumnOpNotUpdate             = "Columnn Operation should be Update"
	ErrColumnOpNotRemove             = "Columnn Operation should be Remove"
	ErrInvalidTransformer            = "Invalid Transformer"
	ErrVarCharBytesMax               = "varchar size exceeds max (65546), 64k-1"
	ErrVarCharNotInt                 = "varchar option provided not an int"
	ErrInvalidIdentifier             = "Provided name is an invalid sql identifier"
	ErrOutboundNameCollision         = "Outbound Column name collides with existing column"
	ErrTooManyColumns                = "Having more than 300 columns in a redshift table slows queries immensely"
)

type ColumnError struct {
	Msg string
}

func (e ColumnError) Error() string {
	return e.Msg
}

type TableError struct {
	Msg string
}

func (e TableError) Error() string {
	return e.Msg
}

func ErrorType(err error) string {
	switch err.(type) {
	default:
		return "GenericError"
	case TableError:
		return "TableError"
	case ColumnError:
		return "ColumnError"
	}
}
