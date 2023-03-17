package tableManager

// Table

type Table interface {
	SearchAll() ([]int64, error)
}
