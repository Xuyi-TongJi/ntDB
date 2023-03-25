package indexManager

// [LeafFlag]1[KeyNumber]8[Brother_UID]8

type Index interface {
	SearchRange(left, right any) ([]int64, error)
}

// BPlusTree Index

type Node struct {
}

type BPlusTree struct {
}
