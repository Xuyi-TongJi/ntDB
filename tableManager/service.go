package tableManager

// Create
// 创建表
// 创建每个表时，会添加一个自增主键字段'ID', 在上层添加
type Create struct {
	tbName string
	fields []*FieldCreate
}

type Select struct {
	tbName        string   // select which table
	fNames        []string // select which table
	readForUpdate bool     // 快照读/当前读
	where         *Where   // nil则没有where子句
}

type Update struct {
	sel      *Select
	fName    string // field to update
	toUpdate any    // update to its value
}

type Delete struct {
	sel *Select
}

type Insert struct {
	tbName string
	values []any // 字段值与字段一一对应,自增主键不用设置 TODO 可以用map实现非一一对应关系
}

type Where struct {
	compare *Compare // 目前只支持单字段条件查询 TODO 可以用组合模式（树）实现多字段
}

type Compare struct {
	fieldName string
	compareTo string // <= >= == < >
	value     any
}

type FieldCreate struct {
	fName   string
	fType   FieldType
	indexed bool
}

type ResponseObject struct {
	payload string
	rowId   int
	colId   int
}
