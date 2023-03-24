package tableManager

// TableManager提供的服务
// 与执行器交互
// REQUEST

// Create
// 创建表
// 创建每个表时，会添加一个自增主键字段'ID', 在上层添加
type Create struct {
	TbName string
	Fields []*FieldCreate
}

type Select struct {
	TbName        string   // select which table
	FNames        []string // select which table
	ReadForUpdate bool     // 快照读/当前读
	Where         *Where   // nil则没有where子句
}

type Update struct {
	TName    string
	FName    string // field to update
	ToUpdate string // update to its value
	Where    *Where
}

type Delete struct {
	TName string
	Where *Where
}

type Insert struct {
	TbName string
	Values []string // 字段值与字段一一对应,自增主键不用设置 TODO 可以用map实现非一一对应关系
}

type Where struct {
	Compare *Compare // 目前只支持单字段条件查询 TODO 可以用组合模式（树）实现多字段
}

type Compare struct {
	FieldName string
	CompareTo string // <= >= == < >
	Value     string
}

type FieldCreate struct {
	FName   string
	FType   string
	Indexed string
}

// TODO ADD 添加索引

// RESPONSE

type ResponseObject struct {
	Payload string `json:"Payload"`
	RowId   int    `json:"RowId"`
	ColId   int    `json:"ColId"`
}
