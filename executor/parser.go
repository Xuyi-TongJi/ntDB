package executor

import (
	"myDB/tableManager"
	"strings"
)

type Parser interface {
	ParseRequest(args []string) (CommandType, []any, error)
}

type TrieParser struct {
	commands      map[string]CommandType // 不需要请求参数的指令
	parseCommands map[string]*Trie
}

type Trie struct {
	root *Node
}

const (
	ANY int = 1 << 30
)

type Node struct {
	minParamNum int
	maxParamNum int
	name        string
	child       map[string]*Node
}

type Status struct {
	paramNum int
	name     string
	entity   any // 实体类
}

func (parser *TrieParser) ParseRequest(args []string) (CommandType, []any, error) {
	cmd := INVALID
	if len(args) == 0 {
		return cmd, nil, &ErrorRequestArgNumber{}
	}
	query := strings.ToUpper(args[0])
	entity := make([]any, 0)
	switch query {
	case "SELECT":
		{
			cmd = SELECT
			sel := &tableManager.Select{}
			where := &tableManager.Where{}
			sel.Where = where
			entity = append(entity, sel, where)
		}
	case "UPDATE":
		{
			cmd = UPDATE
			update := &tableManager.Update{}
			where := &tableManager.Where{}
			update.Where = where
			entity = append(entity, update, where)
		}
	case "INSERT":
		{
			cmd = INSERT
			insert := &tableManager.Insert{}
			entity = append(entity, insert)
		}
	case "DELETE":
		{
			cmd = DELETE
			del := &tableManager.Delete{}
			where := &tableManager.Where{}
			del.Where = where
			entity = append(entity, del, where)
		}
	case "CREATE":
		{
			cmd = CREATE
			cre := &tableManager.Create{}
			cre.Fields = make([]*tableManager.FieldCreate, 0)
			entity = append(entity, cre)
		}
	default:
		{
			for command, value := range parser.commands {
				if command == query {
					cmd = value
				}
			}
			return cmd, nil, nil
		}
	}
	// parseQuery
	trie := parser.parseCommands[query]
	if err := parseQuery(trie, entity, args); err != nil {
		return cmd, nil, err
	}
	return cmd, entity, nil
}

func NewTrieParser() Parser {
	commands := map[string]CommandType{}
	commands["SHOW"] = SHOW
	commands["BEGIN"] = BEGIN
	commands["ABORT"] = ABORT
	commands["COMMIT"] = COMMIT
	st := buildSelectTrie()
	ut := buildUpdateTrie()
	it := buildInsertTrie()
	dt := buildDeleteTrie()
	ct := buildCreateTrie()
	parseCommands := map[string]*Trie{}
	parseCommands["SELECT"] = st
	parseCommands["UPDATE"] = ut
	parseCommands["INSERT"] = it
	parseCommands["DELETE"] = dt
	parseCommands["CREATE"] = ct
	return &TrieParser{
		parseCommands: parseCommands,
		commands:      commands,
	}
}

// select <field>...from <table> [[where <field> compare(>,=,<,>=,<=) <value>]]
func buildSelectTrie() *Trie {
	trie := &Trie{
		root: &Node{0, 0, "", map[string]*Node{}},
	}
	currentNode := trie.root
	currentNode.child["SELECT"] = &Node{
		1, ANY, "SELECT", map[string]*Node{},
	}
	currentNode = currentNode.child["SELECT"]
	currentNode.child["FROM"] = &Node{
		1, 1, "FROM", map[string]*Node{},
	}
	currentNode = currentNode.child["FROM"]
	addWhereCondition(currentNode)
	return trie
}

// update <table> set <field> = <value> [[where <field> compare <value> ]]
func buildUpdateTrie() *Trie {
	trie := &Trie{
		root: &Node{0, 0, "", map[string]*Node{}},
	}
	currentNode := trie.root
	currentNode.child["UPDATE"] = &Node{
		1, 1, "UPDATE", map[string]*Node{},
	}
	currentNode = currentNode.child["UPDATE"]
	currentNode.child["SET"] = &Node{
		1, 1, "SET", map[string]*Node{},
	}
	currentNode = currentNode.child["SET"]
	currentNode.child["="] = &Node{
		1, 1, "=", map[string]*Node{},
	}
	currentNode = currentNode.child["="]
	addWhereCondition(currentNode)
	return trie
}

// insert <table> values <field> ...
func buildInsertTrie() *Trie {
	trie := &Trie{
		root: &Node{0, 0, "", map[string]*Node{}},
	}
	currentNode := trie.root
	currentNode.child["INSERT"] = &Node{
		1, 1, "INSERT", map[string]*Node{},
	}
	currentNode = currentNode.child["INSERT"]
	currentNode.child["VALUES"] = &Node{
		1, ANY, "VALUES", map[string]*Node{},
	}
	return trie
}

// delete <table> [[where <> compare <>]]
func buildDeleteTrie() *Trie {
	trie := &Trie{
		root: &Node{0, 0, "", map[string]*Node{}},
	}
	currentNode := trie.root
	currentNode.child["DELETE"] = &Node{
		1, 1, "DELETE", map[string]*Node{},
	}
	currentNode = currentNode.child["DELETE"]
	addWhereCondition(currentNode)
	return trie
}

// create <table name> {fName fType indexed, fName fType indexed ...}
func buildCreateTrie() *Trie {
	trie := &Trie{
		root: &Node{0, 0, "", map[string]*Node{}},
	}
	currentNode := trie.root
	currentNode.child["CREATE"] = &Node{
		1, 1, "CREATE", map[string]*Node{},
	}
	currentNode = currentNode.child["CREATE"]
	currentNode.child["{"] = &Node{
		2, 3, "{", map[string]*Node{},
	}
	currentNode = currentNode.child["{"]
	currentNode.child["}"] = &Node{
		0, 0, "}", nil,
	}
	endNode := currentNode.child["}"]
	currentNode.child[","] = &Node{
		2, 3, ",", nil,
	}
	currentNode = currentNode.child[","]
	currentNode.child[","] = currentNode // 自环
	currentNode.child["}"] = endNode
	return trie
}

// where <fieldName> <compare> <value>
func addWhereCondition(currentNode *Node) {
	// 1.0版本暂时只支持一个where子句
	currentNode.child["WHERE"] = &Node{
		3, 3, "WHERE", map[string]*Node{},
	}
	currentNode = currentNode.child["WHERE"]
}

func parseQuery(grammar *Trie, entity []any, args []string) error {
	current := grammar.root
	count := len(args)
	var status *Status
	eIndex := -1
	currentArgs := make([]string, 0)
	for i := 0; i < count; i++ {
		// key word
		upper := strings.ToUpper(args[i])
		if _, ext := current.child[upper]; ext {
			if status != nil {
				if err := packStatus(status, current, currentArgs); err != nil {
					return err
				}
			}
			// change status
			if status == nil || upper == "WHERE" {
				eIndex += 1
			}
			current = current.child[upper]
			status = &Status{paramNum: 0, name: current.name, entity: entity[eIndex]}
			currentArgs = make([]string, 0)
		} else {
			// 常规参数
			currentArgs = append(currentArgs, args[i])
		}
		// check last status must be nil
		if err := packStatus(status, current, currentArgs); err != nil {
			return err
		}
	}
	return nil
}

func packStatus(status *Status, current *Node, currentArgs []string) error {
	// check last status must be nil
	if status.paramNum != len(currentArgs) {
		return &ErrorRequestArgNumber{}
	}
	if status.paramNum < current.minParamNum || status.paramNum > current.maxParamNum {
		return &ErrorRequestArgNumber{}
	}
	if err := packQueryEntity(currentArgs, status.entity); err != nil {
		return err
	}
	return nil
}

// args 已经被check
func packQueryEntity(args []string, entity any) error {
	switch entity.(type) {
	case *tableManager.Select:
		{
			// check fNames
			sel := entity.(*tableManager.Select)
			if sel.FNames == nil {
				// pack FNames
				sel.FNames = args
			} else {
				sel.TbName = args[0]
			}
		}
	case *tableManager.Update:
		{
			upd := entity.(*tableManager.Update)
			if upd.TName == "" {
				upd.TName = args[0]
			} else if upd.FName == "" {
				upd.TName = args[0]
			} else {
				upd.ToUpdate = args[0]
			}
		}
	case *tableManager.Delete:
		{
			del := entity.(*tableManager.Delete)
			del.TName = args[0]
		}
	case *tableManager.Insert:
		{
			ins := entity.(*tableManager.Insert)
			if ins.TbName == "" {
				ins.TbName = args[0]
			} else if ins.Values == nil {
				ins.Values = args
			}
		}
	case *tableManager.Where:
		{
			where := entity.(*tableManager.Where)
			if len(args) > 0 {
				where.Compare = &tableManager.Compare{FieldName: args[0], CompareTo: args[1], Value: args[2]}
			}
		}
	case *tableManager.Create:
		{
			create := entity.(*tableManager.Create)
			if create.TbName == "" {
				create.TbName = args[0]
			} else {
				if create.Fields == nil {
					create.Fields = make([]*tableManager.FieldCreate, 0)
				}
				if len(args) == 2 {
					create.Fields = append(create.Fields,
						&tableManager.FieldCreate{FName: args[0], FType: args[1]})
				} else {
					create.Fields = append(create.Fields,
						&tableManager.FieldCreate{FName: args[0], FType: args[1], Indexed: args[2]})
				}
			}
		}
	default:
		return &ErrorInvalidEntity{}
	}
	return nil
}

// error

type ErrorInvalidEntity struct{}

type ErrorRequestArgNumber struct{}

func (err *ErrorInvalidEntity) Error() string {
	return "Unknown query type"
}

func (err *ErrorRequestArgNumber) Error() string {
	return "Invalid arg number"
}
