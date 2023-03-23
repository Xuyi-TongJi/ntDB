package executor

type Parser interface {
	ParseRequest()
}

type StringParser struct {
}
