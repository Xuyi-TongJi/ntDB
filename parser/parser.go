package parser

type Parser interface {
	ParseRequest() Request
}

type StringParser struct {
}
