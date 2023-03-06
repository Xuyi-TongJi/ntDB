package main

type I interface {
	boo()
}

type A struct {
	a int
}

func (a A) boo() {}

func NewI() I {
	return A{}
}

func main() {
}
