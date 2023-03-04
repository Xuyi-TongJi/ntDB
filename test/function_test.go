package main

import (
	"fmt"
	"testing"
)

func TestArray(t *testing.T) {
	arr1 := [10]int{}
	arr2 := arr1[5:]
	arr2[1] = 10
	fmt.Println(arr1)
	fmt.Println(arr2)
	// Conclusion 即使数组是值类型，切片操作依然共享同一片内存
}
