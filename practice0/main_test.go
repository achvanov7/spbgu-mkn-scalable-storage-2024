package main

import (
	//"slices"
	"testing"
)

func TestFiller(t *testing.T) {
	b := [100]byte{}
	zero := byte('0')
	one := byte('1')
	filler(b[:], zero, one)
	// Заполнить здесь ассерт, что b содержит zero и что b содержит one
	foundZero := false
	foundOne := false
	for i := 0; i < len(b); i++ {
		if b[i] == zero {
			foundZero = true
		} else if b[i] == one {
			foundOne = true
		}
	}
	if !foundZero {
		panic("No zero :(")
	}
	if !foundOne {
		panic("No one :(")
	}
}
