package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

func main() {
	b := [100]byte{}
	var mu sync.Mutex
	for {
		go func() {
			mu.Lock()
			fillLeft(b[:])
			mu.Unlock()
		}()
		go func() {
			mu.Lock()
			fillRight(b[:])
			mu.Unlock()
		}()
		go func() {
			mu.Lock()
			print(b[:])
			mu.Unlock()
		}()
	}
}

func filler(b []byte, ifzero byte, ifnot byte) {
	for i := 0; i < len(b); i++ {
		if rand.Int()%2 == 0 {
			b[i] = ifzero
		} else {
			b[i] = ifnot
		}
	}
}

func fillLeft(b []byte) {
	filler(b[0:len(b)/2], byte('0'), byte('1'))
	time.Sleep(time.Second)
}

func fillRight(b []byte) {
	filler(b[len(b)/2:], byte('X'), byte('Y'))
	time.Sleep(time.Second)
}

func print(b []byte) {
	for i := 0; i < len(b); i++ {
		fmt.Printf("%c", b[i])
	}
	fmt.Println()
	time.Sleep(time.Second)
}
