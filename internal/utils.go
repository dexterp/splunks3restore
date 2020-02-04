package internal

import (
	"fmt"
	"github.com/google/uuid"
	"io"
	"math/rand"
	"os"
	"sync"
	"time"
)

// Runs a function in a go routine and manages wg group
func WaitFunc(fn func(), wg *sync.WaitGroup) {
	if wg != nil {
		wg.Add(1)
	}
	go func() {
		fn()
		if wg != nil {
			wg.Add(-1)
		}
	}()

}

func Genuuid() string {
	rand.Seed(time.Now().UnixNano())
	u := uuid.New()
	return u.String()
}

func Copy(src string, dst string) (int64, error) {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return 0, err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return 0, err
	}
	defer destination.Close()
	nBytes, err := io.Copy(destination, source)
	return nBytes, err
}
