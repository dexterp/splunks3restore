package internal

import "sync"

// Runs a function in a go routine and manages wait group
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
