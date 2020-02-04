package simple

import (
	"cd.splunkdev.com/dplameras/s2deletemarkers/internal/routines"
	"math/rand"
	"sync"
	"testing"
)

func TestRoutinesSimple(t *testing.T) {
	min := 15
	max := 4096
	randnum := rand.Intn(max-min) + min
	var cnt int
	mu := &sync.Mutex{}
	fn := routines.ActionFunc(func(id *routines.Id, single interface{}) {
		mu.Lock()
		defer mu.Unlock()
		if _, ok := single.(int); !ok {
			t.Error("Could not determine original value")
		}
		cnt += 1
	})
	single := New(3)
	err := single.Start(fn)
	if err != nil {
		t.Error(err)
	}
	for i := 1; i <= randnum; i++ {
		if err := single.AddJob(1); err != nil {
			t.Error(err)
		}
	}
	single.Close()
	single.Wait()
	if cnt != randnum {
		t.Fail()
	}
}
