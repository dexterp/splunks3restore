package routines

import (
	"math/rand"
	"sync"
	"testing"
)

func TestRoutines_ActionFunc(t *testing.T) {
	min := 15
	max := 4096
	randnum := rand.Intn(max-min) + min
	var singleCnt int
	singleMu := &sync.Mutex{}
	singleFunc := ActionFunc(func(id *Id, single interface{}) {
		singleMu.Lock()
		defer singleMu.Unlock()
		if _, ok := single.(int); !ok {
			t.Error("Could not determine original value")
		}
		singleCnt += 1
	})
	single := New("testactionfunc", 3, 5, 256)
	err := single.Start(singleFunc)
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
	if singleCnt != randnum {
		t.Fail()
	}
}

func TestRoutines_ActionFuncBatch(t *testing.T) {
	min := 15
	max := 4096
	randnum := rand.Intn(max-min) + min
	var batchCnt int
	batchMu := &sync.Mutex{}
	batchFunc := ActionFuncBatch(func(id *Id, batch []interface{}) {
		for _, iface := range batch {
			if val, ok := iface.(int); ok {
				batchMu.Lock()
				batchCnt += val
				batchMu.Unlock()
			} else {
				t.Error("Could not determine original value")
			}
		}
	})
	batch := New("testactionfuncbatch", 3, 5, 256)
	err := batch.Start(batchFunc)
	if err != nil {
		t.Error(err)
	}
	for i := 1; i <= randnum; i++ {
		if err := batch.AddJob(1); err != nil {
			t.Error(err)
		}
	}
	batch.Close()
	if batchCnt != randnum {
		t.Fail()
	}
}

func TestRoutines_Func(t *testing.T) {
	min := 15
	max := 4096
	randnum := rand.Intn(max-min) + min
	var singleCnt int
	singleMu := &sync.Mutex{}
	singleFunc := func(id *Id, single interface{}) {
		singleMu.Lock()
		defer singleMu.Unlock()
		if _, ok := single.(int); !ok {
			t.Error("Could not determine original value")
		}
		singleCnt += 1
	}
	single := New("testfunc", 3, 5, 256)
	err := single.Start(singleFunc)
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
	if singleCnt != randnum {
		t.Fail()
	}
}

func TestRoutines_FuncBatch(t *testing.T) {
	min := 15
	max := 4096
	randnum := rand.Intn(max-min) + min
	var batchCnt int
	batchMu := &sync.Mutex{}
	batchFunc := func(id *Id, batch []interface{}) {
		for _, iface := range batch {
			if val, ok := iface.(int); ok {
				batchMu.Lock()
				batchCnt += val
				batchMu.Unlock()
			} else {
				t.Error("Could not determine original value")
			}
		}
	}
	batch := New("testfuncbatch", 3, 5, 256)
	err := batch.Start(batchFunc)
	if err != nil {
		t.Error(err)
	}
	for i := 1; i <= randnum; i++ {
		if err := batch.AddJob(1); err != nil {
			t.Error(err)
		}
	}
	batch.Close()
	if batchCnt != randnum {
		t.Fail()
	}
}

func TestRoutines_Close(t *testing.T) {
	min := 15
	max := 4096
	randnum := rand.Intn(max-min) + min
	var batchCnt int
	batchMu := &sync.Mutex{}
	batchFunc := func(id *Id, batch []interface{}) {
		for _, iface := range batch {
			if val, ok := iface.(int); ok {
				batchMu.Lock()
				batchCnt += val
				batchMu.Unlock()
			} else {
				t.Error("Could not determine original value")
			}
		}
	}
	batchsz := uint(randnum * 2)
	batch := New("testclose", 1, batchsz, 256)
	err := batch.Start(batchFunc)
	if err != nil {
		t.Error(err)
	}
	for i := 1; i <= randnum; i++ {
		if err := batch.AddJob(1); err != nil {
			t.Error(err)
		}
	}
	if batchCnt > 0 {
		t.Errorf("Expected a zero count before Flush() got %d", batchCnt)
	}
	batch.Close()
	if batchCnt != randnum {
		t.Errorf("Expected generated number %d to match calculated number %d", randnum, batchCnt)
	}
}

func TestRoutines_Flush(t *testing.T) {
	min := 15
	max := 4096
	randnum := rand.Intn(max-min) + min
	var batchCnt int
	batchMu := &sync.Mutex{}
	batchFunc := func(id *Id, batch []interface{}) {
		for _, iface := range batch {
			if val, ok := iface.(int); ok {
				batchMu.Lock()
				batchCnt += val
				batchMu.Unlock()
			} else {
				t.Error("Could not determine original value")
			}
		}
	}
	batchsz := uint(randnum * 2)
	batch := New("testflush", 1, batchsz, 256)
	err := batch.Start(batchFunc)
	if err != nil {
		t.Error(err)
	}
	for i := 1; i <= randnum; i++ {
		if err := batch.AddJob(1); err != nil {
			t.Error(err)
		}
	}
	if batchCnt > 0 {
		t.Errorf("Expected a zero count before Flush() got %d", batchCnt)
	}
	batch.Flush()
	if batchCnt != randnum {
		t.Errorf("Expected generated number %d to match calculated number %d", randnum, batchCnt)
	}
	batch.Close()
	if batchCnt != randnum {
		t.Errorf("Expected generated number %d to match calculated number %d", randnum, batchCnt)
	}
}
