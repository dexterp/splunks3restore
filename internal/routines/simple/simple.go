package simple

import (
	"cd.splunkdev.com/dplameras/s2deletemarkers/internal/routines"
	"github.com/google/uuid"
	"math/rand"
	"time"
)

// Starts a new routine pool
//
// count sets the number of go routines to run
// batchlen limits the batch length, which is the length of the array passed to the ActionFuncBatch function.
// chanlen sets the size of the internal buffered channel
func New(count uint) *routines.Routines {
	name := genuuid()
	return routines.New(name, count, 1, 256)
}

func genuuid() string {
	rand.Seed(time.Now().UnixNano())
	u := uuid.New()
	return u.String()
}
