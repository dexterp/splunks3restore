package internal

import (
	"errors"
	"time"
)
import "github.com/karrick/tparse"

func ParseTime(ts string) (time.Time, error) {
	p := _timeparsers()
	for _, parser := range p {
		t, err := parser(ts)
		if err == nil {
			return t, nil
		}
	}
	return time.Time{}, errors.New("Unrecognised datetime string")
}

func _timeparsers() []func(string) (time.Time, error) {
	return []func(string) (time.Time, error){
		func(ts string) (time.Time, error) {
			return time.Parse("2006-01-02T15:04:05-0700", ts)
		},
		func(ts string) (time.Time, error) {
			return time.Parse("2006-01-02T15:04:05", ts)
		},
		func(ts string) (time.Time, error) {
			return time.Parse("2006-01-02", ts)
		},
		func(ts string) (time.Time, error) {
			return time.Parse("15:04:05", ts)
		},
		func(ts string) (time.Time, error) {
			return time.Parse("15:04", ts)
		},
		func(ts string) (time.Time, error) {
			return tparse.AddDuration(time.Now(), ts)
		},
		func(ts string) (time.Time, error) {
			return tparse.ParseNow(time.RFC3339, ts)
		},
	}
}
