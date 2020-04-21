package internal

import (
	"testing"
)

func TestGetUsage_restore_1(t *testing.T) {
	args := []string{"restore", "--log", "/var/log/restore.log", "--rate", "256", "--start", "now-1hr", "--end", "now", "--s3bucket", "splunks3restore", "--path", "some/path", "index~ID1", "index~ID2"}
	opts := GetUsage(args, "1.0.0")
	if !opts.Config.Restore {
		t.Fail()
	}
	if opts.Config.LogFile != "/var/log/restore.log" {
		t.Fail()
	}
	if opts.Config.RateLimit != 256 {
		t.Fail()
	}
	if opts.Config.S3bucket != "splunks3restore" {
		t.Fail()
	}
	if opts.Config.Path != "some/path" {
		t.Fail()
	}
	if len(opts.Config.BucketIds) != 2 {
		t.Fail()
	}
	diff := opts.Config.ToDate.Sub(opts.Config.FromDate)
	minutes := int(diff.Minutes())
	if minutes != 60 {
		t.Fail()
	}
}