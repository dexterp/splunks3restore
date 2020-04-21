package internal

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io/ioutil"
	"log"
	"os"
	"time"
)

var Config = ConfigType{}
var State = StateStruct{}

type ConfigType struct {
	FromDate        time.Time
	ToDate          time.Time
	LogFile         string
	BucketIdsFile   string
	RestoreListFile string
	S3bucket        string
	Path            string
	bucketRegion    string
	BucketIds       []string
	DateHelp        bool
	DryRun          bool
	Fixup           bool
	Restore         bool
	ListVer         bool
	Syslog          bool
	Verbose         bool
	ZeroFrozen      bool
	RateLimit       float64
}

func (c *ConfigType) Load(opts *OptUsage) {
	from, err := ParseTime(opts.Fromdate)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unrecognised <fromdate> format %s", opts.Fromdate)
	}
	to, err := ParseTime(opts.Todate)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unrecognised <todate> format %s", opts.Todate)
	}
	c.DateHelp = opts.Datehelp
	c.FromDate = from
	c.ListVer = opts.ListVer
	c.LogFile = opts.Logfile
	c.BucketIdsFile = opts.BucketIdsFile
	c.BucketIds = opts.BucketIds
	c.RateLimit = opts.RateLimit
	c.Restore = opts.Restore
	c.S3bucket = opts.S3bucket
	c.Path = opts.Path
	c.ToDate = to
}

func (c *ConfigType) GetBucketRegion() string {
	if c.bucketRegion != "" {
		return c.bucketRegion
	}
	defaultregion := os.Getenv("AWS_DEFAULT_REGION")
	if defaultregion == "" {
		defaultregion = "us-east-1"
	}
	svc := s3.New(session.New(&aws.Config{
		Region: aws.String(defaultregion),
	}))
	input := &s3.GetBucketLocationInput{
		Bucket: aws.String(c.S3bucket),
	}

	result, err := svc.GetBucketLocation(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Fprintf(os.Stderr, "Error while getting Bucket location for \"%s\": %v\n", c.S3bucket, aerr.Error())
			}
		} else {
			fmt.Fprintf(os.Stderr, "Error while getting Bucket location for \"%s\": %v\n", c.S3bucket, aerr.Error())
		}
		Exit(-1)
	}
	if result == nil {
		fmt.Fprintf(os.Stderr, "Error while getting Bucket location for \"%s\": No results returned\n", c.S3bucket)
		Exit(-1)
	}
	c.bucketRegion = *result.LocationConstraint
	return c.bucketRegion
}

type StateStruct struct {
	pid     int
	tempdir string
}

func (s *StateStruct) Pid() int {
	if s.pid > 0 {
		return s.pid
	}
	s.pid = os.Getpid()
	return s.pid
}

// TempDir returns path to a temporary directory
func (s *StateStruct) TempDir() string {
	if s.tempdir != "" {
		return s.tempdir
	}
	tmpdir := os.Getenv("TEMP")
	if tmpdir == "" {
		tmpdir = "/tmp"
	}
	dir, err := ioutil.TempDir(tmpdir, "splunks3restore")
	if err != nil {
		log.Print(err)
		Exit(-1)
	}
	s.tempdir = dir
	return s.tempdir
}
