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
	ListOutput      string
	LogFile         string
	PrefixFile      string
	RestoreListFile string
	S3bucket        string
	Stack           string
	bucketRegion    string
	PrefixList      []string
	Audit           bool
	Continue        bool
	DateHelp        bool
	DryRun          bool
	Fixup           bool
	Restore         bool
	ListVer         bool
	Syslog          bool
	Verbose         bool
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
	c.Audit = opts.Audit
	c.Continue = opts.Continue
	c.Continue = opts.Continue
	c.DateHelp = opts.Datehelp
	c.DryRun = opts.DryRun
	c.Fixup = opts.Fixup
	c.FromDate = from
	c.ListVer = opts.ListVer
	c.ListOutput = opts.ListOutput
	c.LogFile = opts.Logfile
	c.PrefixFile = opts.PrefixFile
	c.PrefixList = opts.PrefixList
	c.RateLimit = opts.RateLimit
	c.Restore = opts.Restore
	c.S3bucket = opts.S3bucket
	c.Stack = opts.Stack
	c.ToDate = to
	c.Verbose = opts.Verbose
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
	dir, err := ioutil.TempDir(tmpdir, "s2deletemarkers")
	if err != nil {
		log.Print(err)
		Exit(-1)
	}
	s.tempdir = dir
	return s.tempdir
}
