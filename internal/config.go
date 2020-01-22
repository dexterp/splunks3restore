package internal

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"os"
	"time"
)

var Config = ConfigType{}

type ConfigType struct {
	Verbose         bool
	Audit           bool
	Restore         bool
	RestoreList     bool
	RestoreListFile string
	FromDate        time.Time
	ToDate          time.Time
	S3bucket        string
	Stack           string
	PrefixList      []string
	PrefixFile      string
	Continue        bool
	DateHelp        bool
	DryRun          bool
	Syslog          bool
	LogFile         string
	ListOutput      string
	RateLimit       float64
	bucketRegion    string
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
	c.Verbose = opts.Verbose
	c.Restore = opts.Restore
	c.FromDate = from
	c.ToDate = to
	c.S3bucket = opts.S3bucket
	c.Stack = opts.Stack
	c.PrefixList = opts.PrefixList
	c.PrefixFile = opts.PrefixFile
	c.Continue = opts.Continue
	c.DateHelp = opts.Datehelp
	c.DryRun = opts.DryRun
	c.Continue = opts.Continue
	c.LogFile = opts.Logfile
	c.RestoreList = opts.RestoreList
	c.ListOutput = opts.ListOutput
	c.RateLimit = opts.RateLimit
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
