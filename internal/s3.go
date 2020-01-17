package internal

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	READPOOL    = iota + 1
	RESTOREPOOL
)

var Pid int

type S3 struct {
	Bucket             string
	Stack              string
	Region             string
	Continue           bool
	DryRun             bool
	gracefuldown       bool
	Maxreadclients     uint16
	Maxundeleteclients uint16
	mu                 *sync.Mutex
	waitGroup          *sync.WaitGroup
	contiuationFile    string
	rmdeletemarkerC    chan *s3.DeleteMarkerEntry
	inputC             chan *s3.ListObjectVersionsInput
	poolids            map[int]int
	list               string
	listFile           *os.File
}

func New(bucket, region, stack string, maxreadclients, maxundeleteclients uint16, cont, dryrun bool, list string) *S3 {
	r := S3{
		Bucket:             bucket,
		Region:             region,
		Stack:              stack,
		Continue:           cont,
		DryRun:             dryrun,
		mu:                 &sync.Mutex{},
		waitGroup:          &sync.WaitGroup{},
		Maxreadclients:     maxreadclients,
		Maxundeleteclients: maxundeleteclients,
		inputC:             make(chan *s3.ListObjectVersionsInput, 256),
		rmdeletemarkerC:    make(chan *s3.DeleteMarkerEntry, 256),
		poolids:            map[int]int{},
		list:               list,
	}
	return &r
}

func (r *S3) RemoveDeleteMarkers(prefixes []string, starttime, endtime time.Time) {
	readFunc := func(output *s3.ListObjectVersionsOutput, run bool) bool {
		for _, marker := range output.DeleteMarkers {
			if !(starttime.Before(*marker.LastModified) && endtime.After(*marker.LastModified) && *marker.IsLatest) {
				continue
			}
			r.waitGroup.Add(1)
			r.rmdeletemarkerC <- marker
		}
		return true
	}

	dryRunFunc := func(client *s3.S3, bucket string, marker *s3.DeleteMarkerEntry) {
		log.Printf("restore status=dryrun pid=%d key=%s\n", Pid, *marker.Key)
		return
	}

	muList := &sync.Mutex{}
	if r.list != "" && r.listFile == nil {
		var err error
		r.listFile, err = os.OpenFile(r.list, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Can not open file %s to store bucket list: %v", r.list, err)
		}
		fmt.Fprintf(os.Stderr, "Writing s2 bucket list to %s\n", r.list)
	}
	listFunc := func(client *s3.S3, bucket string, marker *s3.DeleteMarkerEntry) {
		muList.Lock()
		defer muList.Unlock()
		splunkbucket := strings.TrimPrefix(*marker.Key, r.Stack+"/")
		_, err := r.listFile.WriteString(splunkbucket + "\n")
		if err != nil {
			msg := fmt.Sprintf("Error writing to file %s: %v", r.list, err)
			fmt.Fprint(os.Stderr, msg+"\n")
			log.Printf("restore status=error pid=%d bucket=%s msg=\"%s\"", Pid, bucket, msg)
		}
		log.Printf("restore status=list pid=%d key=%s\n", Pid, splunkbucket)
		return
	}

	restoreFunc := func(client *s3.S3, bucket string, marker *s3.DeleteMarkerEntry) {
		var err error
		_, err = client.DeleteObject(&s3.DeleteObjectInput{
			Bucket:    &bucket,
			Key:       marker.Key,
			VersionId: marker.VersionId,
		})
		if err == nil {
			log.Printf("restore status=ok pid=%d key=%s\n", Pid, *marker.Key)
		} else {
			log.Printf("retore status=fail pid=%d key=%s error=%s", Pid, *marker.Key, err.Error())
		}
	}
	var actionFunc func(client *s3.S3, bucket string, marker *s3.DeleteMarkerEntry)
	switch {
	case r.DryRun:
		actionFunc = dryRunFunc
	case r.list != "":
		actionFunc = listFunc
	default:
		actionFunc = restoreFunc
	}
	r.ListObjectVersionsPool(READPOOL, r.Maxreadclients, readFunc)
	r.DeleteMarkerPool(RESTOREPOOL, r.Maxundeleteclients, actionFunc)

	for _, prefix := range prefixes {
		if prefix == "" {
			continue
		}
		prefix := strings.Join([]string{r.Stack, "/", prefix}, "")

		input := &s3.ListObjectVersionsInput{
			Bucket: aws.String(r.Bucket),
			Prefix: aws.String(prefix),
		}
		if r.gracefuldown {
			r.Wait()
			r.listFile.Close()
			return
		}
		r.waitGroup.Add(1)
		r.inputC <- input
	}

	r.Wait()
}

func (r *S3) Wait() {
	r.waitGroup.Wait()
}

func (r *S3) SetGracefulDown() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gracefuldown = true
}

func (s *S3) ListObjectVersionsPool(poolid int, concur uint16, fn func(output *s3.ListObjectVersionsOutput, run bool) bool) {
	if _, ok := s.poolids[poolid]; ok {
		return // Pool already created
	}
	s.poolids[poolid] = int(concur)
	for i := uint16(0); i < concur; i++ {
		svc := s.GetClient()
		go deleteMarkerReadWorker(svc, fn, s.inputC, s.waitGroup)
	}
}

func deleteMarkerReadWorker(svc *s3.S3, fn func(output *s3.ListObjectVersionsOutput, run bool) bool, jobs <-chan *s3.ListObjectVersionsInput, waitgroup *sync.WaitGroup) {
	for {
		input := <-jobs
		err := svc.ListObjectVersionsPages(
			input,
			fn,
		)
		if err != nil {
			log.Println(err.Error())
		}
		waitgroup.Add(-1)
	}
}

func (r *S3) GetClient() *s3.S3 {
	svc := s3.New(r.Session())
	return svc
}

func (s *S3) DeleteMarkerPool(poolid int, routines uint16, fn func(*s3.S3, string, *s3.DeleteMarkerEntry)) {
	if _, ok := s.poolids[poolid]; ok {
		return // Pool already created
	}
	s.poolids[poolid] = int(routines)
	for i := uint16(0); i < routines; i++ {
		svc := s.GetClient()
		go deleteMarkerRemoveWorker(svc, fn, s.Bucket, s.rmdeletemarkerC, s.waitGroup)
	}
}

func deleteMarkerRemoveWorker(client *s3.S3, fn func(*s3.S3, string, *s3.DeleteMarkerEntry), bucket string, jobs <-chan *s3.DeleteMarkerEntry, waitgroup *sync.WaitGroup) {
	for {
		marker := <-jobs
		fn(client, bucket, marker)
		waitgroup.Add(-1)
	}
}

func (r *S3) Session() *session.Session {
	config := &aws.Config{
		Region: aws.String(r.Region),
	}
	sess, _ := session.NewSession(config)
	return sess
}

func BucketLocation(bucket string) string {
	defaultregion := os.Getenv("AWS_DEFAULT_REGION")
	if defaultregion == "" {
		defaultregion = "us-east-1"
	}
	svc := s3.New(session.New(&aws.Config{
		Region: aws.String(defaultregion),
	}))
	input := &s3.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	}

	result, err := svc.GetBucketLocation(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Fprintf(os.Stderr, "Error while getting Bucket location for \"%s\": %v\n", bucket, aerr.Error())
			}
		} else {
			fmt.Fprintf(os.Stderr, "Error while getting Bucket location for \"%s\": %v\n", bucket, aerr.Error())
		}
		Exit(-1)
	}
	if result == nil {
		fmt.Fprintf(os.Stderr, "Error while getting Bucket location for \"%s\": No results returned\n", bucket)
		Exit(-1)
	}
	region := *result.LocationConstraint
	return region
}

func (r *S3) End() {
	r.listFile.Close()
}
