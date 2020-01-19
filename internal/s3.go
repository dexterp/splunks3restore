package internal

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"golang.org/x/time/rate"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	READPOOL = iota + 1
	RESTOREPOOL
)

var Pid int
var AWSRate *rate.Limiter

const AWSDefaultRate = float64(512)

type S3 struct {
	Config             *ConfigType
	Maxreadclients     uint16
	Maxundeleteclients uint16
	mu                 *sync.Mutex
	waitGroup          *sync.WaitGroup
	RmdeletemarkerC    chan *s3.DeleteMarkerEntry
	InputC             chan *s3.ListObjectVersionsInput
	poolids            map[int]int
	listFile           *os.File
	gracefuldown       bool
}

func New(config *ConfigType, maxreadclients, maxundeleteclients uint16) *S3 {
	r := S3{
		Config:             config,
		mu:                 &sync.Mutex{},
		waitGroup:          &sync.WaitGroup{},
		Maxreadclients:     maxreadclients,
		Maxundeleteclients: maxundeleteclients,
		InputC:             make(chan *s3.ListObjectVersionsInput, 256),
		RmdeletemarkerC:    make(chan *s3.DeleteMarkerEntry, 256),
		poolids:            map[int]int{},
	}
	return &r
}

func (r *S3) ProcessDeleteMarkers(prefixes []string, starttime, endtime time.Time) {
	readFunc := func(output *s3.ListObjectVersionsOutput, run bool) bool {
		for _, marker := range output.DeleteMarkers {
			if !(starttime.Before(*marker.LastModified) && endtime.After(*marker.LastModified) && *marker.IsLatest) {
				continue
			}
			r.WaitAdd(1)
			r.RmdeletemarkerC <- marker
		}
		return true
	}

	dryRunFunc := func(client *s3.S3, bucket string, marker *s3.DeleteMarkerEntry) {
		log.Printf("restore status=dryrun pid=%d key=%s\n", Pid, *marker.Key)
		return
	}

	muList := &sync.Mutex{}
	if r.Config.ListOutput != "" && r.listFile == nil {
		var err error
		r.listFile, err = os.OpenFile(r.Config.ListOutput, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Can not open file %s to store bucket list: %v", r.Config.ListOutput, err)
		}
		fmt.Fprintf(os.Stderr, "Writing s2 bucket list to %s\n", r.Config.ListOutput)
	}
	listFunc := func(client *s3.S3, bucket string, marker *s3.DeleteMarkerEntry) {
		muList.Lock()
		defer muList.Unlock()
		listfile := strings.TrimPrefix(*marker.Key, r.Config.S3bucket+"/")
		if r.listFile != nil {
			_, err := r.listFile.WriteString(listfile + "\n")
			if err != nil {
				msg := fmt.Sprintf("Error writing to file %s: %v", r.Config.ListOutput, err)
				fmt.Fprint(os.Stderr, msg+"\n")
				log.Printf("restore status=error pid=%d key=%s msg=\"%s\"", Pid, bucket, msg)
			}
		} else {
			os.Stdout.WriteString(listfile + "\n")
		}
		log.Printf("restore status=list pid=%d key=%s\n", Pid, listfile)
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
	case r.Config.DryRun:
		actionFunc = dryRunFunc
	case r.Config.RestoreList:
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
		prefix := strings.Join([]string{r.Config.Stack, "/", prefix}, "")

		input := &s3.ListObjectVersionsInput{
			Bucket: aws.String(r.Config.S3bucket),
			Prefix: aws.String(prefix),
		}
		if r.gracefuldown {
			r.Wait()
			if r.listFile != nil {
				r.listFile.Close()
			}
			return
		}
		r.WaitAdd(1)
		r.InputC <- input
	}

	r.Wait()
}

func (r *S3) Wait() {
	r.waitGroup.Wait()
}

func (r *S3) WaitAdd(delta int) {
	r.waitGroup.Add(delta)
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
		go listObjectVersionsWorker(s, fn)
	}
}

func listObjectVersionsWorker(s *S3, fn func(output *s3.ListObjectVersionsOutput, run bool) bool) {
	svc := s.GetClient()
	for {
		input := <-s.InputC
		AWSRateLimit()
		err := svc.ListObjectVersionsPages(
			input,
			fn,
		)
		if err != nil {
			log.Println(err.Error())
		}
		s.WaitAdd(-1)
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
		go deleteMarkerWorker(s, fn)
	}
}

func deleteMarkerWorker(s *S3, fn func(*s3.S3, string, *s3.DeleteMarkerEntry)) {
	client := s.GetClient()
	for {
		marker := <-s.RmdeletemarkerC
		if !s.Config.DryRun && s.Config.ListOutput != "" {
			AWSRateLimit()
		}
		fn(client, s.Config.S3bucket, marker)
		s.WaitAdd(-1)
	}
}

func (r *S3) Session() *session.Session {
	region := r.Config.GetBucketRegion()
	config := &aws.Config{
		Region: aws.String(region),
	}
	sess, _ := session.NewSession(config)
	return sess
}

func (r *S3) End() {
	if r.listFile != nil {
		r.listFile.Close()
	}
}

func AWSRateLimit() {
	if AWSRate != nil {
		err := AWSRate.Wait(context.TODO())
		if err != nil {
			log.Println(err.Error())
		}
	}
}

func SetupAWSRateLimit(defaultrate float64) {
	if AWSRate == nil {
		var l float64
		switch {
		case Config.RateLimit < 0:
			AWSRate = nil
		case Config.RateLimit == 0:
			l = defaultrate
		default:
			l = Config.RateLimit
		}
		AWSRate = rate.NewLimiter(rate.Limit(l), 512)
	}
}
