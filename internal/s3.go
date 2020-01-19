package internal

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	"golang.org/x/time/rate"
	"log"
	"math/rand"
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

const AWSDefaultRate = float64(256)

type S3 struct {
	Config             *ConfigType
	Maxreadclients     uint16
	Maxundeleteclients uint16
	mu                 *sync.Mutex
	waitGroup          *sync.WaitGroup
	RmdeletemarkerC    chan []*s3.DeleteMarkerEntry
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
		RmdeletemarkerC:    make(chan []*s3.DeleteMarkerEntry, 256),
		poolids:            map[int]int{},
	}
	return &r
}

func (r *S3) ProcessDeleteMarkers(prefixes []string, starttime, endtime time.Time) {
	readFunc := func(output *s3.ListObjectVersionsOutput, run bool) bool {
		markers := []*s3.DeleteMarkerEntry{}
		cnt := 0
		for _, marker := range output.DeleteMarkers {
			if !(starttime.Before(*marker.LastModified) && endtime.After(*marker.LastModified) && *marker.IsLatest) {
				continue
			}

			markers = append(markers, marker)
			cnt++
			if cnt == 1000 {
				r.WaitAdd(1)
				r.RmdeletemarkerC <- markers
				markers = []*s3.DeleteMarkerEntry{}
			}
		}
		if len(markers) > 0 {
			r.WaitAdd(1)
			r.RmdeletemarkerC <- markers
		}
		return true
	}

	dryRunFunc := func(client *s3.S3, bucket string, markers []*s3.DeleteMarkerEntry) {
		batchid := genuuid()
		for _, marker := range markers {
			log.Printf("restore status=dryrun batchid=%s pid=%d key=%s\n", batchid, Pid, *marker.Key)
		}
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
	listFunc := func(client *s3.S3, bucket string, markers []*s3.DeleteMarkerEntry) {
		batchid := genuuid()
		muList.Lock()
		defer muList.Unlock()
		for _, marker := range markers {
			listfile := strings.TrimPrefix(*marker.Key, r.Config.S3bucket+"/")
			if r.listFile != nil {
				r.listFile.WriteString(listfile + "\n")
			} else {
				os.Stdout.WriteString(listfile + "\n")
			}
			log.Printf("restore status=list batchid=%s pid=%d key=%s\n", batchid, Pid, *marker.Key)
		}
		return
	}

	restoreFunc := func(client *s3.S3, bucket string, markers []*s3.DeleteMarkerEntry) {
		if len(markers) == 0 {
			return
		}
		var err error
		restoreList := []*s3.ObjectIdentifier{}
		batchid := genuuid()
		for _, marker := range markers {
			obj := s3.ObjectIdentifier{
				Key:       marker.Key,
				VersionId: marker.VersionId,
			}
			restoreList = append(restoreList, &obj)
		}
		deleteOutputs, err := client.DeleteObjects(&s3.DeleteObjectsInput{
			Bucket: &bucket,
			Delete: &s3.Delete{
				Objects: restoreList,
				Quiet:   aws.Bool(false),
			},
		})
		if err != nil {
			log.Printf("restore status=err batchid=%s pid=%d msg=\"%v\"", batchid, Pid, err)
		} else if deleteOutputs != nil {
			for _, marker := range deleteOutputs.Deleted {
				log.Printf("restore status=ok batchid=%s pid=%d key=%s\n", batchid, Pid, *marker.Key)
			}
			for _, marker := range deleteOutputs.Errors {
				log.Printf("retore status=fail batchid=%s pid=%d key=%s error=%v\n", batchid, Pid, *marker.Key, *marker.Message)
			}
		}
	}
	var actionFunc func(client *s3.S3, bucket string, markers []*s3.DeleteMarkerEntry)
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

func (s *S3) DeleteMarkerPool(poolid int, routines uint16, fn func(*s3.S3, string, []*s3.DeleteMarkerEntry)) {
	if _, ok := s.poolids[poolid]; ok {
		return // Pool already created
	}
	s.poolids[poolid] = int(routines)
	rateFn := func() { /* noop */ }
	if !s.Config.DryRun && !s.Config.RestoreList {
		rateFn = AWSRateLimit
	}
	for i := uint16(0); i < routines; i++ {
		go deleteMarkerWorker(s, fn, rateFn)
	}
}

func deleteMarkerWorker(s *S3, fn func(*s3.S3, string, []*s3.DeleteMarkerEntry), rateLimitFn func()) {
	client := s.GetClient()
	for {
		marker := <-s.RmdeletemarkerC
		rateLimitFn()
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

func genuuid() string {
	rand.Seed(time.Now().UnixNano())
	u := uuid.New()
	return u.String()
}
