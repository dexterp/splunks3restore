package internal

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/crosseyed/splunks3restore/internal/receipt"
	"github.com/crosseyed/splunks3restore/internal/routines"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type S3 struct {
	Config       *ConfigType
	State        *StateStruct
	gracefuldown bool
	rtInput      *routines.Routines
	rtRestore    *routines.Routines
	rtFixup      *routines.Routines
	wg           *sync.WaitGroup
}

func NewS3client(config *ConfigType, state *StateStruct) *S3 {
	s := &S3{
		Config:    config,
		State:     state,
		rtInput:   routines.New("input", 64, 20, 2048),
		rtRestore: routines.New("restore", 64, 256, 2048),
		rtFixup:   routines.New("fixup", 32, 4, 2048),
		wg:        &sync.WaitGroup{},
	}
	return s
}

func (s *S3) ScanPrefix(prefix string) error {
	if s.gracefuldown {
		return nil
	}
	return s.rtInput.AddJob(prefix)
}

// Kill gracefully shutdowns
func (s *S3) Kill() {
	s.rtInput.Kill(false)
	s.rtFixup.Kill(false)
	s.Shutdown()
}

func (s *S3) Shutdown() {
	s.rtInput.WaitChan()
	s.rtInput.Close()
	s.rtInput.Wait()
	s.rtRestore.WaitChan()
	s.rtRestore.Close()
	s.rtRestore.Wait()
	s.rtFixup.WaitChan()
	s.rtFixup.Close()
	s.rtFixup.Wait()
	s.wg.Wait()
}

func (s *S3) StartWorkers() {
	var scanFunc routines.ActionFuncBatch
	var restoreFunc routines.ActionFuncBatch
	var fixupFunc routines.ActionFuncBatch

	switch {
	case s.Config.Restore && s.Config.DryRun:
		scanFunc = s.scanDryFunc()
	case s.Config.ListVer:
		scanFunc = s.scanListVer()
	case s.Config.Fixup:
		scanFunc = s.scanFixupFunc()
		fixupFunc = s.actionFixUp()
	case s.Config.Restore:
		scanFunc = s.scanPrefixFunc()
		if s.Config.ZeroFrozen {
			fixupFunc = s.actionFixUp()
		}
		restoreFunc = s.actionRmDm()
	default:

	}

	if scanFunc != nil {
		if err := s.rtInput.Start(scanFunc); err != nil {
			log.Panicf("can not start scan function err=\"%v\"", err)
		}
	}
	if restoreFunc != nil {
		if err := s.rtRestore.Start(restoreFunc); err != nil {
			log.Panicf("can not start restore function err=\"%v\"", err)
		}
	}
	if fixupFunc != nil {
		if err := s.rtFixup.Start(fixupFunc); err != nil {
			log.Panicf("can not start fixup function err=\"%v\"", err)
		}
	}
}

//
// Restore functions
//

func (s *S3) standardPrefixScan(s3PageFunc func(output *s3.ListObjectVersionsOutput, run bool) bool) func(id *routines.Id, batch []interface{}) {
	svc := s.GetClient()
	scanPrefixFunc := func(id *routines.Id, batch []interface{}) {
		for _, item := range batch {
			prefix, ok := item.(string)
			if !ok {
				log.Printf("ERROR: Expecting a prefix of type string. skipping")
				continue
			}
			input := &s3.ListObjectVersionsInput{
				Bucket: aws.String(s.Config.S3bucket),
				Prefix: aws.String(prefix),
			}
			err := svc.ListObjectVersionsPages(
				input,
				s3PageFunc,
			)
			if err != nil {
				log.Println(err.Error())
			}
		}
	}
	return scanPrefixFunc
}

func (s *S3) scanPrefixFunc() func(id *routines.Id, batch []interface{}) {
	s3PageFunc := func(output *s3.ListObjectVersionsOutput, run bool) bool {
		var buf []interface{}
		logEntries := []*LogVersionEntry{}
		for _, marker := range output.DeleteMarkers {
			if !(s.Config.FromDate.Before(*marker.LastModified) && s.Config.ToDate.After(*marker.LastModified) && *marker.IsLatest) {
				if s.Config.Verbose {
					logEntries = AppendDeleteMarkerEntries("skip", logEntries, []*s3.DeleteMarkerEntry{marker})
				}
				continue
			}

			buf = append(buf, marker)
			if s.Config.Verbose {
				logEntries = AppendDeleteMarkerEntries("submit", logEntries, []*s3.DeleteMarkerEntry{marker})
			}
			if len(buf) >= 24 {
				s.rtRestore.AddJob(buf)
				if s.Config.Verbose {
					LogVersions(logEntries, s.wg)
					logEntries = []*LogVersionEntry{}
				}
				buf = []interface{}{}
			}

		}
		if len(buf) > 0 {
			s.rtRestore.AddJob(buf)
		}
		return true
	}
	scanPrefixFunc := s.standardPrefixScan(s3PageFunc)
	return scanPrefixFunc
}

func (s *S3) actionRmDm() func(id *routines.Id, batch []interface{}) {
	client := s.GetClient()
	removeDmFunc := func(id *routines.Id, batch []interface{}) {
		batchid := Genuuid()
		restoreList := []*s3.ObjectIdentifier{}
		for _, item := range batch {
			if len(batch) == 0 {
				return
			}
			marker, ok := item.(*s3.DeleteMarkerEntry)
			if !ok {
				log.Printf("ERROR: Expecting type *s3.DeleteMarkerEntry, skipping")
				continue
			}
			obj := s3.ObjectIdentifier{
				Key:       marker.Key,
				VersionId: marker.VersionId,
			}
			restoreList = append(restoreList, &obj)
		}
		deleteOutputs, err := client.DeleteObjects(&s3.DeleteObjectsInput{
			Bucket: &s.Config.S3bucket,
			Delete: &s3.Delete{
				Objects: restoreList,
				Quiet:   aws.Bool(false),
			},
		})
		s.logRestoreResults(err, batchid, deleteOutputs)
		// Reset frozen_in_cluster to 0
		if s.Config.ZeroFrozen {
			for _, obj := range deleteOutputs.Deleted {
				if strings.HasSuffix(*obj.Key, "receipt.json") {
					s.rtFixup.AddJob(*obj.Key)
				}
			}
		}
	}
	return removeDmFunc
}

//
// Fixup
//

func (s *S3) scanFixupFunc() func(id *routines.Id, batch []interface{}) {
	svc := s.GetClient()
	s3PageFunc := func(output *s3.ListObjectsOutput, run bool) bool {
		for _, obj := range output.Contents {
			if !strings.HasSuffix(*obj.Key, "receipt.json") {
				continue
			}
			s.rtFixup.AddJob(*obj.Key)
		}
		return true
	}

	listReceiptJsons := func(id *routines.Id, batch []interface{}) {
		for _, item := range batch {
			prefix, ok := item.(string)
			if !ok {
				log.Printf("ERROR: Expecting a prefix of type string. skipping")
				continue
			}
			input := &s3.ListObjectsInput{
				Bucket: aws.String(s.Config.S3bucket),
				Prefix: aws.String(prefix),
			}
			err := svc.ListObjectsPages(
				input,
				s3PageFunc,
			)
			if err != nil {
				log.Println(err.Error())
			}
		}
	}

	return listReceiptJsons
}

func (s *S3) actionFixUp() func(id *routines.Id, batch []interface{}) {
	svc := s.GetClient()
	savedir := "/tmp/splunks3restore/fixups"
	bkupprefix := time.Now().Format("20060102150405")

	fixupFunc := func(id *routines.Id, batch []interface{}) {
		for _, item := range batch {
			key, ok := item.(string)
			if !ok {
				log.Printf("ERROR: Expecting type *s3.ObjectIndentifier, skipping")
				continue
			}
			if !strings.HasSuffix(key, "/receipt.json") {
				log.Printf("skip fixup key %s is not a 'receipt.json' file", key)
				continue
			}

			// Download File
			fpath := filepath.Join(savedir, key)
			dpath := filepath.Dir(fpath)
			err := os.MkdirAll(dpath, os.ModePerm)
			ChkErr(err, Epanicf)
			fh, err := os.OpenFile(fpath, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, os.ModePerm)
			ChkErr(err, Epanicf)

			objInput := &s3.GetObjectInput{
				Bucket: aws.String(s.Config.S3bucket),
				Key:    aws.String(key),
			}
			output, err := svc.GetObject(objInput)
			if err != nil {
				log.Printf("restore action=fixup pid=%d status=error msg=\"download error\" err=\"%s\"\n", s.State.Pid(), err.Error())
				continue
			}

			buf := []byte{}
			for {
				b := make([]byte, 4096)
				n, err := output.Body.Read(b)
				buf = append(buf, b[0:n]...)
				if err == io.EOF {
					break
				}
			}
			output.Body.Close()

			_, err = fh.Write(buf)
			if err != nil {
				fh.Close()
				log.Printf("restore action=fixup pid=%d status=error msg=\"file error\" err=\"%s\"\n", s.State.Pid(), err.Error())
				continue
			}

			err = fh.Close()
			if err != nil {
				log.Printf("restore action=fixup pid=%d status=error msg=\"file error\" err=\"%s\"\n", s.State.Pid(), err.Error())
				continue
			}

			// Fix file
			var fixed bool
			if s.Config.ZeroFrozen {
				fixed, err = s.ResetFrozenInCluster(fpath)
			} else {
				fixed, err = s.FixupReceiptJsonHash(fpath)
			}
			if err != nil {
				log.Printf("restore action=fixup pid=%d status=err msg=\"error reseting frozen in cluster\"", s.State.Pid())
				continue
			}

			// Upload
			if fixed {
				backup := strings.Join([]string{key, bkupprefix}, ".")
				log.Printf("restore action=fixup pid=%d status=info msg=\"creating a remote backup\" backup=%s", s.State.Pid(), backup)
				err := s.BackUpKeyS3(svc, key, backup)
				if err != nil {
					log.Printf("restore action=fixup pid=%d status=error msg=\"can not create a backup of %s, skipping restore\": %v", s.State.Pid(), key, err)
					continue
				}
				err = s.UploadToS3(svc, fpath, key)
				if err == nil {
					log.Printf("restore action=fixup pid=%d status=ok msg=\"uploaded to s3\" key=%s file=%s", s.State.Pid(), key, fpath)
				} else {
					log.Printf("restore action=fixup pid=%d status=error msg=\"error uploading to s3\" err=\"%s\" key=%s file=%s", s.State.Pid(), err.Error(), key, fpath)
				}
			}
		}
	}
	return fixupFunc
}

func (s *S3) ResetFrozenInCluster(fpath string) (bool, error) {
	rcpt := receipt.New(fpath)
	if rcpt.CheckFrozenInCluster() {
		_, err := rcpt.ZeroFrozenInCluster(true)
		if err != nil {
			log.Printf("restore action=resetfrozenincluster pid=%d msg=\"error setting frozen_in_cluster to 0\": %v", s.State.Pid(), err)
			return false, err
		}
		log.Printf("restore action=resetfrozenincluster pid=%d msg=\"reset frozen_in_cluster to 0\"", s.State.Pid())
		return true, nil
	}
	return false, nil
}

func (s *S3) FixupReceiptJsonHash(fpath string) (bool, error) {
	fixed := false
	rcpt := receipt.New(fpath)
	if !rcpt.HashesMatch() {
		log.Printf("restore action=fixup pid=%d hash=invalid msg=\"invalid hash, fixing\" file=%s", s.State.Pid(), fpath)
		_, err := rcpt.ZeroFrozenInCluster(true)
		if err != nil {
			return fixed, err
		} else {
			fixed = true
			log.Printf("restore action=fixup pid=%d hash=fixed msg=\"staging fixed hash file\" file=%s", s.State.Pid(), fpath)
			return fixed, nil
		}
	}
	return fixed, nil
}

//
// Dry functions
//

func (s *S3) scanDryFunc() func(id *routines.Id, batch []interface{}) {
	s3PageFunc := func(output *s3.ListObjectVersionsOutput, run bool) bool {
		batchid := Genuuid()
		for _, marker := range output.DeleteMarkers {
			if !(s.Config.FromDate.Before(*marker.LastModified) && s.Config.ToDate.After(*marker.LastModified) && *marker.IsLatest) {
				continue
			}
			log.Printf(
				"restore action=dryrun status=ok batchid=%s pid=%d key=%s version=%s lastmodified=\"%s\"\n",
				batchid, State.Pid(), *marker.Key, *marker.VersionId, *marker.LastModified,
			)
		}
		return true
	}
	scanPrefixFunc := s.standardPrefixScan(s3PageFunc)
	return scanPrefixFunc
}

//
// ListVer functions
//

func (s *S3) scanListVer() func(id *routines.Id, batch []interface{}) {
	muList := &sync.Mutex{}
	var listFile *os.File
	s3PageFunc := func(output *s3.ListObjectVersionsOutput, run bool) bool {
		muList.Lock()
		defer muList.Unlock()
		for _, ver := range output.Versions {
			if !(s.Config.FromDate.Before(*ver.LastModified) && s.Config.ToDate.After(*ver.LastModified) && *ver.IsLatest) {
				continue
			}
			output := fmt.Sprintf("key=%s version=%s latest=%t deletemarker=false", *ver.Key, *ver.VersionId, *ver.IsLatest)
			if listFile != nil {
				listFile.WriteString(output + "\n")
			} else {
				os.Stdout.WriteString(output + "\n")
			}
		}
		for _, dm := range output.DeleteMarkers {
			if !(s.Config.FromDate.Before(*dm.LastModified) && s.Config.ToDate.After(*dm.LastModified) && *dm.IsLatest) {
				continue
			}
			output := fmt.Sprintf("key=%s version=%s latest=%t deletemarker=true", *dm.Key, *dm.VersionId, *dm.IsLatest)
			if listFile != nil {
				listFile.WriteString(output + "\n")
			} else {
				os.Stdout.WriteString(output + "\n")
			}
		}
		return true
	}
	scanPrefixFunc := s.standardPrefixScan(s3PageFunc)
	return scanPrefixFunc
}

//
// Fixup
//

//
// Audit functions
//

func (s *S3) scanAuditFunc() func(id *routines.Id, batch []interface{}) {
	s3AuditPageFunc := func(output *s3.ListObjectVersionsOutput, run bool) bool {
		entries := []*LogVersionEntry{}
		entries = AppendObjectVersionEntries("audit", entries, output.Versions)
		entries = AppendDeleteMarkerEntries("audit", entries, output.DeleteMarkers)
		LogVersions(entries, s.wg)
		return true
	}
	scanPrefixFunc := s.standardPrefixScan(s3AuditPageFunc)
	return scanPrefixFunc
}

//
// S3 Client/Session
//

func (s *S3) Session() *session.Session {
	region := s.Config.GetBucketRegion()
	config := &aws.Config{
		Region: aws.String(region),
	}
	sess, _ := session.NewSession(config)
	return sess
}

func (s *S3) GetClient() *s3.S3 {
	svc := s3.New(s.Session())
	svc.Handlers.Send.PushBack(func(r *request.Request) {
		AWSRateLimit()
	})
	return svc
}

func (r *S3) GracefulShutdown() {
	r.gracefuldown = true
	r.Kill()
}

//
// Logging
//

func (s *S3) logRestoreResults(err error, batchid string, deleteOutputs *s3.DeleteObjectsOutput) {
	s.wg.Add(1)
	go func() {
		if err != nil {
			log.Printf("restore status=error batchid=%s pid=%d msg=\"%v\"", batchid, State.Pid(), err)
		}
		if deleteOutputs != nil {
			for _, marker := range deleteOutputs.Deleted {
				log.Printf("restore status=ok batchid=%s pid=%d key=%s versionid=%s\n",
					batchid, State.Pid(), *marker.Key, *marker.VersionId)
			}
			for _, marker := range deleteOutputs.Errors {
				log.Printf("retore status=fail batchid=%s pid=%d key=%s versionid=%s error=%v\n",
					batchid, State.Pid(), *marker.Key, *marker.VersionId, *marker.Message)
			}
		}
		s.wg.Add(-1)
	}()
}

func (s *S3) BackUpKeyS3(svc *s3.S3, src, backup string) error {
	copysrc := filepath.Join("/", s.Config.S3bucket, src)
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(s.Config.S3bucket),
		CopySource: aws.String(copysrc),
		Key:        aws.String(backup),
	}
	_, err := svc.CopyObject(input)
	return err
}

// UploadToS3 will upload a single file to S3, it will require a pre-built aws session
// and will set file info like content type and encryption on the uploaded file.
func (s *S3) UploadToS3(svc *s3.S3, fileDir, key string) error {
	// Open the file for use
	file, err := os.Open(fileDir)
	if err != nil {
		return err
	}
	defer file.Close()

	// Get file size and read the file content into a buffer
	fileInfo, _ := file.Stat()
	var size int64 = fileInfo.Size()
	buffer := make([]byte, size)
	file.Read(buffer)

	// Config settings: this is where you choose the bucket, filename, content-type etc.
	// of the file you're uploading.
	_, err = svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.Config.S3bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(buffer),
	})
	return err
}
