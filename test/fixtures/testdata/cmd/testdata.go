package main

import (
	"crypto/sha1"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

var putC chan *s3.PutObjectInput
var wait *sync.WaitGroup

const BUCKETNAME = "undeletemarkerstest2"
const PATH = "stacksdeletemarker"
const SLUNKBUCKETCNT = 10000

func main() {
	basepath := "/tmp/_splunks3restoretest"
	region := "us-west-2"
	wait = &sync.WaitGroup{}
	putC = make(chan *s3.PutObjectInput, 256)
	if _, err := os.Stat(basepath); err != nil && os.IsNotExist(err) {
		os.Mkdir(basepath, os.ModePerm)
		generateFiles(basepath, SLUNKBUCKETCNT)
	}
	sess := Session(region)
	startUploadRoutines(64, sess)
	createBucket(sess, BUCKETNAME)
	uploadDirToS3(BUCKETNAME, basepath, PATH)
}

func Session(region string) *session.Session {
	config := &aws.Config{
		Region: aws.String(region),
	}
	sess, _ := session.NewSession(config)
	return sess
}

func generateFiles(basepath string, filecount int) {
	fpath := filepath.Join(basepath, "bidlist.txt")
	write, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	for i := 0; i < filecount; i++ {
		touchfiles(basepath, gensplunkbucket(write))
	}
	write.Close()
}

func touchfiles(basepath string, files []string) {
	for _, f := range files {
		fullpath := filepath.Join(basepath, f)
		dirpath := filepath.Dir(fullpath)
		os.MkdirAll(dirpath, os.ModePerm)
		_, _ = os.Create(fullpath)
	}
}

func gensplunkbucket(outfile *os.File) []string {
	idx := genindex()
	bucketid := genuuid()
	hashdir := hashdirs(bucketid)
	bid := strings.Join([]string{idx, bucketid}, "~")
	outfile.WriteString(fmt.Sprintf("%s\n", bid))
	path := filepath.Join(PATH, idx, "db", hashdir, bucketid, "guidSplunk"+bucketid)
	fmt.Printf("%s\n", path)
	files := []string{
		"1578448903-1578448514-5343359168661492325.tsidx",
		"Hosts.data",
		"SourceTypes.data",
		"Sources.data",
		"Strings.data",
		"bloomfilter",
		"bucket_info.csv",
		"metadata.csv",
		"metadata_checksum",
		"rawdata/journal.gz",
		"rawdata/slicemin.dat",
		"rawdata/slicesv2.dat",
		"splunk-autogen-params.dat",
	}
	paths := []string{}
	for _, f := range files {
		paths = append(paths, filepath.Join(path, f))
	}
	return paths
}

func hashdirs(input string) string {
	h := sha1.New()
	io.WriteString(h, input)
	hash := fmt.Sprintf("%x", h.Sum(nil))
	hash = strings.ToUpper(hash)
	runes := []rune(hash)
	d1 := string(runes[0]) + string(runes[1])
	d2 := string(runes[2]) + string(runes[3])
	dirs := filepath.Join(d1, d2)
	return dirs
}

func genuuid() string {
	rand.Seed(time.Now().UnixNano())
	i := rand.Intn(15) + 1
	u := uuid.New()
	id := strings.Join([]string{strconv.Itoa(i), "~", u.String()}, "")
	id = strings.ToUpper(id)
	return id
}

func genindex() string {
	indexes := []string{
		"_internal",
		"_audit",
		"main",
		"web",
		"logs",
		"syslog",
	}

	max := len(indexes) - 1
	item := rand.Intn(max)
	return indexes[item]
}

func isDirectory(path string) bool {
	fd, err := os.Stat(path)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
	switch mode := fd.Mode(); {
	case mode.IsDir():
		return true
	case mode.IsRegular():
		return false
	}
	return false
}

func uploadDirToS3(bucketName string, basepath string, dirPath string) {
	wd, err := os.Getwd()
	if err != nil {
		log.Println("ERROR: Could not get working director")
		return
	}
	if err := os.Chdir(basepath); err != nil {
		log.Printf("ERROR: Could not change to basepath %s: %v\n", basepath, err)
	}

	filepath.Walk(dirPath, func(path string, f os.FileInfo, err error) error {
		if isDirectory(path) {
			// Do nothing
			return nil
		} else {
			uploadFileToS3(bucketName, basepath, path)
			return nil
		}
	})

	if err := os.Chdir(wd); err != nil {
		log.Printf("ERROR: Could not change to working directory %s: %v\n", wd, err)
	}
}

func createBucket(sess *session.Session, bucketname string) {
	svc := s3.New(sess)
	input := &s3.CreateBucketInput{
		Bucket: aws.String(bucketname),
	}

	result, err := svc.CreateBucket(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeBucketAlreadyExists:
				log.Println(s3.ErrCodeBucketAlreadyExists, aerr.Error())
			case s3.ErrCodeBucketAlreadyOwnedByYou:
				log.Println(s3.ErrCodeBucketAlreadyOwnedByYou, aerr.Error())
			default:
				log.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			log.Println(err.Error())
		}
		return
	}

	fmt.Println(result)

}

func uploadFileToS3(bucketName string, basepath string, filePath string) {
	wd, err := os.Getwd()
	if err != nil {
		log.Println("ERROR: Could not get working director")
		return
	}
	if err := os.Chdir(basepath); err != nil {
		log.Printf("ERROR: Could not change to basepath %s: %v\n", basepath, err)
	}
	key := filePath

	// Upload the file to the s3 given bucket
	contentDisp := fmt.Sprintf("attachment; filename=\"%s\"", filePath)
	params := &s3.PutObjectInput{
		Bucket:             aws.String(bucketName), // Required
		Key:                aws.String(key),        // Required
		ContentDisposition: &contentDisp,
	}
	putC <- params

	if err := os.Chdir(wd); err != nil {
		log.Printf("ERROR: Could not change to working directory %s: %v\n", wd, err)
	}
}

func startUploadRoutines(routines int, sess *session.Session) {
	for i := 0; i < routines; i++ {
		go func() {
			s3Svc := s3.New(sess)
			for {
				putinput := <-putC
				log.Println("upload " + *putinput.Key + " to S3")
				_, err := s3Svc.PutObject(putinput)
				if err != nil {
					log.Printf("Failed to upload data to %s/%s, %s\n",
						*putinput.Bucket, *putinput.Key, err.Error())
					return
				}
			}
		}()
	}
}
