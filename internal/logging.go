package internal

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"log"
	"log/syslog"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

func LogToSyslog(tag string) {
	logwriter, e := syslog.New(syslog.LOG_NOTICE, tag)
	if e == nil {
		log.SetOutput(logwriter)
	}
}

func LogToFile(lname string) {
	info, err := os.Stat(lname)
	if err != nil && !os.IsNotExist(err) {
		log.Println(err)
		return
	}
	var logpath string
	if info != nil && info.IsDir() {
		logpath = filepath.Join(lname, logName())
	} else {
		logpath = lname
	}
	f, err := os.OpenFile(logpath,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
		return
	}
	log.SetOutput(f)
}

func LogDefault(c *ConfigType) {
	logfile := logName()
	td := os.Getenv("TMPDIR")
	if td == "" {
		td = "/tmp"
	}
	logfile = path.Join(td, logfile)
	file, err := os.OpenFile(logfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Error opening log file: %v\n", err)
		Exit(-1)
	}
	var writer io.Writer
	switch {
	case c.RestoreList:
		writer = file
	default:
		writer = io.MultiWriter(os.Stderr, file)
	}
	log.SetOutput(writer)
	fmt.Fprintf(os.Stderr, "Logs saved in file %s\n", file.Name())
}

func logName() string {
	t := time.Now()
	y := t.Format("2006")
	m := t.Format("01")
	d := t.Format("02")
	h := t.Format("15")
	min := t.Format("04")
	s := t.Format("05")
	logfile := fmt.Sprintf("s2deletemarker-%s%s%s%s%s%s.log", y, m, d, h, min, s)
	return logfile
}

type sortByLogVersionEntry []*logVersionEntry

func (s sortByLogVersionEntry) Len() int {
	return len(s)
}

func (s sortByLogVersionEntry) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortByLogVersionEntry) Less(i, j int) bool {
	if yes := s[i].key != s[j].key; yes {
		return s[i].key < s[j].key
	}
	return s[i].lastmodified.Before(*s[j].lastmodified)
}

type logVersionEntry struct {
	status         string
	key            string
	versionid      string
	lastmodified   *time.Time
	islatest       bool
	isdeletemarker bool
}

func appendDeleteMarkerEntries(status string, entries []*logVersionEntry, markers []*s3.DeleteMarkerEntry) []*logVersionEntry {
	for _, marker := range markers {
		entries = append(entries, &logVersionEntry{
			status:         status,
			key:            *marker.Key,
			versionid:      *marker.VersionId,
			lastmodified:   marker.LastModified,
			islatest:       *marker.IsLatest,
			isdeletemarker: true,
		})
	}
	sort.Sort(sortByLogVersionEntry(entries))
	return entries
}

func appendObjectVersionEntries(status string, entries []*logVersionEntry, objects []*s3.ObjectVersion) []*logVersionEntry {
	for _, obj := range objects {
		entries = append(entries, &logVersionEntry{
			status:         status,
			key:            *obj.Key,
			versionid:      *obj.VersionId,
			lastmodified:   obj.LastModified,
			islatest:       *obj.IsLatest,
			isdeletemarker: false,
		})
	}
	sort.Sort(sortByLogVersionEntry(entries))
	return entries
}

func logVersions(entries []*logVersionEntry, wg *sync.WaitGroup) {
	WaitFunc(
		func() {
			for _, entry := range entries {
				log.Printf(
					"restore status=%s key=%s versionid=%s lastmodified=\"%s\" deletemarker=%t islatest=%t",
					entry.status,
					entry.key,
					entry.versionid,
					entry.lastmodified,
					entry.isdeletemarker,
					entry.islatest,
				)
			}
		},
		wg,
	)
}
