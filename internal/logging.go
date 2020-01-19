package internal

import (
	"fmt"
	"io"
	"log"
	"log/syslog"
	"os"
	"path"
	"path/filepath"
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
