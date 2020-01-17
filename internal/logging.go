package internal

import (
	"log"
	"log/syslog"
	"os"
)

func LogToSyslog(tag string) {
	logwriter, e := syslog.New(syslog.LOG_NOTICE, tag)
	if e == nil {
		log.SetOutput(logwriter)
	}
}

func LogToFile(file string) {
	f, err := os.OpenFile(file,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
		return
	}
	log.SetOutput(f)
}
