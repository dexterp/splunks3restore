package internal

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

type Runner struct {
	Fromdate   time.Time
	Todate     time.Time
	sync       *sync.Mutex
	sigTrap    *os.Signal
	Region     string
	S3bucket   string
	Stack      string
	PrefixList []string
	PrefixFile string
	Continue   bool
	Datehelp   bool
	Dryrun     bool
	Syslog     bool
	Logfile    string
	List       string
}

func (r *Runner) Run(trapC <-chan os.Signal) {
	r.runDatehelp(false)

	Pid = os.Getpid()
	r.SetupLogging()
	r.sync = &sync.Mutex{}
	r.installSigHandlers(trapC)

	r.runIndexRecovery(false)
	r.runPrefixRecovery(false)
}

func (r *Runner) installSigHandlers(trapC <-chan os.Signal) {
	go func() {
		sig, _ := <-trapC
		log.Printf("restore pid=%d msg=\"received %s signal, shutting down\"\n", Pid, sig.String())
		r.sync.Lock()
		r.sync.Unlock()
		r.sigTrap = &sig
	}()
}

func (r *Runner) SetupLogging() {
	switch {
	case r.Logfile != "":
		LogToFile(r.Logfile)
	case r.Syslog:
		LogToSyslog("s2deletemarkers")
	default:
		t := time.Now()
		y := t.Format("2006")
		m := t.Format("01")
		d := t.Format("02")
		h := t.Format("15")
		min := t.Format("04")
		s := t.Format("05")
		logfile := fmt.Sprintf("/tmp/s2deletemarker-%s%s%s%s%s%s.log", y, m, d, h, min, s)
		td := os.Getenv("TMPDIR")
		if td == "" {
			td = "/tmp"
		}
		file, err := os.OpenFile(logfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("Error opening log file: %v\n", err)
			Exit(-1)
		}
		if r.List != "" {
			log.SetOutput(file)
		} else {
			writer := io.MultiWriter(os.Stdout, file)
			log.SetOutput(writer)
		}
		fmt.Fprintf(os.Stderr, "Logs saved in file %s\n", file.Name())
	}
}

func (r *Runner) runDatehelp(force bool) {
	if !r.Datehelp && !force {
		return
	}
	fmt.Print(`Format examples:
2006-01-02T15:04:05-0700
2006-01-02T15:04:05
2006-01-02
15:04:05
15:04
now
now-1h
now-2d-1h-3m
-300m
-20d
-1s
`)
	Exit(0)
}

func (r *Runner) runIndexRecovery(force bool) {
	if len(r.PrefixList) <= 0 && !force {
		return
	}
	log.Printf("restore status=start pid=%d\n", Pid)
	s3runner := New(
		r.S3bucket,
		r.Region,
		r.Stack,
		16,
		64,
		false,
		r.Dryrun,
		r.List,
	)
	s3runner.RemoveDeleteMarkers(r.PrefixList, r.Fromdate, r.Todate)
	log.Printf("restore status=end pid=%d\n", Pid)
	Exit(0)
}

func (r *Runner) runPrefixRecovery(force bool) {
	if r.PrefixFile == "" && !force {
		return
	}
	log.Printf("restore status=start pid=%d\n", Pid)
	file, err := os.Open(r.PrefixFile)
	if err != nil {
		log.Printf("Can not open prefix file %s: %v", r.PrefixFile, err)
		Exit(-1)
	}
	reader := bufio.NewReader(file)
	s3runner := New(
		r.S3bucket,
		r.Region,
		r.Stack,
		64,
		64,
		r.Continue,
		r.Dryrun,
		r.List,
	)
	buf := []string{}
	waitFunc := func() {
		s3runner.SetGracefulDown()
		s3runner.Wait()
	}
	fn := func() {
		r.CheckGracefulShutdown(waitFunc)
		s3runner.RemoveDeleteMarkers(buf, r.Fromdate, r.Todate)
		buf = []string{}
	}
	for {
		line, err := reader.ReadString('\n')
		line = strings.Trim(line, "\n")
		if err != nil && err.Error() == "EOF" {
			buf = append(buf, line)
			break
		} else if err != nil {
			log.Printf("ERROR: %v", err)
			break
		}
		buf = append(buf, line)
		if len(buf) >= 256 {
			fn()
		}
	}
	if len(buf) > 0 {
		fn()
	}
	log.Printf("restore status=end pid=%d\n", Pid)
	Exit(0)
}

func (r *Runner) CheckGracefulShutdown(waitfunc func()) {
	r.sync.Lock()
	defer r.sync.Unlock()
	if r.sigTrap != nil {
		log.Printf("restore pid=%d msg=\"shutdown down complete\"", Pid)
		waitfunc()
		Exit(0)
	}
}
