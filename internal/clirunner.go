package internal

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
)

type Runner struct {
	Config  *ConfigType
	sync    *sync.Mutex
	sigTrap *os.Signal
}

func (r *Runner) Run(trapC <-chan os.Signal) {
	r.Setup()
	r.runDatehelp(false)

	r.SetupLogging()
	r.installSigHandlers(trapC)

	r.runIndexRecovery(false)
	r.runPrefixRecovery(false)
}

func (r *Runner) Setup() {
	Pid = os.Getpid()
	SetupAWSRateLimit(AWSDefaultRate)
	r.sync = &sync.Mutex{}
}

func (r *Runner) installSigHandlers(trapC <-chan os.Signal) {
	go func() {
		sig, _ := <-trapC
		log.Printf("restore pid=%d msg=\"received %s signal, shutting down\"\n", Pid, sig.String())
		r.sync.Lock()
		defer r.sync.Unlock()
		r.sigTrap = &sig
	}()
}

func (r *Runner) SetupLogging() {
	switch {
	case r.Config.LogFile != "":
		LogToFile(r.Config.LogFile)
	case r.Config.Syslog:
		LogToSyslog("s2deletemarkers")
	default:
		LogDefault(r.Config)
	}
}

func (r *Runner) runDatehelp(force bool) {
	if !r.Config.DateHelp && !force {
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
	if len(r.Config.PrefixList) <= 0 && !force {
		return
	}
	log.Printf("restore status=start pid=%d cli=\"%s\"\n", Pid, Cli2Sting())
	s3runner := New(
		&Config,
		16,
		64,
	)
	s3runner.ProcessDeleteMarkers(r.Config.PrefixList, r.Config.FromDate, r.Config.ToDate)
	log.Printf("restore status=end pid=%d\n", Pid)
	Exit(0)
}

func (r *Runner) runPrefixRecovery(force bool) {
	if r.Config.PrefixFile == "" && !force {
		return
	}
	log.Printf("restore status=start pid=%d cli=\"%s\"\n", Pid, Cli2Sting())
	s3runner := New(
		&Config,
		64,
		64,
	)
	buf := []string{}
	waitFunc := func() {
		s3runner.SetGracefulDown()
		s3runner.Wait()
	}
	fn := func() {
		r.CheckGracefulShutdown(waitFunc)
		s3runner.ProcessDeleteMarkers(buf, r.Config.FromDate, r.Config.ToDate)
		buf = []string{}
	}
	reader := r.PrefixReader()
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
		if r.sigTrap != nil {
			waitFunc()
			return
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

func (r *Runner) PrefixReader() *bufio.Reader {
	file, err := os.Open(r.Config.PrefixFile)
	if err != nil {
		log.Printf("Can not open prefix file %s: %v", r.Config.PrefixFile, err)
		Exit(-1)
	}
	reader := bufio.NewReader(file)
	return reader
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
