package internal

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"sync"
)

type Runner struct {
	Config   *ConfigType
	State    *StateStruct
	sync     *sync.Mutex
	sigTrap  *os.Signal
	s3Client *S3
}

func (r *Runner) Run(trapC <-chan os.Signal) {
	r.Setup()
	r.runDatehelp(false)

	r.SetupLogging()
	r.installSigHandlers(trapC)

	r.runList(false)
	r.runRecovery(false)
}

func (r *Runner) Setup() {
	SetupAWSRateLimit(AWSDefaultRate)
	r.sync = &sync.Mutex{}
	r.s3Client = NewS3client(&Config, &State)
}

func (r *Runner) installSigHandlers(trapC <-chan os.Signal) {
	go func() {
		sig, _ := <-trapC
		log.Printf("restore pid=%d msg=\"received %s signal, shutting down\"\n", r.State.Pid(), sig.String())
		r.sync.Lock()
		defer r.sync.Unlock()
		r.s3Client.GracefulShutdown()
		r.sigTrap = &sig
	}()
}

func (r *Runner) SetupLogging() {
	switch {
	case r.Config.LogFile != "":
		LogToFile(r.Config.LogFile)
	case r.Config.Syslog:
		LogToSyslog("splunks3restore")
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

func (r *Runner) runList(force bool) {
	if !r.Config.ListVer && !force {
		return
	}
	log.Printf("restore status=start pid=%d cli=\"%s\"\n", r.State.Pid(), Cli2Sting())
	r.s3Client.StartWorkers()
	r.iterMain()
	r.s3Client.Shutdown()
	log.Printf("restore status=end pid=%d\n", r.State.Pid())
	Exit(0)
}

func (r *Runner) runRecovery(force bool) {
	if !r.Config.Restore && !force {
		return
	}
	action := "recover"
	log.Printf("restore action=%s status=start pid=%d cli=\"%s\"\n", action, r.State.Pid(), Cli2Sting())
	r.s3Client.StartWorkers()
	r.iterMain()
	r.s3Client.Shutdown()

	log.Printf("restore action=%s status=end pid=%d\n", action, r.State.Pid())
	Exit(0)
}

func (r *Runner) prefixReader() *bufio.Reader {
	file, err := os.Open(r.Config.BucketIdsFile)
	if err != nil {
		log.Printf("Can not open prefix file %s: %v", r.Config.BucketIdsFile, err)
		Exit(-1)
	}
	reader := bufio.NewReader(file)
	return reader
}

func (r *Runner) iterMain() {
	switch {
	case r.Config.BucketIdsFile != "":
		r.iterFile()
	case len(r.Config.BucketIds) > 0:
		r.iterList()
	}
}

func bid2prefix(pth string, bid string) (string, error) {
	itm, err := bid2path(bid)
	if err != nil {
		return "", err
	}
	prefix := strings.Join([]string{pth, itm}, "/")
	prefix = path.Clean(prefix)
	return prefix, nil
}

func (r *Runner) iterList() {
	for _, bid := range r.Config.BucketIds {
		if r.sigTrap != nil {
			break
		}
		prefix, err := bid2prefix(r.Config.Path, bid)
		if err != nil {
			log.Printf("Bucket ID format error: '%v' skipping '%s'", bid, err)
			continue
		}
		if err := r.s3Client.ScanPrefix(prefix); err != nil {
			log.Printf("exiting error recieved: %v", err)
		}
	}
}

func (r *Runner) iterFile() {
	ioreader := r.prefixReader()
	for {
		if r.sigTrap != nil {
			break
		}
		bid, err := ioreader.ReadString('\n')
		bid = strings.Trim(bid, "\n")
		if err != nil && err.Error() == "EOF" {
			break
		} else if err != nil {
			log.Printf("ERROR: %v", err)
			break
		}
		prefix, err := bid2prefix(r.Config.Path, bid)
		if err != nil {
			log.Printf("Bucket ID format error: '%v' skipping '%s'", bid, err)
			continue
		}
		if r.Config.Verbose {
			log.Printf("restore scanning bid=%s prefix=%s pid=%d\n", bid, prefix, r.State.Pid())
		}
		if err := r.s3Client.ScanPrefix(prefix); err != nil {
			log.Printf("exiting error recieved: %v", err)
		}
	}
}
