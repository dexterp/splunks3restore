// +build go1.9

package internal

import (
	"github.com/docopt/docopt-go"
	"log"
	"os"
	"strings"
)

var Usage = `Restore Splunk files stored on S3 

Usage:
    splunks3restore restore [--verbose] [--log=<logfile>] [--logsyslog] [--rate=<actions>] [--start=<sdate>] [--end=<edate>] --s3bucket=<s3bucket> [--path=<path>] <bucketid>...
    splunks3restore restore [--verbose] [--log=<logfile>] [--logsyslog] [--rate=<actions>] [--start=<sdate>] [--end=<edate>] --s3bucket=<s3bucket> [--path=<path>] --bucketids=<bucketids>
    splunks3restore listver [--verbose] [--rate=<actions>] [--start=<sdate>] [--end=<edate>] --s3bucket=<s3bucket> [--path=<path>] <bucketid>...
    splunks3restore listver [--verbose] [--rate=<actions>] [--start=<sdate>] [--end=<edate>] --s3bucket=<s3bucket> [--path=<path>] --bucketids=<bucketids>
    splunks3restore --dateformat

Options:
    -h --help                           Print help
    -v --version                        Print version
    -f --dateformat                     Print help on date formats
    -b --bucketids=<bucketids>          File containing a list of bucket ids
    -p --path=<path>                    Optional path to bucket location
    <bucketid>                          Splunk bucket id(s)
    -r --rate=<actions>                 Rate limit AWS s3Client calls to <actions> per second.
                                        -1 will disable rate limiting.
                                        0 will set to the default which is 256.
    -s --logsyslog                      Log to syslog
    -l --log=<logfile>                  Log to a logfile
    --verbose                           Verbose output
    -b --start=<sdate>                  Start date
    -e --end=<edate>                    End date
`

type OptUsage struct {
	Restore       bool     `docopt:"restore"`
	ListVer       bool     `docopt:"listver"`
	Path          string   `docopt:"--path"`
	BucketIdsFile string   `docopt:"--bucketids"`
	BucketIds     []string `docopt:"<bucketid>"`
	Datehelp      bool     `docopt:"--dateformat"`
	Verbose       bool     `docopt:"--verbose"`
	Logfile       string   `docopt:"--log"`
	RateLimit     float64  `docopt:"--rate"`
	S3bucket      string   `docopt:"--s3bucket"`
	Syslog        bool     `docopt:"--logsyslog"`
	Fromdate      string   `docopt:"--start"`
	Todate        string   `docopt:"--end"`
}

func GetUsage(args []string, version string) *Runner {
	log.SetOutput(os.Stderr)
	opts, err := docopt.ParseArgs(Usage, args, version)
	ChkErr(err, Efatalf, "Can not parse options doc: %s", err) // nolint
	options := &OptUsage{}
	err = opts.Bind(options)
	ChkErr(err, Efatalf, "Can not parse options doc: %s", err) // nolint
	Config.Load(options)
	runner := &Runner{
		Config: &Config,
		State:  &State,
	}
	return runner
}

func Cli2Sting() string {
	return strings.Join(os.Args, " ")
}
