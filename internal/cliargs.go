// +build go1.9

package internal

import (
	"github.com/docopt/docopt-go"
	"log"
	"os"
	"strings"
)

var Usage = `Remove Smart Store S3 delete markers

Usage:
    s2deletemarkers restore [--dryrun] [--verbose] [--log=<logfile>] [--logsyslog] [--rate=<actions>] --s3bucket=<s3bucket> --start=<sdate> --end=<edate> <stack> <prefixes>...
    s2deletemarkers restore [--dryrun] [--verbose] [--log=<logfile>] [--logsyslog] [--rate=<actions>] --s3bucket=<s3bucket> --start=<sdate> --end=<edate> --prefixfile=<prefixfile> <stack>
    s2deletemarkers fixup [--log=<logfile>] [--logsyslog] [--rate=<actions>] --s3bucket=<s3bucket> <stack> <prefixes>...
    s2deletemarkers fixup [--log=<logfile>] [--logsyslog] [--rate=<actions>] --s3bucket=<s3bucket> --prefixfile=<prefixfile> <stack>
    s2deletemarkers listver [--log=<logfile>] [--logsyslog] [--rate=<actions] [--output=<listfile>] --s3bucket=<s3bucket> --start=<sdate> --end=<edate> <stack> <prefixes>...
    s2deletemarkers listver [--log=<logfile>] [--logsyslog] [--rate=<actions>] [--output=<listfile>] --s3bucket=<s3bucket> --start=<sdate> --end=<edate> --prefixfile=<prefixfile> <stack>
    s2deletemarkers --dateformat

Options:
    -h --help                     Print help
    -v --version                  Print version
    -f --dateformat               Print date formats
    -d --dryrun                   Run in simulation. Log entries will have a status=dryrun.
    -s --start=<sdate>            Starting date
    -e --end=<edate>              End date
    -l --output=<listfile>        Write bucket list to <listfile>
    -r --region=<region>          Set AWS Region
    -p --prefixfile=<prefixfile>  Load prefixes from a file
    <prefixes>                    list of prefixes (E.G. index names or full paths)
                                  Note that the Stack name is pre-pended to each prefix
    -t --rate=<actions>           Rate limit AWS s3Client calls to <actions> per second.
                                  -1 will disable rate limiting.
                                  0 will set to the default which is 256.
    -u --logsyslog                Log to syslog
    -x --log=<logfile>            Log to a logfile
    -b --verbose                  Verbose logs
`

type OptUsage struct {
	Audit      bool     `docopt:"audit"`
	Continue   bool     `docopt:"--continue"`
	Datehelp   bool     `docopt:"--dateformat"`
	DryRun     bool     `docopt:"--dryrun"`
	Fixup      bool     `docopt:"fixup"`
	Fromdate   string   `docopt:"--start"`
	ListOutput string   `docopt:"--output"`
	Logfile    string   `docopt:"--log"`
	PrefixFile string   `docopt:"--prefixfile"`
	PrefixList []string `docopt:"<prefixes>"`
	RateLimit  float64  `docopt:"--rate"`
	Restore    bool     `docopt:"restore"`
	ListVer    bool     `docopt:"listver"`
	S3bucket   string   `docopt:"--s3bucket"`
	Stack      string   `docopt:"<stack>"`
	Syslog     bool     `docopt:"--logsyslog"`
	Todate     string   `docopt:"--end"`
	Verbose    bool     `dcopt:"--verbose"`
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
