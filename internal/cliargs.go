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
    s2deletemarkers restore [--dryrun] [--log=<logfile>] [--logsyslog] [--rate=<actions>] --s3bucket=<s3bucket> --start=<sdate> --end=<edate> <stack> <prefixes>...
    s2deletemarkers restore [--dryrun] [--log=<logfile>] [--logsyslog] [--rate=<actions>] --s3bucket=<s3bucket> --start=<sdate> --end=<edate> --prefixfile=<prefixfile> <stack>
    s2deletemarkers list [--log=<logfile>] [--logsyslog] [--rate=<actions] [--output=<listfile>] --s3bucket=<s3bucket> --start=<sdate> --end=<edate> <stack> <prefixes>...
    s2deletemarkers list [--log=<logfile>] [--logsyslog] [--rate=<actions>] [--output=<listfile>] --s3bucket=<s3bucket> --start=<sdate> --end=<edate> --prefixfile=<prefixfile> <stack>
    s2deletemarkers --dateformat

Options:
    -h --help                     Print help
    -v --version                  Print version
    -f --dateformat               Print date formats
    -d --dryrun                   Run in simulation. Log entries will have a status=dryrun.
    -s --start=<sdate>            Starting date
    -e --end=<edate>              End date
    -l --output=<listfile>        ListOutput buckets only and write to <listfile>
    -r --region=<region>          Set AWS Region
    -p --prefixfile=<prefixfile>  Load prefixes from a file
    <prefixes>                    list of prefixes (E.G. index names or full paths)
                                  Note that the Stack name is pre-pended to each prefix
    -t --rate=<actions>           Rate limit AWS client calls to <actions> per second.
                                  -1 will disable rate limiting.
                                  0 will set to the default which is 256.
    -u --logsyslog                Log to syslog
    -x --log=<logfile>            Log to a logfile
`

type OptUsage struct {
	Restore     bool     `docopt:"restore"`
	RestoreList bool     `docopt:"list"`
	Fromdate    string   `docopt:"--start"`
	Todate      string   `docopt:"--end"`
	Stack       string   `docopt:"<stack>"`
	PrefixList  []string `docopt:"<prefixes>"`
	PrefixFile  string   `docopt:"--prefixfile"`
	Continue    bool     `docopt:"--continue"`
	Datehelp    bool     `docopt:"--dateformat"`
	DryRun      bool     `docopt:"--dryrun"`
	S3bucket    string   `docopt:"--s3bucket"`
	Syslog      bool     `docopt:"--logsyslog"`
	Logfile     string   `docopt:"--log"`
	ListOutput  string   `docopt:"--output"`
	RateLimit   float64  `docopt:"--rate"`
}

func GetUsage(args []string) *Runner {
	log.SetOutput(os.Stderr)
	opts, err := docopt.ParseArgs(Usage, args, Version)
	ChkErr(err, Efatalf, "Can not parse options doc: %s", err) // nolint
	options := &OptUsage{}
	err = opts.Bind(options)
	ChkErr(err, Efatalf, "Can not parse options doc: %s", err) // nolint
	Config.Load(options)
	runner := &Runner{
		Config: &Config,
	}
	return runner
}

func Cli2Sting() string {
	return strings.Join(os.Args, " ")
}
