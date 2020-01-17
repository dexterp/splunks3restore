package internal

import (
	"fmt"
	"github.com/docopt/docopt-go"
	"os"
)

var Usage = `Remove Smart Store S3 delete markers

Usage:
    s2deletemarkers [--dryrun] [--log=<logfile>] [--logsyslog] --s3bucket=<s3bucket> --start=<sdate> --end=<edate> <Stack> <prefixes>...
    s2deletemarkers [--dryrun] [--log=<logfile>] [--logsyslog] [--list=<listfile>] --s3bucket=<s3bucket> --start=<sdate> --end=<edate> --prefixfile=<prefixfile> <Stack>
    s2deletemarkers --dateformat

Runner:
    -h --help                     Print help
    -v --version                  Print version
    -f --dateformat               Print date formats
    -d --dryrun                   Run in simulation. Log entries will have a status=dryrun.
    -s --start=<sdate>            Starting date
    -e --end=<edate>              End date
    -l --list=<listfile>          List buckets only and write to <listfile>
    -r --Region=<Region>          Set AWS Region
    -p --prefixfile=<prefixfile>  Load prefixes from a file
    <prefixes>                    list of prefixes (E.G. index names or full paths)
                                  Note that the Stack name is pre-pended to each prefix
    --logsyslog                   Log to syslog
    -x --log=<logfile>            Log to a logfile
`

type OptUsage struct {
	Fromdate   string   `docopt:"--start"`
	Todate     string   `docopt:"--end"`
	Stack      string   `docopt:"<Stack>"`
	PrefixList []string `docopt:"<prefixes>"`
	PrefixFile string   `docopt:"--prefixfile"`
	Continue   bool     `docopt:"--continue"`
	Datehelp   bool     `docopt:"--dateformat"`
	Drunrun    bool     `docopt:"--dryrun"`
	S3bucket   string   `docopt:"--s3bucket"`
	Syslog     bool     `docopt:"--logsyslog"`
	Logfile    string   `docopt:"--log"`
	List       string   `docopt:"--list"`
}

func GetUsage(args []string) *Runner {
	opts, err := docopt.ParseArgs(Usage, args, Version)
	ChkErr(err, Efatalf, "Can not parse options doc: %s", err) // nolint
	options := &OptUsage{}
	err = opts.Bind(options)
	ChkErr(err, Efatalf, "Can not parse options doc: %s", err) // nolint
	from, err := ParseTime(options.Fromdate)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unrecognised <fromdate> format %s", options.Fromdate)
	}
	to, err := ParseTime(options.Todate)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unrecognised <todate> format %s", options.Todate)
	}
	runner := &Runner{
		Fromdate:   from,
		Todate:     to,
		PrefixList: options.PrefixList,
		PrefixFile: options.PrefixFile,
		S3bucket:   options.S3bucket,
		Stack:      options.Stack,
		Datehelp:   options.Datehelp,
		Dryrun:     options.Drunrun,
		Continue:   options.Continue,
		List:       options.List,
		Logfile:    options.Logfile,
	}
	if options.S3bucket != "" {
		runner.Region = BucketLocation(options.S3bucket)
	}
	return runner
}
