# splunks3restore

A command line utility to recover Splunk buckets store in S3

# Isn't there other tools to do this already?

This code implements concurrency and is blazing fast.
The other tools will perform 600 S3 object restores per minute or 10 per
second, splunks3restore is capable of restores of over 1000 objects per
second. Note that by default the tool is rate limited to 256 S3 calls
per second, rate limiting is crolled by the --rate=<rate> flag.

For a comparison in a test environment it would take over 30 days to
restore 1.5M Splunk buckets using the python tool in a continuous run.
s3deletemarkers scanned and restored 1.5M in under 5 hours.

# Help

*Get command line help*
```bash
splunks3restore --help
```

*Get dateformat help*
```bash
splunks3restore --dateformat
```

# Examples

*Recover _internal and _audit indexes from between 7 & 6 days ago*
```bash
splunks3restore restore --s3bucket=s3bucket --start=-7d --end=-6d mystack _internal _audit
```

*Restore buckets from an input list*
```bash
splunks3restore restore --s3bucket s3-bucket --start -7d --end now --bidfile bidfile.txt stack
```

*Instead of restoring, get a list of Splunk buckets using a prefix file and save to splunkbuckets.txt*
```bash
splunks3restore restore --s3bucket s3-bucket --start -7d --end now --list=splunkbuckets.txt --prefixfile=inputlist.txt stack
```
