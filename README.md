# s2deletemarkers

A command line utility to recover Splunk buckets store in S3

# Isn't there other tools to do this already?

This code implements concurrency and is blazing fast.
The other tools will perform 600 S3 object restores per minute or 10 per
second, s2deletemarkers restores 144 objects per second.

For a comparison in a test environment it would take over 30 days to
restore 1.5M Splunk buckets using the python tool in a continuous run.
s3deletemarkers will restore in under 40 hours.

# Help

*Get command line help*
```bash
s2deletemarkers --help
```

*Get dateformat help*
```bash
s2deletemarkers --dateformat
```

# Examples

*Recover _internal and _audit indexes from between 7 & 6 days ago*
```bash
s2deletemarkers --s3bucket=s3bucket --start=-7d --end=-6d mystack _internal _audit
```

*Restore buckets from an input list*
```bash
s2deletemarkers --s3bucket s3-bucket --start -7d --end now --prefixfile=inputlist.txt stack
```

*Instead of restoring, get a list of Splunk buckets using a prefix file and save to splunkbuckets.txt*
```bash
s2deletemarkers --s3bucket s3-bucket --start -7d --end now --list=splunkbuckets.txt --prefixfile=inputlist.txt stack
```