# s2deletemarkers

A command line utility to recover Splunk buckets store in S3

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