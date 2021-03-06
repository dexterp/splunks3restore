# splunks3restore

A command line utility to recover Splunk buckets store in S3

This code implements concurrency and is blazing fast.
A single threaded boto3 script will restore 600 S3 object restores per minute
or 10 per second, splunks3restore is capable of restores of over 1000 objects
per second. For a comparison in a test environment it would take over 30 days
to restore 1.5M Splunk buckets using a single threaded python script in a
continuous run. s3deletemarkers scanned and restored 1.5M in under 5 hours.

Note that by default the tool is rate limited to 256 S3 calls
per second, rate limiting is crolled by the --rate=<rate> flag.

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
splunks3restore restore --s3bucket=s3bucket --start=-7d --end=-6d _internal~15~55B6B1CA-07FB-416E-A50F-D29C1E1B05E6 _internal~15~55B6B1CA-07FB-416E-A50F-D29C1E1B05E6
```

*Restore buckets from an input list*
```bash
splunks3restore restore --s3bucket s3-bucket --path s3/path --start -7d --end now --bidfile bidfile.txt
```

