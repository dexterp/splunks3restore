# s2deletemarkers

A command line utility to recover Splunk buckets store in S3

# Examples

*Recover _internal and _audit indexes from between 7 & 6 days ago*
```bash
s2deletemarkers --s3bucket=s3bucket --start=-7d --end=-6d mystack _internal _audit
```

```bash
s2deletemarkers --s3bucket=s3bucket --start=-7d --end=-6d mystack _internal _audit
```