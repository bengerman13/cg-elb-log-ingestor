# cg-elb-log-ingestor

## what does it do?
This is an app to pull alb/elb logs from S3, parse them, and insert them into Elasticsearch

## why not use Logstash?
Logstash does not play well in our infra. Managing its config and fighting with upstream 
opinions took more work than writing our own single-purpose app

## How do I run it?

### configuration
All other configuration is done via environment variables:

#### Base configuration

| env variable                       | description                                            | default                        |
|------------------------------------|--------------------------------------------------------|--------------------------------|
| `ELB_INGESTOR_FILE_BATCH_SIZE`     | Number of log files to pull at a time for processing   | 5                              |
| `ELB_INGESTOR_LISTEN_HOST`         | Hostname or IP address to listen on for healthchecks   | "localhost"                    |
| `ELB_INGESTOR_LISTEN_PORT`         | Port to listen on for healthchecks                     | 13131                          |
| `ELB_INGESTOR_ELASTICSEARCH_HOSTS` | Comma-separated list of hosts (`host1:443,host2:9123`) | No default                     |
| `ELB_INGESTOR_INDEX_PATTERN`       | Pattern to index documents into                        | "logs-platform-%{+YYYY.MM.dd}" |
| `ELB_INGESTOR_FETCH_MODE`          | How to look for files - you should use `local_file`    |                                |
|                                    |                                                        |                                |

#### local file ingestor configuration

| env variable                       | description                                            | default                        |
|------------------------------------|--------------------------------------------------------|--------------------------------|
| `ELB_INGESTOR_INPUT_DIR`           | Local directory to look for log files in               |                                |
| `ELB_INGESTOR_PROCESSING_DIR`      | Local directory to put currently-processing files in   |                                |
| `ELB_INGESTOR_PROCESSED_DIR`       | Local directory to put semaphores of processed files in|                                |
|                                    |                                                        |                                |


#### s3 ingestor configuration

AWS authentication is via [boto3][boto3], so use any auth method supported there.

| env variable                       | description                                            | default                        |
|------------------------------------|--------------------------------------------------------|--------------------------------|
| `ELB_INGESTOR_BUCKET`              | Name of the bucket to look for logs in                 | No default                     |
| `ELB_INGESTOR_SEARCH_PREFIX`       | Prefix ("folder") under which to look for log files    | "logs/"                        |
| `ELB_INGESTOR_WORKING_PREFIX`      | Prefix to put logs being ingested into                 | "logs-working/"                |
| `ELB_INGESTOR_DONE_PREFIX`         | Prefix to put log files that have been ingested into   | "logs-done/"                   |
|                                    |                                                        |                                |

### reprocessing logs
If you need to reprocess logs (for instance, an instance failed or records were lost from elasticsearch), 
copy the files containing the logs back into the bucket/prefix that the parser reads from. The parser
will re-parse the document, and any duplicate records will be ignored on insert.


## How does it work?

### Delivery model
We use an at-least-once model, then rely on using predictable ids in Elasticsearch to deduplicate logs

### log fetcher
The log fetcher pulls logs from a mode-dependent sourc. When a log is pulled, it moves it into a processing directory.
When the log parser finishes parsing a file, the fetcher moves it from the processing directory to the processed directory.

#### local file fetcher
The local file fetcher looks for logs in a local directory. Some other process should load files into that directory.
It uses another local directory for processing files, and third directory for processed files. Processed files are
truncated to save space.

#### s3 log fetcher
the s3 log fetcher looks for logs in an s3 bucket. It changes file prefixes to mark them as processing, then changes
them again to mark them processed.

### log parser
The log parser gets logs from the fetcher, then proceses them line-by-line into dictionaries. The dictionaries are then put
onto a queue for processing by the event uploader

### event uploader
The event uploader reads events from the queue it shares with the log parser. It takes events off the queue, 
adds a suitable, predicatble ID and then indexes them into Elasticsearch

### stats
The stats objects are thread-safe metrics reporters, allowing the other objects to report their metrics, so the stats
entpoint can expose them

### health
The health server is a simple webserver to expose stats and health information. It exposes the data in the stats object
for a metrics and monitoring system to read

## How do I work on it?

### running tests

```
$ python -m venv venv
$ . venv/bin/activate
$ python setup.py test
```

[boto3]: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
