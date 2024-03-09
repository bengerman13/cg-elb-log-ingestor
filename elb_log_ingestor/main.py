import http.server
import os
import pathlib
import queue
import sys
import threading
import logging

import elasticsearch

from . import api_endpoint
from . import elasticsearch_shipper
from . import elb_log_fetcher
from . import elb_log_parse
from . import stats


def start_server():
    parser_stats = stats.ParserStats()
    shipper_stats = stats.ShipperStats()

    shipper_count = int(os.environ.get("ELB_INGESTOR_SHIPPER_COUNT", "5"))

    elasticsearch_hosts = os.environ["ELB_INGESTOR_ELASTICSEARCH_HOSTS"]
    elasticsearch_hosts = elasticsearch_hosts.split(",")
    es_client = elasticsearch.Elasticsearch(
        elasticsearch_hosts, sniff_on_start=True, sniffer_timeout=60
    )
    server_address = get_server_address()

    logs_to_be_processed = queue.Queue(maxsize=10)
    logs_processed = queue.Queue()
    records = queue.Queue(maxsize=100000)
    file_batch_size = int(os.environ.get("ELB_INGESTOR_FILE_BATCH_SIZE", 5))
    index_pattern = os.environ.get("ELB_INDEX_PATTERN", "logs-platform-%Y.%m.%d")
    fetch_mode = os.environ["ELB_INGESTOR_FETCH_MODE"]
    if fetch_mode == "bad_aggressive_fetcher_do_not_use_until_we_fix_backoff":
        bucket_name = os.environ["ELB_INGESTOR_BUCKET"]
        unprocessed_prefix = os.environ.get("ELB_INGESTOR_SEARCH_PREFIX", "logs/")
        processing_prefix = os.environ.get("ELB_INGESTOR_WORKING_PREFIX", "logs-working/")
        processed_prefix = os.environ.get("ELB_INGESTOR_DONE_PREFIX", "logs-done/")
        fetcher = elb_log_fetcher.S3LogFetcher(
            bucket_name,
            to_do=logs_to_be_processed,
            done=logs_processed,
            file_batch_size=file_batch_size,
            unprocessed_prefix=unprocessed_prefix,
            processing_prefix=processing_prefix,
            processed_prefix=processed_prefix,
        )
    elif fetch_mode == "local_file":
        input_dir = pathlib.Path(os.environ["ELB_INGESTOR_INPUT_DIR"])
        processing_dir = pathlib.Path(os.environ["ELB_INGESTOR_PROCESSING_DIR"])
        processed_dir = pathlib.Path(os.environ["ELB_INGESTOR_PROCESSED_DIR"])
        fetcher = elb_log_fetcher.LocalLogFetcher(
            input_dir,
            processing_dir,
            processed_dir,
            to_do=logs_to_be_processed,
            done=logs_processed,
            file_batch_size=file_batch_size

        )
    else:
        raise Exception("No valid fetch mode found!")

    parser = elb_log_parse.LogParser(
        logs_to_be_processed, logs_processed, records, parser_stats
    )
    shipper = elasticsearch_shipper.ElasticsearchShipper(
        es_client, records, index_pattern, shipper_stats
    )

    # prepare the ApiEndpoint class for use
    api_endpoint.ApiEndpoint.parser_stats = parser_stats
    api_endpoint.ApiEndpoint.shipper_stats = shipper_stats
    api_endpoint.ApiEndpoint.fetcher = fetcher
    api_endpoint.ApiEndpoint.shipper = shipper

    server = http.server.HTTPServer(server_address, api_endpoint.ApiEndpoint)

    fetcher_thread = threading.Thread(target=fetcher.run, daemon=False)
    parser_thread = threading.Thread(target=parser.run, daemon=True)
    shippers = []
    for i in range(shipper_count):
        shippers.append(threading.Thread(target=shipper.run, daemon=True))
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)

    fetcher_thread.start()
    parser_thread.start()
    for shipper in shippers:
        shipper.start()
    server_thread.start()


def get_server_address() -> (str, int):
    listen_host = os.environ.get("ELB_INGESTOR_LISTEN_HOST", "localhost")
    if listen_host == "0.0.0.0":
        listen_host = ""
    listen_port = int(os.environ.get("ELB_INGESTOR_LISTEN_PORT", 13131))
    return listen_host, listen_port


if __name__ == "__main__":
    start_server()
