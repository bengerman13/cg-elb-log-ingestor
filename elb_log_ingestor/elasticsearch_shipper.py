"""
Sends messages to Elasticsearch
"""
import datetime
import logging
import queue
import time
import typing

import elasticsearch

from . import stats


logger = logging.Logger(__name__)
HTTP_CONFLICT = 409


class ElasticsearchShipper:
    """
    Send messages to elasticsearch
    """

    def __init__(
        self,
        elasticsearch_client: elasticsearch.client,
        record_queue: queue.Queue,
        index_pattern: str,
        stats: stats.ShipperStats,
    ) -> None:
        self.es = elasticsearch_client
        self.record_queue = record_queue
        self.index_pattern = index_pattern
        self.stats = stats

    def run(self) -> None:
        """
        Actually do the work:
        - pull a message off the queue
        - send the message to elasticsearch
        """
        while True:
            record = self.record_queue.get()
            self.index_record(*record)

    def index_record(self, id_: str, record: typing.Dict) -> None:
        """
        Index a document into elasticsearch
        """
        index = self.figure_index(record)
        try:
            self.es.create(index=index, id=id_, body=record, doc_type='_doc')
        except elasticsearch.ConflictError:
            self.stats.increment_duplicates_skipped()
            logger.info("Skipping duplicate document with id %s", id_)
        except Exception:
            # if it failed for an unknown reason, log it and put it back on the queue so we can try again
            self.stats.increment_documents_errored()
            logger.exception("Failed to index document")
            self.record_queue.put((id_, record))
            time.sleep(0.1)
        else:
            self.stats.increment_documents_indexed()
            self.stats.document_time()
            logger.debug("Indexing document with id %s", id_)

    def figure_index(self, record: typing.Dict) -> str:
        ts = record['@timestamp']
        if ts.endswith('Z'):
            ts = ts[:-1]
        ts = datetime.datetime.fromisoformat(ts)
        return ts.strftime(self.index_pattern)

    @property
    def healthy(self) -> bool:
        """
        Check if we can reach elasticsearch
        """
        healthy = False
        try:
            healthy = self.es.ping()
        except:
            healthy = False
        return healthy
