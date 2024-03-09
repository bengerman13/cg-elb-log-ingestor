"""
Retrieves ELB log files
"""
import io
import logging
import pathlib
import queue
import time

import boto3

logger = logging.Logger(__name__)


class S3LogFetcher:
    """
    Fetches logs from S3, moves logs in S3 around to indicate they're processing/processed
    """

    def __init__(
        self,
        bucket,
        to_do: queue.Queue,
        done: queue.Queue,
        *,
        unprocessed_prefix: str,
        processing_prefix: str,
        processed_prefix: str,
        file_batch_size: int = 5,
    ) -> None:
        """
        bucket: the name of the bucket
        s3_client: a boto s3 client
        unprocessed_prefix: the prefix in the bucket to look for new logs
        processing_prefix: the prefix in the bucket to put/find processing logs
        processed_prefix: the prefix in the bucket to put processed logs
        to_do: the queue to send work to the log parser
        done: the queue to listen on for finished work
        file_batch_size: how many log files to pull down at a time
        """
        self.bucket = bucket
        self.unprocessed_prefix = unprocessed_prefix
        self.processing_prefix = processing_prefix
        self.processed_prefix = processed_prefix
        self.to_do = to_do
        self.done = done
        self.file_batch_size = file_batch_size
        self.healthy = True

    def run(self) -> None:
        """
        Do the work:
        - first, prime the queue with some logs
        - then, poll to see when the logs are done
        - if we can get a log off the done queue, mark it as done
        - if we get a log off the done queue, get another one for the to_do queue
        """
        while True:
            if self.to_do.empty():
                self.enqueue_log(self.file_batch_size)
            try:
                finished_log = self.done.get(timeout=1)
            except queue.Empty:
                continue
            try:
                self.mark_log_processed(finished_log)
            except Exception as e:
                # if it fails:
                #   - log it
                #   - put it back on the queue, so we can retry
                #   - mark ourselves unhealthy
                logger.error(e)
                self.done.put(finished_log)
                self.healthy = False
            else:
                self.healthy = True

    def enqueue_log(self, count: int = 1) -> str:
        """
        Download one log from S3, mark it as processing, and return its name.
        If there are no logs to get, return None.
        """
        try:
            boto_response = self.bucket.objects.filter(
                MaxKeys=count, Prefix=self.unprocessed_prefix
            )
        except Exception:
            # ignore it and try again later - hopefully someone's checking health
            logger.exception("Failed listing logs in S3")
            self.healthy = False
            return None
        else:
            self.healthy = True
        boto_response = list(boto_response)
        for response in boto_response:
            next_object = response.key
            processing_name = self.mark_log_processing(next_object)
            contents = io.BytesIO()
            self.bucket.download_fileobj(processing_name, contents)
            contents.seek(0)
            strings = [line.decode("utf-8") for line in contents.readlines()]
            self.to_do.put((processing_name, strings),)

    def mark_log_processed(self, logname: str) -> None:
        """
        Move a logfile from the processing to the processed prefix.
        """
        processed_name = self.processed_name_from_processing_name(logname)
        self.move_object(from_=logname, to=processed_name)
        return processed_name

    def mark_log_processing(self, logname: str) -> None:
        """
        Move a logfile from the unprocessed to the processing prefix.
        """
        # Note - this is one of the places where a race condition can cause
        # us to process a file more than once
        processing_name = self.processing_name_from_unprocessed_name(logname)
        self.move_object(from_=logname, to=processing_name)
        return processing_name

    def move_object(self, from_, to) -> None:
        """
        Move/rename an object within this bucket.
        """
        # boto doesn't have a move operation, so we need to copy then delete
        self.bucket.copy(dict(Bucket=self.bucket.name, Key=from_), to)
        delete_request = {"Objects": [{"Key": from_}], "Quiet": True}
        self.bucket.delete_objects(Delete=delete_request)

    def processing_name_from_unprocessed_name(self, unprocessed_name: str) -> str:
        """
        Determine the processing name from an unprocessed name
        """
        return replace_prefix(
            unprocessed_name, self.unprocessed_prefix, self.processed_prefix
        )

    def processed_name_from_processing_name(self, processing_name: str) -> str:
        """
        Determine the processed name from an unprocessed name
        """
        return replace_prefix(
            processing_name, self.processing_prefix, self.processed_prefix
        )


class LocalLogFetcher:

    def __init__(self, input_dir: pathlib.Path, processing_dir: pathlib.Path, done_dir: pathlib.Path, to_do: queue.Queue, done: queue.Queue, file_batch_size: int = 5,):
        self.input_dir = input_dir
        self.processing_dir = processing_dir
        self.done_dir = done_dir
        self.to_do = to_do
        self.done = done
        self.file_batch_size = file_batch_size

    def run(self) -> None:
        """
        Do the work:
        - first, prime the queue with some logs
        - then, poll to see when the logs are done
        - if we can get a log off the done queue, mark it as done
        - if we get a log off the done queue, get another one for the to_do queue
        """
        while True:
            if self.to_do.empty():
                self.enqueue_log(self.file_batch_size)
            try:
                finished_log = self.done.get(timeout=1)
            except queue.Empty:
                continue
            try:
                self.mark_log_processed(finished_log)
            except Exception as e:
                # if it fails:
                #   - log it
                #   - put it back on the queue, so we can retry
                #   - mark ourselves unhealthy
                logger.error(e)
                self.done.put(finished_log)
                self.healthy = False
            else:
                self.healthy = True

    def enqueue_log(self, count: int = 1) -> str:
        """
        Download one log from S3, mark it as processing, and return its name.
        If there are no logs to get, return None.
        """
        # get the list of files in the processing directory
        # take the first count of them
        # push them to the queue && move them
        filenames = self.input_dir.glob("*.log")
        for filename in list(filenames)[:self.file_batch_size]:
            try:
                processing_name = self.mark_log_processing(filename)
            except:
                continue
            contents = io.BytesIO()
            with open(processing_name, "r") as contents:
                strings = [line for line in contents.readlines()]
            self.to_do.put((processing_name, strings),)

    def mark_log_processed(self, logname: str) -> None:
        """
        Move a logfile from the processing to the processed prefix.
        """
        processed_name = self.done_dir / logname.name
        #self.move_object(from_=logname, to=processed_name)
        open(processed_name, "w").close() # truncate to save disk space
        return processed_name

    def mark_log_processing(self, logname: pathlib.Path) -> None:
        """
        Move a logfile from the unprocessed to the processing prefix.
        """
        # Note - this is one of the places where a race condition can cause
        # us to process a file more than once
        processing_name = self.processing_dir / logname.name
        self.move_object(from_=logname, to=processing_name)
        return processing_name

    def move_object(self, from_: pathlib.Path, to: pathlib.Path) -> None:
        """
        Move/rename an object within this bucket.
        """
        from_.rename(to)







def replace_prefix(logname: str, old_prefix: str, new_prefix: str) -> str:
    if not logname.startswith(old_prefix):
        raise ValueError
    return logname.replace(old_prefix, new_prefix, 1)
