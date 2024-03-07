"""
Retrieves ELB log files
"""
import io
import logging
import pathlib
import queue

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


class S3FixedLogFetcher(S3LogFetcher):
    
    def __init__(
        self,
        bucket,
        to_do: queue.Queue,
        done: queue.Queue,
        file_batch_size: int = 5,
        *,
        target_file_patterns: list[str],
        lock_dir: str
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
        self.to_do = to_do
        self.done = done
        self.file_batch_size = file_batch_size
        self.healthy = True
        self.target_file_patterns = target_file_patterns
        self.lock_dir = pathlib.Path(lock_dir)

    def enqueue_log(self, count):
        """
        Download all the logs from a given prefix and put them on the work queue
        marking each as processing in the process
        """
        prefix = self.get_next_prefix()
        maybe_more = True
        last_file = None
        while maybe_more:
            try:
                boto_response = self.bucket.objects.filter(
                    MaxKeys=count, Prefix=prefix, StartAfter=last_file
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
                try:
                    self.mark_log_processing(next_object)
                except FileExistsError:
                    logger.warn(f"Found {next_object} but a lock exists - skipping this")
                    continue
                contents = io.BytesIO()
                self.bucket.download_fileobj(next_object, contents)
                contents.seek(0)
                strings = [line.decode("utf-8") for line in contents.readlines()]
                self.to_do.put((next_object, strings),)
            if len(boto_response) == count:
                maybe_more = True
                last_file = boto_response[-1].key

    def get_next_prefix(self) -> str:
        for pattern in self.target_file_patterns:
            lock_file = self.lock_dir / pattern
            try:
                lock_file.touch(exist_ok=False)
            except FileExistsError:
                continue
            else:
                return pattern
        else:
            # we're done
            return

    def mark_log_processed(self, log_name: str) -> None:
        # this is a file for humans 
        # if the process crashes, we can interrogate this list of files to see
        # if there are any partially-processed files
        flag = self.lock_dir / log_name + ".done"
        flag.touch()

    def mark_log_processing(self, log_name: str) -> None:
        # this is the file that prevents us from reprocessing files
        lock_file = self.lock_dir / log_name
        lock_file.touch(exist_ok=False)

    def move_object(self, from_, to) -> None:
        pass

def replace_prefix(logname: str, old_prefix: str, new_prefix: str) -> str:
    if not logname.startswith(old_prefix):
        raise ValueError
    return logname.replace(old_prefix, new_prefix, 1)

