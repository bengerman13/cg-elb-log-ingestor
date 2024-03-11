"""
Parser for ALB/ELB logs
"""

import datetime
import hashlib
import logging
import re
import time
import typing
from pathlib import Path
import queue

from .stats import ParserStats


logger = logging.Logger(__name__)


def timestamp_to_timestamp(timestamp: str) -> str:
    """
    Convert timestamp from what we want to what Elasticsearch wants
    """
    dt = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f%z")
    dt = datetime.datetime.utcfromtimestamp(dt.timestamp())
    return dt.isoformat(timespec="milliseconds") + "Z"


# source: https://docs.aws.amazon.com/athena/latest/ug/application-load-balancer-logs.html
ALB_LOG_LINE_REGEX = re.compile(
    r"""
          (?P<type>[^ ]*)
        \ (?P<time>[^ ]*)  # leading backslash escapes the leading space
        \ (?P<elb>[^ ]*)
        \ (?P<client_ip>[^ ]*):(?P<client_port>[0-9]*)
        \ (?P<target_ip>[^ ]*)[:-](?P<target_port>[0-9]*)
        \ (?P<request_processing_time>[-.0-9]*)
        \ (?P<target_processing_time>[-.0-9]*)
        \ (?P<response_processing_time>[-.0-9]*)
        \ (?P<elb_status_code>[-0-9]*)
        \ (?P<target_status_code>[-0-9]*)
        \ (?P<received_bytes>[-0-9]*)
        \ (?P<sent_bytes>[-0-9]*)
        \ "(?P<request_verb>[^ ]*)
        \ (?P<request_url>.*) # apparently URLs can include spaces
        \  (?P<request_proto>-|[^ ]*)\ ?" # note the leading space here
        \ "(?P<user_agent>[^"]*)"
        \ (?P<ssl_cipher>[A-Z0-9-]+)
        \ (?P<ssl_protocol>[A-Za-z0-9.-]*)
        \ (?P<target_group_arn>[^ ]*)
        \ "(?P<trace_id>[^"]*)"
        \ "(?P<domain_name>[^"]*)"
        \ "(?P<chosen_cert_arn>[^"]*)"
        \ (?P<matched_rule_priority>[-.0-9]*)
        \ (?P<request_creation_time>[^ ]*)
        \ "(?P<actions_executed>[^"]*)"
        \ "(?P<redirect_url>[^"]*)
        "(?P<error_reason>$|\ "[^ ]*")  # probably never used
        (?P<new_field>.*)  # probably never used
        """,
    re.VERBOSE,
)
ALB_LOG_LINE_REGEX_NEW = re.compile(
    r'''
          (?P<type>[^ ]*)
        \ (?P<time>[^ ]*)  # leading backslash escapes the leading space
        \ (?P<elb>[^ ]*)
        \ (?P<client_ip>[^ ]*):(?P<client_port>[0-9]*)
        \ (?P<target_ip>[^ ]*)[:-](?P<target_port>[0-9]*)
        \ (?P<request_processing_time>[-.0-9]*)
        \ (?P<target_processing_time>[-.0-9]*)
        \ (?P<response_processing_time>[-.0-9]*)
        \ (?P<elb_status_code>[-0-9]*)
        \ (?P<target_status_code>[-0-9]*)
        \ (?P<received_bytes>[-0-9]*)
        \ (?P<sent_bytes>[-0-9]*)
        \ "(?P<request_verb>[^ ]*)
        \ (?P<request_url>.*) # apparently URLs can include spaces
        \  (?P<request_proto>-|[^ ]*)\ ?" # note the leading space here
        \ "(?P<user_agent>[^"]*)"
        \ (?P<ssl_cipher>[A-Z0-9-_]+)
        \ (?P<ssl_protocol>[A-Za-z0-9.-]*)
        \ (?P<target_group_arn>[^ ]*)
        \ "(?P<trace_id>[^"]*)"
        \ "(?P<domain_name>[^"]*)"
        \ "(?P<chosen_cert_arn>[^"]*)"
        \ (?P<matched_rule_priority>[-.0-9]*)
        \ (?P<request_creation_time>[^ ]*)
        \ "(?P<actions_executed>[^"]*)"
        \ "(?P<redirect_url>[^"]*)"
        \ "(?P<error_reason>[^"]*)"
        \ "(?P<processing_targets_list>[^"]*)"
        \ "(?P<target_status_code_list>[^"]*)"
        \ "(?P<desync_mitigation_classification>[^"]*)"
        \ "(?P<desync_mitigation_classification_reason>[^"]*)"
        ''',
    re.VERBOSE,
)

ELB_LOG_LINE_REGEX = re.compile(
    r"""
        (?P<time>[^ ]*)
        \ (?P<elb>[^ ]*)
        \ (?P<client_ip>[^ ]*):(?P<client_port>[0-9]*)
        \ (?P<target_ip>[^ ]*)[:-](?P<target_port>[0-9]*)
        \ (?P<request_processing_time>[-.0-9]*)
        \ (?P<target_processing_time>[-.0-9]*)
        \ (?P<response_processing_time>[-.0-9]*)
        \ (?P<elb_status_code>|[-0-9]*)
        \ (?P<target_status_code>-|[-0-9]*)
        \ (?P<received_bytes>[-0-9]*)
        \ (?P<sent_bytes>[-0-9]*)
        \ "(?P<request_verb>[^ ]*)
        \ (?P<request_url>[^ ]*)
        \ (?P<request_proto>-|[^ ]*)\ ?"
        \ "(?P<user_agent>[^"]*)"
        \ (?P<ssl_cipher>[A-Z0-9-]+)
        \ (?P<ssl_protocol>[A-Za-z0-9.-]*)
        """,
    re.VERBOSE,
)
# map of key name to desired type constructor
ALB_LOGS_FIELD_TYPES = {
    "type": str,
    "time": timestamp_to_timestamp,
    "elb": str,
    "client_ip": str,
    "client_port": int,
    "target_ip": str,
    "target_port": int,
    "request_processing_time": float,
    "target_processing_time": float,
    "response_processing_time": float,
    "elb_status_code": int,
    "target_status_code": int,
    "received_bytes": int,
    "sent_bytes": int,
    "request_verb": str,
    "request_url": str,
    "request_proto": str,
    "user_agent": str,
    "ssl_cipher": str,
    "ssl_protocol": str,
    "target_group_arn": str,
    "trace_id": str,
    "domain_name": str,
    "chosen_cert_arn": str,
    "matched_rule_priority": str,
    "request_creation_time": timestamp_to_timestamp,
    "actions_executed": str,
    "redirect_url": str,
    "error_reason": str,
    "new_field": str,
    "processing_targets_list": str,
    "target_status_code_list": str,
    "desync_mitigation_classification": str,
    "desync_mitigation_classification_reason": str,
}

ALB = "alb"
ELB = "elb"


class LogParser:
    """
    Parses a/elb log files into dictionaries of a/elb events
    """

    def __init__(
        self,
        file_in_queue: queue.Queue,
        file_out_queue: queue.Queue,
        record_out_queue: queue.Queue,
        stats: ParserStats,
    ) -> None:
        # where we get files to process
        self.file_in_queue = file_in_queue
        # where we notifiy when files are done processing
        self.file_out_queue = file_out_queue
        # where we send records
        self.outbox = record_out_queue
        # where we publish stats
        self.stats = stats

    def run(self) -> None:
        """
        Actually do the work:
            - pull log files off the queue
            - parse them
            - put log events on the out queue
            - put log filenames on the done queue
        """
        while True:
            lines = None
            name = None
            try:
                name, lines = self.file_in_queue.get()
            except queue.Empty:
                pass
            if name is not None:
                self.stats.new_file_time()
                self.parse_alb_logs(name, lines)
                self.file_out_queue.put(name)
                self.stats.increment_files_processed()

    def parse_alb_logs(self, name, lines: typing.List[str]) -> None:
        """
        Parse log lines and push their messages to the queue
        """
        if isinstance(name, Path):
            name_string = name.name
        else:
            name_string = name
        for line in lines:
            line = line.strip()
            log_type = ALB
            match = ALB_LOG_LINE_REGEX.match(line)
            if match is None:
                match = ALB_LOG_LINE_REGEX_NEW.match(line)
            if match is None:
                log_type = ELB
                match = ELB_LOG_LINE_REGEX.match(line)
            if match is None:
                self.stats.increment_lines_errored()
                logger.error("failed to match: '%s'", line)
                return
            try:
                match = coerce_match_types(match)
            except ValueError as e:
                logger.error("failed to coerce match: %s with %s", match, e)
            if log_type is ALB:
                match = format_alb_match(match)
            else:
                match = format_elb_match(match)
            match = remove_empty_fields(match)
            match = add_metadata(match, line, name_string)
            if match is not None:
                id_ = generate_id(match, line)
                self.outbox.put((id_, match,))
                self.stats.increment_lines_processed()
            else:
                self.stats.increment_lines_errored()
                logger.error("match None after processing: '%s'", line)


def format_alb_match(match: typing.Dict) -> typing.Dict:
    """
    Turn a match dict from an ELB log into a record appropriate for Elasticsearch
    """
    new_match = {
        # request_verb, _url, and _proto will be empty strings in some cases. Replace the empty strings with -
        "@message": f"{match['request_verb'] or '-'} {match['request_url'] or '-'} {match['request_proto'] or '-'}",
        "@timestamp": match["time"],
        "@alb": {
            "matched_rule_priority": match["matched_rule_priority"],
            "actions_executed": match["actions_executed"],
            "target_group_arn": match["target_group_arn"],
            "domain_name": match["domain_name"],
            "alb": {"id": match["elb"], "status_code": match["elb_status_code"]},
            "received_bytes": match["received_bytes"],
            "chosen_cert_arn": match["chosen_cert_arn"],
            "client": {"ip": match["client_ip"], "port": match["client_port"]},
            "response": {"processing_time": match["response_processing_time"]},
            "redirect_url": match["redirect_url"],
            "sent_bytes": match["sent_bytes"],
            "trace_id": match["trace_id"],
            "target": {
                "port": match["target_port"],
                "processing_time": match["target_processing_time"],
                "status_code": match["target_status_code"],
                "ip": match["target_ip"],
            },
            "type": match["type"],
            "request": {
                "verb": match["request_verb"],
                "url": match["request_url"],
                "protocol": match["request_proto"],
                "processing_time": match["request_processing_time"],
                "creation_time": match["request_creation_time"],
            },
            "user_agent": match["user_agent"],
        },
    }
    return new_match


def format_elb_match(match: typing.Dict) -> typing.Dict:
    """
    Turn a match dict from an ALB log into a record appropriate for Elasticsearch
    """

    new_match = {
        # request_verb, _url, and _proto will be empty strings in some cases. Replace the empty strings with -
        "@message": f"{match['request_verb'] or '-'} {match['request_url'] or '-'} {match['request_proto'] or '-'}",
        "@elb": {
            "response": {"processing_time": match["response_processing_time"]},
            "elb": {"id": match["elb"], "status_code": match["elb_status_code"]},
            "ssl": {"cipher": match["ssl_cipher"], "protocol": match["ssl_protocol"]},
            "sent_bytes": match["sent_bytes"],
            "target": {
                "port": match["target_port"],
                "processing_time": match["target_processing_time"],
                "status_code": match["target_status_code"],
                "ip": match["target_ip"],
            },
            "received_bytes": match["received_bytes"],
            "request": {
                "user_agent": match["user_agent"],
                "url": match["request_url"],
                "processing_time": match["request_processing_time"],
                "verb": match["request_verb"],
                "protocol": match["request_proto"],
            },
            "client": {"ip": match["client_ip"], "port": match["client_port"]},
        },
        "@timestamp": match["time"],
    }
    return new_match


def add_metadata(record: typing.Dict, line: str, filename: str) -> typing.Dict:
    """
    Add common metadata to match _in place_
    line: the line the match was based on
    filename: the name of the logfile the log was found in
    """
    extra_metadata = {
        "@input": "s3v2",
        "@shipper.name": "elb_log_ingestor",
        "@version": "1",
        "@raw": line,
        "@level": "INFO",
        "tags": [],
        "path": filename,
    }
    return {**record, **extra_metadata}


def coerce_match_types(match: re.Match) -> typing.Dict:
    """Convert an A/ELB log match into a dict with appropriate datatypes"""
    d = match.groupdict()
    for field, converter in ALB_LOGS_FIELD_TYPES.items():
        if field in d:
            # '-' is used to represent None-ish values
            if d[field] == "-" or d[field] == "":
                d[field] = None
            if d[field] is not None:
                d[field] = converter(d[field])
    return d


def remove_empty_fields(d: typing.Dict) -> typing.Dict:
    """
    Recursively remove empty collections and Nones from dict
    """
    if d is None:
        return None
    # stash keys because we can't change a dict while iterating
    # using keys()
    keys = list(d.keys())
    for k in keys:
        v = d[k]
        if isinstance(v, dict):
            remove_empty_fields(v)
        if v is None or v == {} or v == []:
            d.pop(k)
    return d


def generate_id(entry: typing.Dict, line: str) -> str:
    """
    Generate a fairly unique key for a log entry
    """
    key = line
    #if "@alb" in entry:
    #    # ALBs already have one
    #    key = ":".join(
    #        [
    #            entry["@alb"]["alb"]["id"],
    #            entry["@alb"]["client"]["ip"],
    #            str(entry["@alb"]["client"]["port"]),
    #            entry["@timestamp"],
    #            str(entry["@alb"]["received_bytes"]),
    #        ]
    #   )
    #else:
    #    # for ELBs, take the elb id, client socket, timestamp, and size of the client request`
    #    key = ":".join(
    #        [
    #            entry["@elb"]["elb"]["id"],
    #            entry["@elb"]["client"]["ip"],
    #            str(entry["@elb"]["client"]["port"]),
    #            entry["@timestamp"],
    #            str(entry["@elb"]["received_bytes"]),
    #        ]
    #    )
    key = bytes(key, "utf-8")

    # take a shasum of the key, mostly so people don't try to attach meaning to it
    return hashlib.sha256(key).hexdigest()
