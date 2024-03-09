import json

from http.server import BaseHTTPRequestHandler


class ApiEndpoint(BaseHTTPRequestHandler):
    """
    Responds to web requests for health and stats checks. Set parser_stats, shipper_stats, shipper, and fetcher before using!
    """
    parser_stats = None
    shipper_stats = None
    shipper = None
    fetcher = None

    def do_GET(self) -> None:
        """
        Handle an HTTP GET
        """
        self.protocol_version = 'HTTP/1.1'
        if self.path == "/stats":
            self.send_stats()
        elif self.path == "/health":
            self.send_health()
        else:
            self.send_error(404)
            self.end_headers()

    def send_stats(self) -> None:
        """
        Send statistics as JSON
        """
        parser = self.parser_stats.summary
        shipper = self.shipper_stats.summary
        parser["last_new_file_time"] = str(parser["last_new_file_time"])
        shipper["last_document_indexed_at"] = str(shipper["last_document_indexed_at"])
        stats = dict(parser=parser, shipper=shipper)
        stats['queues'] = dict()
        stats['queues']['shipper'] = dict(description='Records waiting to be sent to Elasticsearch', length=self.shipper.record_queue.qsize())
        stats['queues']['files'] = dict(description='Files waiting to be processed', length=self.fetcher.to_do.qsize())
        response = bytes(json.dumps(stats), 'utf-8')

        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.send_header("Content-length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def send_health(self) -> None:
        """
        Send health information, with a 500 if the service is unhealthy
        """
        response = dict()
        response["elasticsearch_connected"] = self.shipper.healthy
        response["s3_connected"] = self.fetcher.healthy
        if response["elasticsearch_connected"] and response["s3_connected"]:
            response["status"] = "UP"
            response = bytes(json.dumps(response), 'utf-8')
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
        else:
            response["status"] = "DOWN"
            response = bytes(json.dumps(stats), 'utf-8')
            self.send_error(500, explain=response)
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
        self.wfile.write(response)
