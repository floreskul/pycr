"""
Simple multi-processed crawler.

Logging is initially disabled. It is performed only to files.
You can specify a path to directory holding log files.
The name of files are:
* master_[pid].log for master process
* worker_[pid].log for worker processes
Here [pid] is replaced with pid of a process.
"""

import httplib
import os
import logging
import time
from urlparse import urlparse
from heapq import heappush, heappop
from multiprocessing import Process, Pipe, Event, current_process


VERSION = 0.7
DEFAULT_AGENT = "PyCR crawler v." + str(VERSION)


class Options(object):
    """Options used for crawling.

    TODO: add robots.txt processing, maximal redirects number
    """

    def __init__(self):
        # Master options
        self.number_of_workers = 2
        self.maximal_url_queue_size = 100
        # There should be at least some positive timeout for correct work
        self.timeout = 0.2
        self.sleep_time = 0.01
        # Worker options
        self.maximal_timeout = None
        self.maximal_page_size = None
#        self.maximal_redirrects_number = None
        self.agent = DEFAULT_AGENT

        # Common options
        # Logging
        self.logging_path = None
        self.logging_format = '%(asctime)s %(levelname)-8s %(message)s'
        self.accepted_content_types = ['text/html', 'text/plain']
        # Robots
#        self.use_robots = True


class CrawlingError(BaseException):
    def __init__(self, url=None, message=None, status=None):
        self.url = url
        # Error message
        self.message = message
        # HTTP response status
        self.status = status


def crawl(url, options):
    """Open page with the given URL."""

    # Get connection with host and relative path
    host = urlparse(url).hostname
    path = urlparse(url).path
    port = urlparse(url).port
    if not host:
        raise CrawlingError(url)
    connection = httplib.HTTPConnection(host, port)
    #Seet maximal timeout limit
    if connection.sock:
        connection.sock.settimeout(options.maximal_timeout)
    headers = {'User-Agent': options.agent,
#               'Accept' : 'text/html, text/plain'
                'Accept': ','.join(options.accepted_content_types)
               }
    result = None
    try:
        # Make request
        connection.request("GET", path, headers=headers)
        response = connection.getresponse()
        # Check response code
        if response.status == httplib.OK:
            # Check content-type
            is_content_ok = False
            received_content_type = response.getheader('content-type')
            for content_type in options.accepted_content_types:
                if content_type in received_content_type:
                    is_content_ok = True
                    break
            if not is_content_ok:
                logging.info('Bad content-type %s' % received_content_type)
                raise CrawlingError(url, "Bad content-type", response.status)
            # Check page size if necessary
            if options.maximal_page_size:
                data = response.read(options.maximal_page_size)
                # Return an error if page is too big
                if response.read():
                    raise CrawlingError(url, "Page is too big", response.status)
                else:
                    result = data
            else:
                # Read all page
                result = response.read()
        else:
            raise CrawlingError(url, "Bad response status", response.status)
    # Return error if any exception is caught
    except Exception, e:
        raise CrawlingError(url, str(e) + str(e.__class__))
    # Close connection and return result
    connection.close()
    return result


def process_crawling(connection, is_avaliable, options, process=None):
    """Crawling function performed by workers."""

    # Start logging if it is enabled in options
    reload(logging)
    pid = current_process().pid
    if options.logging_path:
        filename = options.logging_path + 'worker_' + str(pid) + '.log'
        logging.basicConfig(level=logging.INFO,
                            filename=filename,
                            format=options.logging_format
                            )
    logging.info("Worker %d started" % pid)

    # Set worker as available
    is_avaliable.set()
    while True:
        if connection.poll(None):
            url = connection.recv()
            # Set worker as busy
            is_avaliable.clear()
            if url:
                logging.info("Received new url: " + url)
                try:
                    page_html = crawl(url, options)
                    logging.info("Crawled")
                except CrawlingError, e:
                    page_html = None
                    logging.error("Error: " + str(e.status))
                # Call passed function on received data
                if process:
                    logging.info('Started processing')
                    result = process(url, page_html)
                    logging.info('Finished processing')
                    connection.send(result)
                else:
                    connection.send((url, page_html))
            # Set worker as available
            is_avaliable.set()


class Crawler(object):
    """Master to start child processes (workers) to crawl."""

    def __init__(self, next_urls, put, finished=None, process=None, options=Options()):
        # Save options
        self.options = options
        self.next_urls = next_urls
        self.put = put
        self.finished = finished
        self.process = process
        # Indicator when to stop crawling
        self._continue = True
        # Connections to communicate with workers
        self._connections = []
        # Worker processes
        self._workers = []
        # Initialize worker processes
        for i in xrange(self.options.number_of_workers):
            worker_connection, self_connection = Pipe()
            is_avaliable = Event()
            worker = Process(target=process_crawling, args=(self_connection, is_avaliable,
                                                       self.options, process))
            self._workers.append(worker)
            self._connections.append((is_avaliable, worker_connection))
        # List of urls
        self._urls = []
        # List to save crawling results
        self._results = []
        # Set url queue attributes
        self._host_heap = []
        self._back_queue_dict = {}
        self._back_queue_length = 3 * self.options.number_of_workers
        # Current number of urls in back queue
        self._url_queue_size = 0

    def _start_workers(self):
        for worker in self._workers:
            worker.start()

    def _stop_workers(self):
        for worker in self._workers:
            worker.terminate()

    def run(self):
        """The method of master process work."""

        # Start logging into files if it is enabled in options
        # This should run only after worker processes are started
        reload(logging)
        pid = current_process().pid
        if self.options.logging_path:
            # Check if the directory for logs exist
            if not os.path.isdir(self.options.logging_path):
                try:
                    os.makedirs(self.options.logging_path)
                except os.error:
                    pass
            try:
                filename = self.options.logging_path + 'master_' + str(pid) + '.log'
                logging.basicConfig(level=logging.DEBUG,
                                    filename=filename,
                                    format=self.options.logging_format
                                    )
            except IOError:
                # Set not to use logging in files
                logging.error('Cannot access directory %s',
                                  self.options.logging_path)
                self.options.logging_path = None
        # Start workers
        self._start_workers()
        logging.info("Master %d started" % pid)

        try:
            # Main master cycle
            while not (self.finished and self.finished()) and self._continue:
                # Check data from workers
                for is_avaliable, connection in self._connections:
                    # If there is any data
                    if connection.poll():
                        result = connection.recv()
                        logging.info("Received result")
                        # Save result
                        self.put(result)
                    # Check if the worker is available
                    if is_avaliable.is_set():
                        url = self._get_url_to_crawl()
                        if url:
                            connection.send(url)
                            logging.info("Sent url: " + url)
                        elif not self.finished:
                            self._continue = False
                time.sleep(self.options.sleep_time)
        finally:
            # Stop all worker processes
            self._stop_workers()
            logging.info("Workers stopped")

    def stop(self):
        self._stop_workers()

    def _check_host_heap(self):
        if self._url_queue_size > 0:
            return
        else:
            # Get new urls to add to queue
            new_urls = self.next_urls(self.options.maximal_url_queue_size)

            # Process all new urls
            for new_url in new_urls:
                # Get new host name
                new_host = urlparse(new_url).hostname
                if not new_host:
                    logging.error('Bad url %s' % new_url)
                    return

                # Check for host already to be in the back queues
                if new_host in self._back_queue_dict:
                    self._back_queue_dict[new_host].append(new_url)
                else:
                    # Add new queue to back dictionary
                    self._back_queue_dict[new_host] = [new_url]
                    # Add new host to the url heap
                    heappush(self._host_heap, (time.time() + self.options.timeout, new_host))
                self._url_queue_size += 1
#        logging.info(self._host_heap)
#        # Dictionary for logging purposes
#        d = {}
#        for key in self._back_queue_dict:
#            d[key] = len(self._back_queue_dict[key])
#        logging.info(d)

    def _get_url_to_crawl(self):
        """Get next url."""

        # Check host heap and add urls if necessary
        self._check_host_heap()

        # Check heap to have at least one url
        if not self._host_heap:
            return None

        # Get host name and  next_timetime to crawl from the url heap
        next_time, host = heappop(self._host_heap)
        # Sleep if time has not come
        delta_time = next_time - time.time()
        if delta_time > 0:
            time.sleep(delta_time)
        # Get url to parse
        queue = self._back_queue_dict[host]
        url = queue.pop(0)
        self._url_queue_size -= 1
        # Check if current queue is not empty
        if queue:
            # Add host to the url heap
            heappush(self._host_heap, (time.time() + self.options.timeout, host))
        else:
            # Remove the host from back queue dictionary
            self._back_queue_dict.pop(host)
        return url
