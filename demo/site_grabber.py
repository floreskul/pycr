"""
Example of site grabber.

Download all pages starting with the given url.
Keep directory structure according to urls.
"""
import os
import sys
import time
import urlparse
import pycr


class SiteGrabber(object):
    def __init__(self, start_link, path_to_save):
        # Check if the directory to save files exists
        if not os.path.isdir(path_to_save):
            os.mkdir(path_to_save)
        if path_to_save.endswith('/'):
            path_to_save = path_to_save[:len(path_to_save) - 1]
        self._path_to_save = path_to_save
        # Use sets to store processed urls
        # and those to be processed in order to ignore urls repetition
        self._processed_urls = set()
        self._urls_to_process = set()
        self._urls_to_process.add(start_link)
        # Save host
        self._host_name = urlparse.urlparse(start_link).hostname
        # Time when we ran out of urls to process
        self._none_urls_time = None

    def put(self, crawling_result):
        url, urls, page_html = crawling_result
        for u in urls:
            # Add links
            if (urlparse.urlparse(u).hostname == self._host_name and
                not u in self._processed_urls):
                self._urls_to_process.add(u)
        # Add page to already processed pages set
        self._processed_urls.add(url)
        # Save file
        self._save(url, page_html)
        # Mark that there are still some urls
        if self._none_urls_time:
            self._none_urls_time = None

    def _save(self, url, page_html):
        # If url ends with '/' remove it
        if url.endswith('/'):
            url = url[:len(url) - 1]
        # Set default values
        path = self._path_to_save
        file_name = 'index'

        parsed_result = urlparse.urlparse(url)
        s = parsed_result.path
        if '/' in s:
            i = s.rindex('/')
            if i != 0:
                path += s[:i]
                file_name = s[i + 1:]
        try:
            # Create directories if necessary
            if path != self._path_to_save and not os.path.isdir(path):
                os.makedirs(path)
            # Save file
            f = file(path + os.sep + file_name, "w")
            f.write(page_html)
            f.close()
        except Exception, e:
            print >> sys.stderr, 'Error saving file for URL %s: %s' % (url, e)

    def next_urls(self, maximal_url_queue_size):
        if self._urls_to_process:
            return list(self._urls_to_process)[:maximal_url_queue_size]
        else:
            if not self._none_urls_time:
                self._none_urls_time = time.time()
            return None

    def finished(self):
        # Just wait 5 second
        if self._none_urls_time and (time.time() - self._none_urls_time > 5):
            return True
        return False


def include_links(url, page_html, parser=pycr.html.HTMLParser()):
    parser.parse(page_html, url)
    return url, parser.get_links(), page_html

if __name__ == '__main__':
        options = pycr.Options()
        options.timeout = 0.1
        options.use_logging = True
        options.logging_path = './logs/'
        grabber = SiteGrabber("http://python.org", "./downloaded/")
        master = pycr.Crawler(grabber.next_urls, grabber.put, grabber.finished, include_links, options)
        master.run()
