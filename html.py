import sys
import urlparse
import re
import lxml.html
from lxml.etree import ParseError, ParserError
from lxml.cssselect import CSSSelector
from lxml.html.clean import Cleaner


class HTMLParser(object):
    """Parser used to extract meaningful elements from html."""

    def __init__(self):
        self._document = None
        self._url = None
        self._title_selector = CSSSelector('title')
        self._cleaner = Cleaner(page_structure=False, links=False, style=True, remove_tags=['link'])

    def parse(self, html, url):
        if not html or not url:
            return
        self._url = url
        # Insert newlines after br tag if there aren't already there
        html = re.sub('<\s*br\s*/?>(?!\n)', '<br/>\n', html)
        html = re.sub('<\s*BR\s*/?>(?!\n)', '<br/>\n', html)
        try:
            # Clean document html
            html = self._cleaner.clean_html(html)
            # Parse document
            self._document = lxml.html.fromstring(html, url)
            self._document.make_links_absolute()
        except ParseError, e:
            print >> sys.stderr, "Document parsing exception. ", e
        except ParserError, e:
            print >> sys.stderr, "Document parsing exception. ", e

    def get_title(self):
        if self._document is not None:
            title_elements = self._document.cssselect('title')
            if title_elements:
                return title_elements[0].text_content().strip()
        return None

    def get_links(self, http_only=True):
        if self._document is not None:
            links = set()
            # Extract links
            for element, attribute, link, pos in self._document.iterlinks():
                if attribute == "href" and link:
                    # Check hash symbol
                    if '#' in link:
                        link = link[:link.index('#')]
                    links.add(link)
            links = list(links)
            if http_only:
                links = filter(lambda x: x.startswith('http'), links)
            return links
        return []

    def get_visible_text(self):
        if self._document is not None:
            # Get content
            text_content = self._document.text_content()
            # Encode to ascii
            text = unicode(text_content).encode('ascii', 'ignore')
            return clean_whitespaces(text)
        return None

    def get_domain(self):
        if not self._url:
            return None
        return urlparse.urlparse(self._url).hostname


def clean_whitespaces(text):
    consecutive_empty_line_number = 0
    clean_lines = []
    for line in text.split('\n'):
        clean_line = line.strip()
        if clean_line:
            clean_lines.append(line.rstrip() + '\n')
            consecutive_empty_line_number = 0
        elif consecutive_empty_line_number < 2:
            clean_lines.append('\n')
            consecutive_empty_line_number += 1
    return ''.join(clean_lines)
