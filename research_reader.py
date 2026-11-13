"""Add website navigation assets without rewriting finalized packet sources."""
from html import escape
from html.parser import HTMLParser
import re


class _HeadBoundary(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=False)
        self.end = None
        self.has_canonical = False

    def handle_endtag(self, tag):
        if tag == "head" and self.end is None:
            self.end = self.getpos()

    def handle_starttag(self, tag, attrs):
        values = dict(attrs)
        if tag == "link" and "canonical" in (values.get("rel") or "").lower().split():
            self.has_canonical = True


def reader_html(source, *, stylesheet_url, script_url, canonical_url, ticker="", youtube_id="", analytics_id="", alerts_enabled=False):
    """Insert only asset links into a verified copy; preserve all source bytes."""
    text = source.decode("utf-8")
    parser = _HeadBoundary()
    parser.feed(text)
    if parser.end is None:
        raise ValueError("Research packet has no document head")
    line, column = parser.end
    start = sum(len(part) + 1 for part in text.split("\n")[:line - 1])
    offset = len(text[:start + column].encode("utf-8"))
    assets = '\n<!-- Charged Alpha website reading view -->\n'
    if not parser.has_canonical:
        assets += f'<link rel="canonical" href="{escape(canonical_url, quote=True)}">\n'
    assets += f'<link rel="stylesheet" href="{escape(stylesheet_url, quote=True)}">\n'
    assets += (f'<script src="{escape(script_url, quote=True)}" '
               f'data-ticker="{escape(ticker, quote=True)}" '
               f'data-youtube-id="{escape(youtube_id, quote=True)}" '
               f'data-alerts-enabled="{str(bool(alerts_enabled)).lower()}" defer></script>\n')
    assets += '<script src="/static/brand.js?v=measurement-1" defer></script>\n'
    if analytics_id and re.fullmatch(r'G-[A-Z0-9]+', analytics_id):
        assets += f'<script async src="https://www.googletagmanager.com/gtag/js?id={analytics_id}"></script>\n'
        assets += f'<script src="/static/research-measurement.js?v=1" data-id="{analytics_id}" defer></script>\n'
    return source[:offset] + assets.encode("utf-8") + source[offset:]
