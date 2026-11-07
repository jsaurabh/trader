"""Fetch documented article links without inventing a trial body API."""
import asyncio
import base64
import hashlib
from html.parser import HTMLParser
import re
from urllib.parse import urlsplit, urljoin

import httpx

from .timebase import Stamp, parse_rfc3339

MAX_DOCUMENT_BYTES = 8 * 1024 * 1024
ARTICLE_ADAPTER_VERSION = "rtpr-signed-article-v1"


def article_id(url: str) -> str:
    parsed = urlsplit(url)
    if (parsed.scheme != "https" or parsed.hostname != "rtpr.io" or parsed.port not in (None, 443)
            or parsed.username or parsed.password or not parsed.path.startswith("/a/")
            or not parsed.path[3:] or parsed.fragment):
        raise ValueError("unrecognized documented RTPR article URL")
    # Query signature expiry is not part of an article's stable identity.
    return parsed.path[3:]


class ArticleText(HTMLParser):
    """Candidate text only. Raw HTML retains all tables and source structure."""
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.in_article = 0
        self.hidden = 0
        self.parts = []
        self.found_article = False

    def handle_starttag(self, tag, attrs):
        if tag == "article":
            self.in_article += 1
            self.found_article = True
        if tag in ("script", "style"):
            self.hidden += 1
        if self.in_article and tag in ("p", "div", "tr", "br", "table", "h1", "h2", "h3"):
            self.parts.append("\n")
        if self.in_article and tag in ("td", "th"):
            self.parts.append("\t")

    def handle_endtag(self, tag):
        if tag == "article":
            self.in_article = max(0, self.in_article - 1)
        if tag in ("script", "style"):
            self.hidden = max(0, self.hidden - 1)

    def handle_data(self, data):
        if self.in_article and not self.hidden:
            self.parts.append(data)


class RTPRArticle(HTMLParser):
    """Known signed-link page: header, body, and footer are distinct regions."""
    VOID = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.stack, self.parts = [], {k: [] for k in ("title", "body", "source", "footer")}
        self.tickers, self.publication = [], None
        self.body_count, self.body_closed, self.table_count, self.pre_count = 0, False, 0, 0
        self.header_count, self.html_body = 0, False
        self.empty_placeholder = False

    def active(self, key):
        return any(key in frame[1] for frame in self.stack)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        classes = set(attrs.get("class", "").split())
        markers = set()
        in_body = self.active("body")
        if tag == "article" and "card" in classes and not in_body:
            markers.add("article")
        if self.active("article") and not in_body:
            if tag == "header" and "card-header" in classes:
                markers.add("header")
                self.header_count += 1
            if self.active("header"):
                if tag == "h1" and "title" in classes:
                    markers.add("title")
                if "meta-source" in classes:
                    markers.add("source")
                if "ticker" in classes:
                    markers.add("ticker")
                    self.tickers.append([])
                if tag == "time":
                    self.publication = attrs.get("datetime")
            if tag == "div" and "article-body" in classes:
                markers.add("body")
                self.body_count += 1
                self.html_body = "is-html" in classes
            if tag == "div" and "footer-meta" in classes:
                markers.add("footer")
        if tag in ("script", "style"):
            markers.add("hidden")
        if in_body and not self.active("hidden"):
            if "empty" in classes:
                self.empty_placeholder = True
            if tag in ("p", "div", "tr", "br", "table", "pre", "h1", "h2", "h3", "li"):
                self.parts["body"].append("\n")
            if tag in ("td", "th"):
                self.parts["body"].append("\t")
            if tag == "table":
                self.table_count += 1
            if tag == "pre":
                self.pre_count += 1
        if tag not in self.VOID:
            self.stack.append((tag, markers))

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if tag not in self.VOID:
            self.handle_endtag(tag)

    def handle_endtag(self, tag):
        for index in range(len(self.stack)-1, -1, -1):
            if self.stack[index][0] == tag:
                removed = self.stack[index:]
                if any("body" in frame[1] for frame in removed):
                    self.body_closed = True
                del self.stack[index:]
                return

    def handle_data(self, data):
        if self.active("hidden"):
            return
        for region in self.parts:
            if self.active(region):
                self.parts[region].append(data)
        if self.active("ticker") and self.tickers:
            self.tickers[-1].append(data)

    def document(self, expected_event_id):
        title, body, source, footer = ("".join(self.parts[k]).strip() for k in ("title", "body", "source", "footer"))
        match = re.search(r"Article ID:\s*(\S+)", footer)
        opaque_id = match.group(1) if match else None
        publication_ns, precision = None, None
        try:
            publication_ns, precision = parse_rfc3339(self.publication)
        except (TypeError, ValueError):
            pass
        issues = []
        if self.body_count != 1 or self.header_count != 1 or not self.body_closed:
            issues.append("unrecognized_or_incomplete_article_structure")
        if not title or not source or not body or len(body) < 80:
            issues.append("missing_or_empty_release_content")
        if opaque_id != expected_event_id or expected_event_id is None:
            issues.append("article_identity_not_verified")
        if publication_ns is None:
            issues.append("missing_or_invalid_document_publication_time")
        if self.empty_placeholder:
            issues.append("empty_release_placeholder")
        return {"adapter_version": ARTICLE_ADAPTER_VERSION, "title": title, "wire_source": source,
                "source_tickers": ["".join(t).strip() for t in self.tickers],
                "document_published_raw": self.publication, "document_published_ns": publication_ns,
                "document_publication_precision_ns": precision, "opaque_id_from_document": opaque_id,
                "article_body_text": body, "body_table_count": self.table_count, "preformatted_block_count": self.pre_count,
                "body_format": "html" if self.html_body else "plain_text", "adapter_issues": issues,
                "provider_body_complete": not issues,
                "completeness_scope": "Complete body supplied by RTPR in the recognized signed-article template; original wire version may differ"}


def inspect_document(body: bytes, content_type: str, expected_event_id=None) -> dict:
    text = body.decode("utf-8", errors="replace")
    parser = ArticleText()
    if "html" in content_type:
        parser.feed(text)
    candidate = "".join(parser.parts).strip() if parser.found_article else None
    result = {
        "status": "retrieved_unverified", "full_text_verified": False,
        "content_sha256": hashlib.sha256(body).hexdigest(),
        "raw_body_base64": base64.b64encode(body).decode("ascii"),
        "content_type": content_type, "byte_count": len(body),
        "contains_html_tables": bool(re.search(r"<table(?:\s|>)", text, re.I)),
        "candidate_article_text": candidate,
        "completeness_reason": "Complete release body and expected article structure have not yet been validated",
    }
    if "html" in content_type and expected_event_id is not None:
        adapter = RTPRArticle()
        adapter.feed(text)
        parsed = adapter.document(expected_event_id)
        result.update(parsed)
        if parsed["provider_body_complete"]:
            result.update(status="full_text_available", full_text_verified=True,
                          candidate_article_text=parsed["title"] + "\n\n" + parsed["article_body_text"],
                          completeness_reason="Known RTPR article body, publication metadata, and opaque ID validated")
    return result


class DocumentCollector:
    def __init__(self, journal, client: httpx.AsyncClient, capacity=128, workers=4, on_result=None):
        self.journal, self.client = journal, client
        self.pending = asyncio.Queue(maxsize=capacity)
        self.worker_count = workers
        self.on_result = on_result

    def enqueue(self, event_id, url, receipt: Stamp, deadline_comparable=True):
        try:
            self.pending.put_nowait((event_id, url, receipt, deadline_comparable))
        except asyncio.QueueFull:
            self.journal.emit("document", {"event_id": event_id, "status": "queue_overflow",
                              "full_text_verified": False, "receipt_mono_ns": receipt.mono_ns})

    async def worker(self):
        while True:
            event_id, url, receipt, comparable = await self.pending.get()
            started = Stamp.now()
            data = {"event_id": event_id, "receipt_mono_ns": receipt.mono_ns,
                    "started_mono_ns": started.mono_ns, "full_text_verified": False}
            try:
                article_id(url)
                # No bearer key or browser cookies are sent to the article host.
                # Preserve the exact signed link; do not guess private endpoints.
                async with self.client.stream("GET", url, follow_redirects=False) as response:
                    data["http_status"] = response.status_code
                    if response.is_redirect:
                        location = urlsplit(urljoin(url, response.headers.get("location", "")))
                        data.update(status="authentication_required" if location.path.startswith(("/sign-in", "/login"))
                                    else "redirect_requires_verified_mechanism",
                                    redirect_host=location.hostname, redirect_path=location.path)
                    elif response.status_code != 200:
                        data["status"] = "http_error"
                    else:
                        body = bytearray()
                        async for chunk in response.aiter_bytes():
                            body.extend(chunk)
                            if len(body) > MAX_DOCUMENT_BYTES:
                                raise ValueError("document_size_limit")
                        downloaded = Stamp.now()
                        data["download_complete_utc_ns"] = downloaded.utc_ns
                        data["download_complete_mono_ns"] = downloaded.mono_ns
                        data.update(await asyncio.to_thread(inspect_document, bytes(body), response.headers.get("content-type", ""), event_id))
            except asyncio.CancelledError:
                data["status"] = "cancelled_at_shutdown"
                self.journal.emit("document", data)
                raise
            except Exception as exc:
                # Exception messages may contain a signed URL or a credential.
                data.update(status="retrieval_error", error_class=type(exc).__name__)
            finally:
                self.pending.task_done()
            completed = Stamp.now()
            data["finished_within_five_seconds"] = (completed.mono_ns <= receipt.mono_ns + 5_000_000_000) if comparable else None
            data["deadline_comparable_in_this_run"] = comparable
            self.journal.emit("document", data, completed)
            if self.on_result:
                await self.on_result(event_id, receipt, data, comparable)
