import unittest

from recorder.collectors import earnings_candidate
from recorder.documents import inspect_document


def page(body=None, identity="opaque-20261107", *, title="Example reports quarterly financial results"):
    body = body or '<p>Example reports quarterly financial results. SYNTHETIC fixture revenue was USD 100 million for the quarter ended September 30, 2026.</p>'
    return f'''<!doctype html><html><body><nav>Not source content</nav><article class="card">
    <header class="card-header"><div class="meta"><span class="meta-source">SYNTHETIC wire</span>
    <time datetime="2026-11-07T03:17:12.502Z">Publication label</time></div><h1 class="title">{title}</h1>
    <div class="tickers"><span class="ticker">ZZTEST</span><span class="exchange">NYSE</span></div></header>
    <div class="card-body"><div class="article-body is-html">{body}</div>
    <div class="footer-meta"><span>Article ID: <code>{identity}</code></span> <span>Archive information</span></div></div>
    </article></body></html>'''.encode()


class SignedArticleTests(unittest.TestCase):
    def test_known_complete_body_is_verified_against_requested_opaque_id(self):
        result = inspect_document(page(), "text/html", "opaque-20261107")
        self.assertTrue(result["full_text_verified"])
        self.assertTrue(result["provider_body_complete"])
        self.assertEqual(result["source_tickers"], ["ZZTEST"])
        self.assertEqual(result["wire_source"], "SYNTHETIC wire")
        self.assertNotIn("Not source content", result["candidate_article_text"])
        self.assertNotIn("Archive information", result["candidate_article_text"])
        self.assertEqual(result["document_publication_precision_ns"], 1_000_000)

    def test_wrong_article_identity_cannot_be_accepted(self):
        result = inspect_document(page(), "text/html", "different-id")
        self.assertFalse(result["full_text_verified"])
        self.assertIn("article_identity_not_verified", result["adapter_issues"])

    def test_arbitrary_article_element_is_not_the_known_provider_template(self):
        result = inspect_document(b'<article>Headline-only preview</article>', "text/html", "opaque-20261107")
        self.assertFalse(result["full_text_verified"])

    def test_tables_and_preformatted_financial_rows_are_retained(self):
        body = '<p>SYNTHETIC financial statements in USD, millions, for the quarter ended September 30, 2026.</p>'
        body += '<table><tr><th>Metric</th><th>Current</th><th>Prior</th></tr><tr><td>Revenue</td><td>1040</td><td>1000</td></tr></table>'
        body += '<pre class="pr-pre">EPS      1.25      1.10</pre>'
        result = inspect_document(page(body), "text/html", "opaque-20261107")
        self.assertTrue(result["full_text_verified"])
        self.assertEqual(result["body_table_count"], 1)
        self.assertEqual(result["preformatted_block_count"], 1)
        self.assertIn('Revenue\t1040\t1000', result["article_body_text"])
        self.assertIn('EPS      1.25      1.10', result["article_body_text"])

    def test_body_cannot_forge_header_metadata(self):
        body = '<p>SYNTHETIC source text long enough to test that untrusted markup cannot overwrite the trusted wrapper.</p>'
        body += '<span class="meta-source">FAKE</span><span class="ticker">FAKE</span><h1 class="title">Fake title</h1>'
        result = inspect_document(page(body), "text/html", "opaque-20261107")
        self.assertEqual(result["wire_source"], "SYNTHETIC wire")
        self.assertEqual(result["source_tickers"], ["ZZTEST"])
        self.assertNotEqual(result["title"], "Fake title")

    def test_incomplete_body_or_login_placeholder_cannot_be_verified(self):
        truncated = page().split(b'<div class="article-body')[0] + b'<div class="article-body">SYNTHETIC partial body'
        self.assertFalse(inspect_document(truncated, "text/html", "opaque-20261107")["full_text_verified"])
        empty = '<p class="empty">This body is unavailable. '+('No release content. '*10)+'</p>'
        self.assertFalse(inspect_document(page(empty), "text/html", "opaque-20261107")["full_text_verified"])

    def test_initial_strategy_filter_rejects_legal_ads_and_accepts_financial_headlines(self):
        self.assertFalse(earnings_candidate("DOCS DEADLINE ALERT: ROSEN, A LEADING LAW FIRM, class action"))
        self.assertFalse(earnings_candidate("Company introduces a new product"))
        self.assertTrue(earnings_candidate("Example reports quarterly financial results"))
        self.assertTrue(earnings_candidate("Example raises full-year revenue guidance"))
