"""Small, read-only capability probes. Never place orders or change subscriptions."""
import asyncio

import httpx

from .collectors import ALPACA_DATA, RTPR_FEED, alpaca_headers, required, safe_error
from .documents import DocumentCollector, article_id


async def probe(journal):
    async with httpx.AsyncClient(timeout=12, follow_redirects=False) as client:
        async def feed():
            data = {"check": "rtpr_rest_feed"}
            try:
                r = await client.get(RTPR_FEED, headers={"Authorization": "Bearer " + required("RTPR_API_KEY")}, params={"limit": 1})
                data["http_status"] = r.status_code
                data["rate_limit_headers"] = {k: v for k, v in r.headers.items() if k.lower().startswith("x-ratelimit-")}
                if r.status_code == 200:
                    body = r.json()
                    data.update(article_count=len(body.get("articles", [])), pagination_present=bool(body.get("next_token")),
                                json_fields=list(body), trial_billing_or_expiry_exposed=False)
                    for a in body.get("articles", []):
                        data["article_fields"] = list(a)
                        data["sample_publication_raw"] = a.get("article_published_at")
                        data["sample_id"] = article_id(a["article_url"])
                        documents = DocumentCollector(journal, client, workers=1)
                        from .timebase import Stamp
                        # This is a historical REST lookup, never an alert or a latency benchmark.
                        documents.enqueue(data["sample_id"], a["article_url"], Stamp.now(), deadline_comparable=False)
                        worker = asyncio.create_task(documents.worker())
                        try:
                            await documents.pending.join()
                        finally:
                            worker.cancel()
                            await asyncio.gather(worker, return_exceptions=True)
            except Exception as exc:
                data.update(safe_error(exc))
            journal.emit("probe", data)
        async def quotes(feed_name):
            data = {"check": "alpaca_latest_quote", "feed": feed_name}
            try:
                r = await client.get(ALPACA_DATA + "/v2/stocks/quotes/latest", headers=alpaca_headers(),
                                     params={"symbols": "AAPL", "feed": feed_name})
                data["http_status"] = r.status_code
                if r.status_code == 200:
                    quote = r.json().get("quotes", {}).get("AAPL")
                    data.update(quote_present=bool(quote), source_timestamp_raw=quote.get("t") if quote else None)
                # Do not print vendor error bodies, which may echo request credentials.
            except Exception as exc:
                data.update(safe_error(exc))
            journal.emit("probe", data)
        await asyncio.gather(feed(), quotes("iex"), quotes("sip"))
        journal.emit("run_end", {"reason": "read_only_probes_complete"})
