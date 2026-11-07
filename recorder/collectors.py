"""One observation application; independent wire, document, and quote tasks."""
import asyncio
import base64
from copy import deepcopy
import importlib.metadata
import hashlib
import json
import os
import random
import re
import time
from pathlib import Path
from urllib.parse import urlencode

import httpx
from websockets.asyncio.client import connect

from .documents import ARTICLE_ADAPTER_VERSION, DocumentCollector, article_id
from .journal import previous_run_boundary, read_rows
from .timebase import DEADLINE_NS, MarketCalendar, Stamp, parse_rfc3339
from .analysis import DEFAULT_MODEL, ModelRequestBudget, OpenAIExtractor, TimedAnalyzer, digest
from .announcement_clock import AnnouncementClockRegistry
from .candidates import build_catalogue, CandidateExtractor
from .execution import PaperExecution
from .issuer import verify_issuer, VERB, RESULTS, PERIOD
from .ingress import (StampedClientConnection, IngressError, receive_stamped, ingress_metadata,
                      RECEIPT_BOUNDARY, UNMEASURED_BEFORE_RECEIPT, INGRESS_VERSION)
from .paper_broker import PaperBroker
from .portfolio import PaperLedger
from .snapshots import QuoteSnapshots
from .market_inputs import MarketInputs

SYMBOL = re.compile(r"^[A-Z][A-Z0-9./-]{0,14}$")
ALPACA_DATA = "https://data.alpaca.markets"
ALPACA_STREAM = "wss://stream.data.alpaca.markets/v2/"
RTPR_FEED = "https://api.rtpr.io/feed/articles"


def earnings_candidate(title):
    if not isinstance(title, str) or not title.strip():
        return False
    if re.search(r"class action|securities fraud|deadline alert|investor alert|law firm|shareholder alert|securities litigation|lead plaintiff", title, re.I):
        return False
    return bool(re.search(r"earnings|financial results|\bquarter(?:ly)?\b.{0,35}\bresults\b|\bQ[1-4]\b.{0,35}\bresults\b|results for.*quarter|guidance|business outlook", title, re.I))


def required(name):
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Missing secure environment binding: {name}")
    return value


def alpaca_headers():
    return {"APCA-API-KEY-ID": required("ALPACA_API_KEY"), "APCA-API-SECRET-KEY": required("ALPACA_API_SECRET")}


def normalize_alert(msg):
    if msg.get("type") != "alert":
        raise ValueError("not an alert")
    symbol = msg.get("ticker", "")
    if not isinstance(symbol, str) or not SYMBOL.fullmatch(symbol):
        raise ValueError("invalid ticker")
    event = article_id(msg.get("article_url", ""))
    published = msg.get("article_published_at")
    parsed, precision = None, None
    if isinstance(published, str):
        try:
            parsed, precision = parse_rfc3339(published)
        except ValueError:
            pass
    return {
        "collector_source": "RTPR",
        "event_id": event, "ticker": symbol, "article_url": msg["article_url"],
        "published_raw": published, "published_ns": parsed, "published_precision_ns": precision,
        "publication_meaning": "RTPR article_published_at; earliest public availability unverified",
        "source_wire": msg.get("source") or msg.get("author") or "not_in_alert",
        "alert_kind": msg.get("alert_kind", "rule_match"),
        "rules": msg.get("rules", []),
        "correction_claim": {k: msg[k] for k in ("correction", "corrects", "version", "updated_at") if k in msg},
    }


def safe_error(exc):
    # Never log exception text: websockets/httpx may include credentials in URLs.
    return {"error_class": type(exc).__name__,
            "close_code": getattr(getattr(exc, "rcvd", None), "code", None),
            **({"ingress_fault": exc.code} if isinstance(exc, IngressError) else {})}


async def backoff(seconds):
    while seconds > 0:
        step = min(seconds, 60)
        await asyncio.sleep(step)
        seconds -= step


class Recorder:
    def __init__(self, journal, symbols, feed="iex", *, analyze=False, contexts=None, model=DEFAULT_MODEL, market_inputs=None,
                 context_store=None, extractor_mode="candidates", paper=False,
                 paper_state="data/paper-ledger.sqlite", kill_path="data/paper.kill",
                 reference_symbols=(), reference_interval=10,
                 followup_symbols=(), followup_root="data/research-followups"):
        if feed not in ("iex", "sip"):
            raise ValueError("Only explicit real-time iex or sip feeds supported; no delayed fallback")
        if not symbols or any(not SYMBOL.fullmatch(s) for s in symbols):
            raise ValueError("Provide valid symbols to pre-subscribe before announcements")
        if extractor_mode not in ("candidates", "numeric"):
            raise ValueError("Explicit candidates or numeric extractor required")
        if reference_symbols and (context_store is None or len(reference_symbols) > 3
                or len(set(reference_symbols)) != len(reference_symbols)
                or any(s not in symbols for s in reference_symbols)):
            raise ValueError("At most three explicit subscribed reference symbols require a context store")
        if followup_symbols and (len(followup_symbols) > 3 or len(set(followup_symbols)) != len(followup_symbols)
                or any(s not in symbols for s in followup_symbols) or (context_store is None and not contexts)):
            raise ValueError("At most three explicit subscribed followup symbols require prepared company context")
        self.journal, self.symbols, self.feed = journal, sorted(set(symbols)), feed
        self.calendar = MarketCalendar(Stamp.now().utc_ns)  # Prepare before any wire receipt.
        self.checkpoints = set()
        self.first_receipts = {}
        self.analysis_enabled, self.contexts, self.model = analyze or paper, deepcopy(contexts or {}), model
        self.context_store, self.extractor_mode = context_store, extractor_mode
        self.paper_enabled, self.paper_state, self.kill_path = bool(paper), paper_state, kill_path
        self.paper_execution, self.paper_ledger, self.paper_broker = None, None, None
        self.reference_symbols, self.reference_interval = tuple(reference_symbols), reference_interval
        self.reference_recorder = None
        self.followup_symbols, self.followup_root = tuple(followup_symbols), followup_root
        self.research_followup = None
        self.final_decisions = {}
        self.analyzer = None
        self.quote_snapshots = None
        self.market_inputs = market_inputs
        self._market_inputs_injected = market_inputs is not None
        # Direct recorder-only wire use has no market preparation dependency.
        # run() clears this until its shared-client adapter has been prewarmed.
        self.market_metadata_ready = asyncio.Event()
        self.market_metadata_ready.set()
        self.alert_metadata, self.decided, self.analysis_in_flight = {}, set(), set()
        boundary = previous_run_boundary(journal.path, journal.run_id)
        if boundary:
            resumed = Stamp.now()
            self.journal.emit("gap", {"stream": "application", "reason": "process_restart_interval",
                "previous_run_id": boundary["run_id"], "previous_boundary_kind": boundary["kind"],
                "previous_last_record_utc_ns": boundary["utc_ns"], "resumed_utc_ns": resumed.utc_ns,
                "wall_interval_ns": resumed.utc_ns-boundary["utc_ns"],
                "previous_shutdown_clean": boundary["kind"] == "run_end", "replay_completeness_verified": False}, resumed)
        for row in read_rows(journal.path, kinds=("run_start", "alert")):
            if row["kind"] == "run_start" and row["data"].get("mode") != "OBSERVED":
                raise ValueError("Observed and synthetic recordings require separate databases")
            if row["kind"] == "alert":
                self.first_receipts.setdefault(row["data"]["event_id"], (row["run_id"], row["stamp"]))
        self.announcement_clocks = AnnouncementClockRegistry(journal)

    @staticmethod
    def clock_record(binding):
        """JSON-safe immutable evidence; process monotonic values remain evidence."""
        result = deepcopy({k: v for k, v in binding.items() if k != "effective_receipt"})
        result.update(first_receipt_utc_ns=binding["effective_receipt"].utc_ns,
                      first_receipt_mono_ns=binding["effective_receipt"].mono_ns)
        return result

    def finalize_decision(self, event, receipt, result, decided_at=None):
        """Append a snapshot; a pre-header abort can supersede a ready candidate."""
        decided_at = decided_at or Stamp.now()
        snapshot = deepcopy(result)
        previous = self.final_decisions.get(event)
        supersedes = (snapshot.get("pre_dispatch_abort") is True and previous
            and previous.get("decision") == "paper_entry_ready"
            and snapshot.get("supersedes_candidate_client_id") == previous.get("client_order_id"))
        if event in self.decided and not supersedes:
            return
        snapshot.update(event_id=event, receipt_mono_ns=receipt.mono_ns,
            elapsed_from_receipt_ns=decided_at.mono_ns-receipt.mono_ns,
            completed_within_deadline=decided_at.mono_ns < receipt.mono_ns+DEADLINE_NS,
            live_orders_enabled=False)
        if supersedes:
            snapshot["supersedes_decision"] = "paper_entry_ready"
        self.decided.add(event)
        self.final_decisions[event] = deepcopy(snapshot)
        self.journal.emit("decision", snapshot, decided_at)

    def skip(self, event, receipt, reasons, *, binding=None, **extra):
        result = {"event_id": event, "decision": "skip", "orders_enabled": self.paper_enabled,
                  "paper": self.paper_enabled, "reasons": list(dict.fromkeys(reasons)), **extra}
        if binding is not None:
            result["announcement_clock"] = self.clock_record(binding)
        self.finalize_decision(event, receipt, result)

    async def checkpoint(self, event, receipt):
        deadline = receipt.mono_ns + DEADLINE_NS
        await asyncio.sleep(max(0, (deadline - time.monotonic_ns()) / 1e9))
        fired = Stamp.now()
        if self.quote_snapshots and event in self.alert_metadata:
            self.quote_snapshots.enqueue(event, self.alert_metadata[event]["ticker"], receipt, DEADLINE_NS)
        self.journal.emit("observation_window", {
            "event_id": event, "receipt_mono_ns": receipt.mono_ns,
            "target_mono_ns": deadline, "scheduler_lateness_ns": max(0, fired.mono_ns - deadline),
            "decision": "see_decision_record" if self.analysis_enabled else "not_implemented_recorder_only",
        }, fired)
        if self.analysis_enabled and event not in self.decided:
            self.finalize_decision(event, receipt, {"decision": "skip", "orders_enabled": self.paper_enabled,
                "reasons": ["five_second_deadline_expired", "complete_analysis_not_available"]}, fired)

    async def prepare_context(self, ticker, publication_ns):
        if self.context_store is None:
            return deepcopy(self.contexts.get(ticker))
        if publication_ns is None:
            return None
        return await asyncio.to_thread(self.context_store.prepare, ticker, publication_ns, mode="OBSERVED")

    @staticmethod
    def source_document(data):
        """Use the verified adapter's entire extracted text and original HTML."""
        source = next((data[k] for k in ("candidate_article_text", "text", "full_text", "article_body_text")
                       if isinstance(data.get(k), str) and data[k].strip()), None)
        raw_html = data.get("raw_html") if isinstance(data.get("raw_html"), str) else None
        problems = []
        if raw_html is None and data.get("raw_body_base64") and "html" in data.get("content_type", "").lower():
            try:
                raw_html = base64.b64decode(data["raw_body_base64"], validate=True).decode("utf-8", errors="replace")
            except (ValueError, TypeError):
                problems.append("invalid_original_html_encoding")
        if raw_html is None and (data.get("body_table_count", 0) or data.get("contains_html_tables")):
            problems.append("original_html_missing_for_financial_tables")
        return {"full_text_verified": data.get("full_text_verified") is True, "text": source,
                "raw_html": raw_html, **({"article_body_text": data["article_body_text"]}
                    if isinstance(data.get("article_body_text"), str) else {})}, problems

    @staticmethod
    def classify_source(document, company, ticker):
        """Deterministic relevance only. Failure to extract is never irrelevance."""
        issuer = verify_issuer(document, company)
        source = document.get("text")
        catalogue = None
        if document.get("full_text_verified") is True and isinstance(source, str) and source.strip():
            catalogue = build_catalogue(source, ticker, document.get("raw_html"))
        period = (catalogue or {}).get("current_period") or {}
        ending = period.get("period_end")
        names = {c["fact"]["name"] for c in (catalogue or {}).get("candidates", [])
            if c.get("usable_as_source_fact") and c["fact"].get("period_end") == ending
            and c["fact"].get("period_kind") == "QUARTER"}
        actuals = {"DILUTED_EPS", "REVENUE"} <= names
        relevance, reason = None, "current_quarter_actuals_or_primary_issuer_unverified"
        if issuer.get("verified") is True and ending and actuals:
            relevance, reason = True, "primary_issuer_current_quarter_eps_and_revenue"
        elif document.get("full_text_verified") is True and isinstance(source, str):
            body = document.get("article_body_text", source)
            opening = body.lstrip()[:500]
            if re.match(r"(?:SHAREHOLDER ALERT|INVESTOR ALERT|CLASS ACTION|LAW FIRM|LEGAL NOTICE|SECURITIES FRAUD|DEADLINE ALERT)\b", opening, re.I):
                relevance, reason = False, "definitive_legal_notice_opening"
            elif not actuals and re.search(r"\b(?:will\s+(?:report|release|announce)|scheduled\s+to\s+(?:report|release|announce))\b.{0,100}\b(?:results|earnings)\b", body[:1600], re.I):
                declarations = []
                for verb in VERB.finditer(body):
                    tail = body[verb.end():verb.end()+320]
                    prefix = body[max(0, verb.start()-80):verb.start()]
                    if RESULTS.match(tail) and PERIOD.search(tail[:240]) and not re.search(r"\b(?:will|would|plans? to|scheduled to|expects? to|expected to|going to)\s*$", prefix, re.I):
                        declarations.append(verb.start())
                if not declarations:
                    relevance, reason = False, "definitive_future_results_schedule_without_actuals"
        issuer["analysis_source_sha256"] = digest(source) if isinstance(source, str) else None
        return relevance, ending, issuer, catalogue, reason

    async def analyze_document(self, event, receipt, data, comparable):
        if event in self.analysis_in_flight:
            return
        self.analysis_in_flight.add(event)
        try:
            alert = self.alert_metadata[event]
            if event not in self.announcement_clocks.alerts:  # Direct test/adapter callback compatibility.
                self.announcement_clocks.observe_alert(event, alert["ticker"], alert.get("published_ns"), receipt)
            document, source_problems = await asyncio.to_thread(self.source_document, data)
            context_problem = None
            try:
                context = await self.prepare_context(alert["ticker"], alert.get("published_ns"))
            except Exception as exc:
                context, context_problem = None, "context_preparation_failed"
                self.journal.emit("gap", {"stream": "context", "event_id": event, "reason": context_problem,
                    "error_class": type(exc).__name__})
            try:
                relevance, ending, issuer, catalogue, classification_reason = await asyncio.to_thread(
                    self.classify_source, document, (context or {}).get("company"), alert["ticker"])
            except Exception as exc:
                relevance, ending, issuer, catalogue = None, None, {"verified": False}, None
                classification_reason = "deterministic_source_classification_failed"
                self.journal.emit("gap", {"stream": "source_classification", "event_id": event,
                    "reason": classification_reason, "error_class": type(exc).__name__})
            target = (context or {}).get("actual_period_end") or ((context or {}).get("event_plan") or {}).get("actual_period_end")
            target_kind = (context or {}).get("actual_period_kind", "QUARTER")
            if relevance is True and target and (target != ending or target_kind != "QUARTER"):
                relevance, classification_reason = None, "prepared_actual_period_target_conflicts_with_release"
            # This precedes all eligibility, model and deadline rejection, even if
            # the original version already produced a skip.
            binding = self.announcement_clocks.classify(event, relevance, ending if relevance else None,
                announcement_key=(context or {}).get("announcement_key") if relevance else None)
            self.journal.emit("source_classification", {"event_id": event, "relevant": relevance,
                "reason": classification_reason, "current_period_end": ending, "issuer_identity": issuer,
                "catalogue_source_sha256": (catalogue or {}).get("source_sha256"),
                "announcement_clock": self.clock_record(binding)})
            effective = binding["effective_receipt"]
            if event in self.decided or not self.analyzer:
                return
            if not comparable or not binding["comparable_in_current_run"]:
                self.skip(event, effective, ["first_relevant_receipt_from_prior_run_research_only"], binding=binding)
                return
            # Corrected IDs may have a later vendor publication field. Expectations
            # and identity must still be known before the first relevant publication.
            published_ns = binding["publication_ns"]
            if published_ns != alert.get("published_ns"):
                try:
                    context = await self.prepare_context(alert["ticker"], published_ns)
                    issuer = await asyncio.to_thread(verify_issuer, document, (context or {}).get("company"))
                    issuer["analysis_source_sha256"] = digest(document.get("text")) if isinstance(document.get("text"), str) else None
                except Exception as exc:
                    context, context_problem = None, "context_preparation_failed_at_original_publication"
                    self.journal.emit("gap", {"stream": "context", "event_id": event,
                        "reason": context_problem, "error_class": type(exc).__name__})
            if Stamp.now().mono_ns >= effective.mono_ns+DEADLINE_NS:
                self.skip(event, effective, ["five_second_deadline_expired"], binding=binding)
                return
            if document["full_text_verified"]:
                reasons = list(source_problems)
                if context_problem:
                    reasons.append(context_problem)
                if relevance is not True:
                    reasons.append(classification_reason)
                reasons.extend(binding["block_reasons"])
                if issuer.get("verified") is not True:
                    reasons.append("primary_issuer_identity_not_verified")
                if alert.get("published_ns") != data.get("document_published_ns"):
                    reasons.append("release_publication_time_does_not_match_alert")
                target = (context or {}).get("actual_period_end") or ((context or {}).get("event_plan") or {}).get("actual_period_end")
                target_kind = (context or {}).get("actual_period_kind", "QUARTER")
                if target and (target != ending or target_kind != "QUARTER"):
                    reasons.append("prepared_actual_period_target_conflicts_with_release")
                if reasons:
                    self.skip(event, effective, reasons, binding=binding)
                    return
            document.update(issuer_identity=deepcopy(issuer), announcement_clock=self.clock_record(binding))
            async def loaded():
                return deepcopy(document)
            result = await self.analyzer.process(event, alert["ticker"], effective, loaded,
                context=deepcopy(context), published_ns=published_ns, defer_execution_checks=self.paper_enabled)
            if event in self.decided:
                return
            if not self.announcement_clocks.is_current(binding):
                self.skip(event, effective, ["announcement_clock_changed_during_analysis"], binding=binding)
                return
            if self.paper_enabled and result.get("decision") == "pending_execution_checks" and not result.get("reasons"):
                deadline = effective.mono_ns+DEADLINE_NS
                if binding["block_reasons"] or document["full_text_verified"] is not True or issuer.get("verified") is not True:
                    self.skip(event, effective, binding["block_reasons"]+["paper_source_admission_not_verified"], binding=binding)
                    return
                if not await self.announcement_clocks.wait_durable(binding, deadline):
                    self.skip(event, effective, ["announcement_clock_not_durable_or_changed_before_deadline"], binding=binding)
                    return
                if self.paper_execution is None:
                    self.skip(event, effective, ["paper_execution_not_initialized"], binding=binding)
                    return
                def on_decision(decision, decided_at):
                    self.finalize_decision(event, effective, {**deepcopy(decision),
                        "announcement_clock": self.clock_record(binding)}, decided_at)
                await self.paper_execution.enter(deepcopy(result), effective, published_ns, deepcopy(context),
                    on_decision, admission_guard=lambda: self.announcement_clocks.is_current(binding))
            elif event not in self.decided:
                self.skip(event, effective, result.get("reasons", []), binding=binding)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.journal.emit("gap", {"stream": "analysis_pipeline", "event_id": event,
                "reason": "analysis_pipeline_failed", "error_class": type(exc).__name__})
            self.skip(event, receipt, ["analysis_pipeline_failed"])
        finally:
            self.analysis_in_flight.discard(event)

    def handle_wire(self, raw, received, documents, read=None):
        timing = ingress_metadata(received, read) if read is not None else {}
        self.journal.emit("wire_frame", {"raw": raw, **timing}, received)
        try:
            msg = json.loads(raw)
            if not isinstance(msg, dict):
                raise ValueError("expected object")
            kind = msg.get("type")
            if kind == "alert":
                alert = normalize_alert(msg)
                if read is not None:
                    alert.update(timing, full_message_available_utc_ns=read.utc_ns,
                                 full_message_available_mono_ns=read.mono_ns)
                event = alert["event_id"]
                # Record at the original recv boundary, before document queues.
                # All IDs participate, including duplicates and later entry skips.
                self.announcement_clocks.observe_alert(event, alert["ticker"], alert.get("published_ns"), received)
                repeated = event in self.first_receipts
                self.first_receipts.setdefault(event, (self.journal.run_id, received))
                first_run, first_stamp = self.first_receipts[event]
                same_run = first_run == self.journal.run_id
                alert.update(first_receipt_run_id=first_run, first_receipt_utc_ns=first_stamp.utc_ns,
                             first_deadline_utc_ns=first_stamp.utc_ns + DEADLINE_NS,
                             deadline_mono_ns=first_stamp.mono_ns + DEADLINE_NS if same_run else None,
                             research_only_repeat=repeated)
                self.journal.emit("alert", alert, received)
                self.alert_metadata.setdefault(event, alert)
                documents.enqueue(event, alert["article_url"], first_stamp if same_run else received,
                                  deadline_comparable=same_run)
                if repeated:
                    return kind
                if self.quote_snapshots:
                    self.quote_snapshots.enqueue(event, alert["ticker"], received)
                if len(self.checkpoints) < 2048:
                    task = asyncio.create_task(self.checkpoint(event, received))
                    self.checkpoints.add(task)
                    task.add_done_callback(self.checkpoints.discard)
                else:
                    self.journal.emit("gap", {"stream": "checkpoints", "reason": "checkpoint_capacity_exceeded"}, received)
            return kind
        except (ValueError, TypeError, KeyError, AttributeError):
            self.journal.emit("gap", {"stream": "rtpr", "reason": "unrecognized_frame_preserved"}, received)
            return "invalid"

    async def wire(self, documents):
        await self.market_metadata_ready.wait()
        url = "wss://ws.rtpr.io/ws-alerts?" + urlencode({"apiKey": required("RTPR_API_KEY")})
        failures = 0
        while True:
            self.journal.emit("transport", {"stream": "rtpr", "state": "connecting"})
            opened = time.monotonic_ns()
            error = {}
            try:
                async with connect(url, open_timeout=10, close_timeout=2, ping_interval=20,
                                   ping_timeout=20, max_size=2**20, max_queue=16,
                                   create_connection=StampedClientConnection) as ws:
                    self.journal.emit("transport", {"stream": "rtpr", "state": "handshake_accepted"})
                    while True:
                        raw, received, read = await asyncio.wait_for(receive_stamped(ws), timeout=75)
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="replace")
                        kind = self.handle_wire(raw, received, documents, read)
                        if kind == "ping":
                            await ws.send('{"type":"pong"}')
                            self.journal.emit("heartbeat", {"stream": "rtpr", "pong_sent": True})
                        elif kind == "connected":
                            self.journal.emit("transport", {"stream": "rtpr", "state": "ready"}, received)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                error = safe_error(exc)
            finally:
                self.journal.emit("transport", {"stream": "rtpr", "state": "disconnected", **error})
            failures = 0 if time.monotonic_ns() - opened > 90_000_000_000 else failures + 1
            delay = 300 if error.get("close_code") in (4001, 4004) or failures >= 6 else min(60, 2 ** max(1, failures))
            self.journal.emit("gap", {"stream": "rtpr", "reason": "disconnected_no_replay_guarantee", "retry_in_s": delay})
            await backoff(delay + random.uniform(0, 0.25))

    def handle_market(self, raw, received, read=None):
        timing = ingress_metadata(received, read) if read is not None else {}
        # A fragmented/queued quote wasn't available at the first frame's callback.
        # Raw transport timing and complete-message availability are different.
        available = read if read is not None else received
        self.journal.emit("market_frame", {"raw": raw, "feed": self.feed, **timing}, received)
        try:
            messages = json.loads(raw, parse_float=str)
            if not isinstance(messages, list):
                raise ValueError("expected array")
            for msg in messages:
                if not isinstance(msg, dict):
                    raise ValueError("expected object")
                kind = msg.get("T")
                if self.market_inputs:
                    if kind == "subscription":
                        self.market_inputs.status.subscribed(msg, available)
                    elif kind in ("q", "s"):
                        self.market_inputs.status.ingest(msg, available, self.feed)
                if kind in ("q", "t", "c", "x", "s", "l"):
                    record = {"feed": self.feed, "raw": msg, **timing,
                              "available_utc_ns": available.utc_ns, "available_mono_ns": available.mono_ns,
                              "calendar_session_at_receipt": self.calendar.session(available.utc_ns),
                              "exchange_halt_status": "unknown_unless_status_message_observed"}
                    self.journal.emit({"q": "quote", "t": "trade", "c": "trade_correction", "x": "trade_cancel",
                                       "s": "market_status", "l": "market_status"}[kind], record, available)
            return messages
        except (ValueError, TypeError, KeyError, AttributeError):
            if self.market_inputs:
                self.market_inputs.status.disconnected(available)
            self.journal.emit("gap", {"stream": "alpaca", "reason": "unrecognized_frame_preserved"}, available)
            return []

    async def prepare_market_inputs(self):
        """Prepare static condition evidence before receiving wire alerts.

        A failed preparation does not invent capability or stop observation. It is
        recorded and the adapter continues to reject unverified conditions.
        """
        try:
            if self.market_inputs is None:
                self.journal.emit("gap", {"stream": "market_inputs", "reason": "market_inputs_not_configured"})
                return
            results = await asyncio.wait_for(self.market_inputs.prewarm(), timeout=5)
            prepared = {r.get("tape") for r in results if isinstance(r, dict) and r.get("status") == "prepared"} if isinstance(results, list) else set()
            missing = sorted({"A", "B", "C"}-prepared)
            self.journal.emit("market_input_prewarm", {"feed": self.feed, "prepared_tapes": sorted(prepared),
                "complete": not missing, "status_baseline_established": False})
            if missing:
                failures = [{k: r.get(k) for k in ("tape", "status", "http_status", "error_class")}
                            for r in results if isinstance(r, dict) and r.get("status") != "prepared"] if isinstance(results, list) else []
                self.journal.emit("gap", {"stream": "market_inputs", "reason": "condition_metadata_not_fully_prepared",
                    "missing_tapes": missing, "failures": failures})
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.journal.emit("gap", {"stream": "market_inputs", "reason": "condition_metadata_prewarm_failed",
                                      "error_class": type(exc).__name__})
        finally:
            self.market_metadata_ready.set()

    async def market(self):
        headers = alpaca_headers()
        await self.prepare_market_inputs()
        failures = 0
        while True:
            self.journal.emit("transport", {"stream": "alpaca", "state": "connecting", "feed": self.feed})
            opened = time.monotonic_ns()
            error = {}
            try:
                async with connect(ALPACA_STREAM + self.feed, open_timeout=10, close_timeout=2,
                                   ping_interval=20, ping_timeout=20, max_size=4*2**20, max_queue=16,
                                   create_connection=StampedClientConnection) as ws:
                    if self.market_inputs:
                        self.market_inputs.status.connected(self.feed, Stamp.now())
                    await ws.send(json.dumps({"action": "auth", "key": headers["APCA-API-KEY-ID"],
                                              "secret": headers["APCA-API-SECRET-KEY"]}))
                    authenticated, subscribed = False, False
                    while True:
                        # Quiet markets may have no application traffic; protocol ping checks health.
                        raw, received, read = (await asyncio.wait_for(receive_stamped(ws), timeout=10)
                                              if not (authenticated and subscribed) else await receive_stamped(ws))
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="replace")
                        messages = self.handle_market(raw, received, read)
                        for msg in messages:
                            if msg.get("T") == "error":
                                error = {"vendor_error_code": msg.get("code")}
                                raise RuntimeError("market_data_access_error")
                            if msg.get("T") == "success" and msg.get("msg") == "authenticated" and not authenticated:
                                authenticated = True
                                await ws.send(json.dumps({"action": "subscribe", "quotes": self.symbols, "trades": self.symbols,
                                                          "statuses": self.symbols}))
                            if msg.get("T") == "subscription":
                                subscribed = set(self.symbols) <= set(msg.get("quotes", [])) and set(self.symbols) <= set(msg.get("trades", []))
                                status_subscribed = "*" in msg.get("statuses", []) or set(self.symbols) <= set(msg.get("statuses", []))
                                self.journal.emit("transport", {"stream": "alpaca", "state": "ready" if subscribed and status_subscribed else "partial_subscription",
                                                               "feed": self.feed, "quotes": msg.get("quotes", []),
                                                               "trades": msg.get("trades", []), "statuses": msg.get("statuses", []),
                                                               "quote_trade_subscription_confirmed": subscribed,
                                                               "status_subscription_confirmed": status_subscribed}, received)
                                if not status_subscribed:
                                    self.journal.emit("gap", {"stream": "market_status", "reason": "trading_action_subscription_not_confirmed",
                                                              "feed": self.feed}, received)
                        if not authenticated and time.monotonic_ns() - opened > 10_000_000_000:
                            raise TimeoutError("authentication_not_confirmed")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                error.update(safe_error(exc))
            finally:
                if self.market_inputs:
                    self.market_inputs.status.disconnected(Stamp.now())
                self.journal.emit("transport", {"stream": "alpaca", "state": "disconnected", "feed": self.feed, **error})
            failures = 0 if time.monotonic_ns() - opened > 90_000_000_000 else failures + 1
            delay = 300 if error.get("vendor_error_code") in (401, 402, 403, 405, 406, 409) or failures >= 6 else min(60, 2 ** max(1, failures))
            self.journal.emit("gap", {"stream": "alpaca", "reason": "disconnected_no_quote_replay", "retry_in_s": delay})
            await backoff(delay + random.uniform(0, 0.25))

    async def health(self):
        previous = Stamp.now()
        while True:
            await asyncio.sleep(1)
            self.journal.check()
            current = Stamp.now()
            lag = max(0, current.mono_ns - previous.mono_ns - 1_000_000_000)
            wall_step = (current.utc_ns - previous.utc_ns) - (current.mono_ns - previous.mono_ns)
            if lag > 100_000_000 or abs(wall_step) > 100_000_000:
                self.journal.emit("gap", {"stream": "clock", "reason": "event_loop_lag_or_wall_clock_step",
                                           "loop_lag_ns": lag, "wall_step_ns": wall_step}, current)
            previous = current

    async def run(self, seconds, report_callback=None):
        required("RTPR_API_KEY")
        alpaca_headers()
        async with httpx.AsyncClient(timeout=httpx.Timeout(10, connect=5), follow_redirects=False) as client:
            if not self._market_inputs_injected:
                self.market_inputs = MarketInputs(client, self.journal, calendar=self.calendar, feed=self.feed)
            self.market_metadata_ready.clear()
            self.quote_snapshots = QuoteSnapshots(self.journal, client, self.feed, self.calendar)
            if self.analysis_enabled:
                budget = ModelRequestBudget(self.journal)
                extractor = (CandidateExtractor(client, self.model, budget, self.journal) if self.extractor_mode == "candidates"
                             else OpenAIExtractor(client, self.model, budget))
                self.analyzer = TimedAnalyzer(extractor, self.journal)
            if self.paper_enabled:
                self.paper_ledger = await asyncio.to_thread(PaperLedger, self.paper_state, mode="OBSERVED")
                self.paper_broker = PaperBroker(client, self.journal, allow_orders=True)
                self.paper_execution = PaperExecution(self.paper_ledger, self.paper_broker, self.journal, self.calendar,
                    self.market_inputs, kill_path=self.kill_path)
            if self.reference_symbols:
                from .reference import ContextReferenceRecorder
                self.reference_recorder = ContextReferenceRecorder(self.context_store, self.market_inputs, self.journal,
                    symbols=list(self.reference_symbols), interval_s=self.reference_interval)
            if self.followup_symbols:
                from .research_followup import HistoricalResearchFollowup
                self.research_followup = HistoricalResearchFollowup(self.journal.path, self.followup_root, client,
                    symbols=list(self.followup_symbols), calendar=self.calendar, journal=self.journal)
            documents = DocumentCollector(self.journal, client,
                on_result=self.analyze_document if self.analysis_enabled or self.followup_symbols else None)
            tasks = [asyncio.create_task(self.wire(documents)), asyncio.create_task(self.market()),
                     asyncio.create_task(self.health())]
            if self.paper_enabled:
                tasks.append(asyncio.create_task(self.paper_execution.run()))
            refresh_metadata = getattr(self.market_inputs, "refresh_metadata", None)
            if callable(refresh_metadata):
                async def refresh_conditions():
                    await self.market_metadata_ready.wait()
                    await refresh_metadata()
                tasks.append(asyncio.create_task(refresh_conditions()))
            if self.reference_recorder:
                tasks.append(asyncio.create_task(self.reference_recorder.run()))
            if self.research_followup:
                tasks.append(asyncio.create_task(self.research_followup.run()))
            tasks += [asyncio.create_task(documents.worker()) for _ in range(documents.worker_count)]
            tasks += [asyncio.create_task(self.quote_snapshots.worker()) for _ in range(2)]
            if report_callback:
                async def reports():
                    while True:
                        try:
                            await asyncio.to_thread(report_callback)
                        except Exception as exc:
                            self.journal.emit("gap", {"stream": "report", "reason": "report_refresh_failed", "error_class": type(exc).__name__})
                        await asyncio.sleep(30)
                tasks.append(asyncio.create_task(reports()))
            async def lifetime():
                if seconds is None:
                    await asyncio.Event().wait()
                else:
                    await asyncio.sleep(seconds)
            duration = asyncio.create_task(lifetime())
            try:
                done, _ = await asyncio.wait([duration, *tasks], return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    if task is not duration:
                        task.result()
                        raise RuntimeError("recording task stopped unexpectedly")
            finally:
                for task in [duration, *tasks, *self.checkpoints]:
                    task.cancel()
                await asyncio.gather(duration, *tasks, *self.checkpoints, return_exceptions=True)
                if self.research_followup:
                    try:
                        await self.research_followup.aclose()
                    except Exception as exc:
                        self.journal.emit("gap", {"stream": "research_followup", "reason": "research_shutdown_failed",
                            "error_class": type(exc).__name__})
                self.journal.emit("run_end", {"reason": "observation_stopped", "document_jobs_not_started": documents.pending.qsize(),
                                              "quote_snapshot_jobs_not_started": self.quote_snapshots.pending.qsize()})


def run_metadata(symbols, feed, synthetic=False):
    # Record exact production source hashes before opening any stream. Git commits
    # preserve the source; uncommitted variants remain distinguishable by hash.
    manifest = {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted(Path(__file__).parent.glob("*.py"))}
    return {"schema_version": 1, "mode": "SYNTHETIC" if synthetic else "OBSERVED",
            "symbols": sorted(set(symbols)), "feed": feed, "orders_enabled": False,
            "paper_enabled_requested": False, "live_orders_enabled": False,
            "receipt_boundary": RECEIPT_BOUNDARY, "ingress_version": INGRESS_VERSION,
            "unmeasured_before_receipt": UNMEASURED_BEFORE_RECEIPT,
            "market_availability_boundary": "complete-message application read; first-fragment callback retained separately",
            "local_clock_precision": "time_ns representation; synchronization accuracy unverified",
            "deadline_ns": DEADLINE_NS,
            "document_adapter_version": ARTICLE_ADAPTER_VERSION,
            "implementation_manifest": manifest,
            "implementation_manifest_sha256": hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest(),
            "versions": {name: importlib.metadata.version(name) for name in ("httpx", "websockets", "exchange-calendars")}}
