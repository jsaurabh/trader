"""Pre-registered, delayed SIP price diagnostics for observed primary earnings.

This optional background recorder never grants entry authority. Inclusion uses the
observed source classification, before fetching prices; later quote archives cannot
establish past system availability, trading status, fills or a strategy return.
"""
import asyncio
from contextlib import closing, contextmanager
from datetime import date, datetime
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
from uuid import uuid4
from zoneinfo import ZoneInfo

import httpx

from .decision import next_exit
from .journal import Journal, row_record
from .locking import CaptureLock
from .timebase import MarketCalendar, SECOND, Stamp, iso_utc, parse_rfc3339

VERSION = "prospective_historical_price_followup_v1"
MODE = "HISTORICAL_DIAGNOSTIC"
MIN_AGE_NS = 16 * 60 * SECOND
ENDPOINT = "https://data.alpaca.markets/v2/stocks/quotes"
MAX_PAGE_BYTES = 8 * 1024 * 1024
QUOTE_FIELDS = {"t", "bp", "ap", "bs", "as", "bx", "ax", "c", "z"}
SYMBOL = re.compile(r"^[A-Z][A-Z0-9./-]{0,14}$")
ALLOWED_BLOCKS = {"first_relevant_receipt_from_prior_run_research_only"}
LIMITATIONS = ["Price-only diagnostic; not a qualified trade, fill or strategy return",
              "Historical quotes retrieved later were not known to the system at the target",
              "Archive can include later corrections; original quote version availability unknown",
              "Quote-size units and current TradingAction/status are unverified; no execution inference",
              "No point-in-time consensus or eligibility is inferred from source relevance"]


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _hash(value):
    return hashlib.sha256(value.encode()).hexdigest()


def _private_file(path):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    os.close(fd)


class HistoricalResearchFollowup:
    """One shared HTTP client; no account changes, model calls or trading methods.

    ``symbols`` is an explicit watchlist of one to three symbols. ``run`` polls at
    least once per minute without blocking ingestion. ``scan_once`` registers
    cases; ``run_due_once`` fetches at most two due windows. ``snapshot`` returns
    private state for a research report. Close with ``await aclose()`` on shutdown.
    State/history files must be separate from the capture database.
    """
    def __init__(self, journal_path, history_root, client, *, symbols, calendar=None,
                 clock=Stamp.now, journal=None, interval_s=60, max_pages=5,
                 max_attempts=3, retry_s=60):
        if (not isinstance(symbols, (list, tuple)) or not 1 <= len(symbols) <= 3
                or len(set(symbols)) != len(symbols)
                or any(not isinstance(s, str) or not SYMBOL.fullmatch(s) for s in symbols)):
            raise ValueError("one_to_three_explicit_unique_followup_symbols_required")
        if (isinstance(interval_s, bool) or not isinstance(interval_s, (int, float))
                or not math.isfinite(interval_s) or interval_s < 60):
            raise ValueError("followup_poll_interval_must_be_at_least_sixty_seconds")
        if type(max_pages) is not int or not 1 <= max_pages <= 20:
            raise ValueError("followup_page_budget_must_be_one_to_twenty")
        if type(max_attempts) is not int or not 1 <= max_attempts <= 3:
            raise ValueError("followup_attempt_budget_must_be_one_to_three")
        if type(retry_s) is not int or retry_s < 60:
            raise ValueError("followup_retry_interval_must_be_at_least_sixty_seconds")
        if client is None or not callable(getattr(client, "stream", None)):
            raise ValueError("shared_async_http_client_required")
        self.journal_path = Path(journal_path).resolve()
        self.root = Path(history_root).resolve()
        self.state_path = self.root / "followup-state.sqlite"
        if self.state_path == self.journal_path:
            raise ValueError("followup_state_must_not_be_capture_database")
        self.client, self.symbols, self.calendar = client, tuple(sorted(symbols)), calendar
        self.clock, self.journal = clock, journal
        self.implementation_source = Path(__file__).read_text()
        self.implementation_sha256 = _hash(self.implementation_source)
        self.interval_s, self.max_pages = float(interval_s), max_pages
        self.max_attempts, self.retry_ns = max_attempts, retry_s * SECOND
        self._lock = None
        self._io_lock, self._fetch_lock = asyncio.Lock(), asyncio.Lock()
        self._closed = False
        self._state_verified = False

    @contextmanager
    def _connect(self):
        c = sqlite3.connect(self.state_path, timeout=2)
        c.row_factory = sqlite3.Row
        try:
            if self._state_verified:
                c.execute("PRAGMA synchronous=FULL")
            with c:
                yield c
        finally:
            c.close()

    def _ensure(self):
        if self._closed:
            raise RuntimeError("followup_recorder_closed")
        if self._lock is not None:
            return
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.root, 0o700)
        self._lock = CaptureLock(self.state_path)
        try:
            if not self.state_path.exists():
                _private_file(self.state_path)
            with self._connect() as c:
                tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                if tables and "meta" not in tables:
                    raise ValueError("followup_state_database_mode_unverified")
                expected = {"mode": MODE, "version": VERSION, "source_journal": str(self.journal_path),
                            "symbols": _json(self.symbols)}
                existing = dict(c.execute("SELECT key,value FROM meta")) if "meta" in tables else {}
                if tables and any(existing.get(k) != v for k, v in expected.items()):
                    # Inspect foreign state before changing settings, schemas or
                    # permissions. Never repurpose a broker/capture database.
                    raise ValueError("followup_state_configuration_or_mode_mismatch")
                os.chmod(self.state_path, 0o600)
                c.execute("PRAGMA journal_mode=WAL")
                c.execute("PRAGMA synchronous=FULL")
                c.executescript("""
                    CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                    CREATE TABLE IF NOT EXISTS cases (
                      case_id TEXT PRIMARY KEY, canonical_key TEXT UNIQUE NOT NULL, ticker TEXT NOT NULL,
                      status TEXT NOT NULL, plan TEXT NOT NULL, registered_ns INTEGER NOT NULL);
                    CREATE TABLE IF NOT EXISTS aliases (alias TEXT PRIMARY KEY, case_id TEXT NOT NULL);
                    CREATE TABLE IF NOT EXISTS evidence (
                      source_seq INTEGER PRIMARY KEY, kind TEXT NOT NULL, case_id TEXT,
                      status TEXT NOT NULL, reasons TEXT NOT NULL, data TEXT NOT NULL);
                    CREATE TABLE IF NOT EXISTS jobs (
                      job_id TEXT PRIMARY KEY, case_id TEXT NOT NULL, kind TEXT NOT NULL,
                      start_ns INTEGER NOT NULL, end_ns INTEGER NOT NULL, target_ns INTEGER NOT NULL,
                      due_ns INTEGER NOT NULL, status TEXT NOT NULL, attempt_count INTEGER NOT NULL DEFAULT 0,
                      next_attempt_ns INTEGER NOT NULL, result TEXT, UNIQUE(case_id,kind));
                    CREATE TABLE IF NOT EXISTS attempts (
                      attempt_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
                      path TEXT UNIQUE NOT NULL, status TEXT NOT NULL, started_ns INTEGER NOT NULL,
                      finished_ns INTEGER, result TEXT, UNIQUE(job_id,ordinal));
                """)
                if not existing:
                    c.executemany("INSERT INTO meta VALUES (?,?)", [*expected.items(), ("cursor", "0")])
                # The process lock proves no other recorder owns these attempts.
                # A restart appends a new bounded attempt; never overwrites old pages.
                now = self.clock().utc_ns
                c.execute("UPDATE attempts SET status='interrupted',finished_ns=? WHERE status='running'", (now,))
                c.execute("""UPDATE jobs SET status=CASE WHEN attempt_count<? THEN 'retry' ELSE 'failed' END,
                             next_attempt_ns=? WHERE status='running'""", (self.max_attempts, now + self.retry_ns))
            self._state_verified = True
        except BaseException:
            self._state_verified = False
            self._lock.close()
            self._lock = None
            raise

    def _source_batch(self, cursor):
        uri = self.journal_path.as_uri() + "?mode=ro"
        with closing(sqlite3.connect(uri, uri=True, timeout=2)) as c:
            runs = {r[1]: json.loads(r[5]) for r in c.execute("SELECT * FROM journal WHERE kind='run_start'")}
            if not runs or any(m.get("mode") != "OBSERVED" for m in runs.values()):
                raise ValueError("followup_requires_separate_strictly_OBSERVED_capture_journal")
            rows = [row_record(r) for r in c.execute("""SELECT * FROM journal WHERE seq>? AND
                kind IN ('source_classification','announcement_clock_binding') ORDER BY seq LIMIT 2000""", (cursor,))]
            details = {}
            for row in rows:
                if row["kind"] != "source_classification":
                    continue
                event = row["data"].get("event_id")
                binding = c.execute("""SELECT * FROM journal WHERE kind='announcement_clock_binding'
                    AND json_extract(data,'$.event_id')=? ORDER BY seq DESC LIMIT 1""", (event,)).fetchone()
                b = json.loads(binding[5]) if binding else row["data"].get("announcement_clock", {})
                b = b if isinstance(b, dict) else {}
                first = c.execute("""SELECT * FROM journal WHERE kind='alert' AND run_id=?
                    AND json_extract(data,'$.event_id')=? ORDER BY seq LIMIT 1""",
                    (b.get("first_run_id"), b.get("first_event_id"))).fetchone()
                details[row["seq"]] = (b, row_record(first) if first else None)
            return rows, details, runs

    @staticmethod
    def _pin(binding):
        return [binding.get(k) for k in ("first_event_id", "first_run_id", "first_receipt_utc_ns", "first_receipt_mono_ns")]

    def _plan(self, row, binding, first, runs):
        d = row["data"]
        issuer = d.get("issuer_identity", {})
        issuer = issuer if isinstance(issuer, dict) else {}
        ticker, period = issuer.get("ticker") or binding.get("ticker"), d.get("current_period_end")
        reasons = []
        if ticker not in self.symbols:
            return None, ["symbol_not_in_explicit_followup_watchlist"]
        if d.get("relevant") is not True or issuer.get("verified") is not True:
            reasons.append("verified_primary_quarter_relevance_required")
        if any(not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value)
               for value in (issuer.get("source_sha256"), issuer.get("analysis_source_sha256"),
                             d.get("catalogue_source_sha256"))):
            reasons.append("verified_primary_source_version_hashes_required")
        if issuer.get("analysis_source_sha256") != d.get("catalogue_source_sha256"):
            reasons.append("issuer_and_catalogue_document_version_mismatch")
        company = issuer.get("canonical_company_id")
        if not isinstance(company, str) or not company:
            reasons.append("canonical_company_identity_required")
        try:
            if not isinstance(period, str) or date.fromisoformat(period).isoformat() != period:
                raise ValueError
        except ValueError:
            reasons.append("current_quarter_period_end_required")
        aliases = binding.get("identity_aliases", [])
        quarter_alias = f"period:{ticker}:QUARTER:{period}"
        if (binding.get("relevant") is not True or binding.get("first_ticker") != ticker
                or not isinstance(aliases, list) or quarter_alias not in aliases):
            reasons.append("canonical_current_quarter_binding_required")
        blocks = binding.get("block_reasons", [])
        if not isinstance(blocks, list) or any(not isinstance(x, str) for x in blocks) or set(blocks) - ALLOWED_BLOCKS:
            reasons.append("unresolved_canonical_clock_or_identity")
        pin = self._pin(binding)
        if (not first or len(pin) != 4 or any(type(x) is not int for x in pin[2:])
                or runs.get(binding.get("first_run_id"), {}).get("mode") != "OBSERVED"
                or first["stamp"] != Stamp(pin[2], pin[3])
                or first["data"].get("ticker") != ticker
                or first["data"].get("collector_source") != "RTPR"):
            reasons.append("original_observed_wire_receipt_not_reconstructible")
        published = binding.get("publication_ns")
        if type(published) is not int:
            reasons.append("original_publication_timestamp_missing")
        if reasons:
            return None, reasons
        cal = self.calendar or MarketCalendar(published)
        publication_session, receipt_session = cal.session(published), cal.session(pin[2])
        if publication_session not in ("pre_market", "after_hours"):
            return None, ["publication_outside_supported_extended_hours_calendar"]
        if receipt_session not in ("pre_market", "regular", "after_hours"):
            return None, ["receipt_outside_supported_calendar"]
        try:
            exit_ns = next_exit(cal, published)
        except (ValueError, KeyError, OverflowError):
            return None, ["scheduled_exit_outside_calendar"]
        if exit_ns <= pin[2] + 5 * SECOND:
            return None, ["scheduled_exit_not_after_receipt_window"]
        canonical = f"{company}:QUARTER:{period}"
        metadata = runs[pin[1]]
        return {"mode": MODE, "followup_version": VERSION, "canonical_key": canonical,
                "ticker": ticker, "period_kind": "QUARTER", "period_end": period,
                "canonical_company_id": company, "source_pin": pin,
                "original_announcement_clock": binding, "original_alert": first["data"],
                "source_journal": str(self.journal_path), "source_run_id": row["run_id"],
                "classification_source_seq": row["seq"], "classification": d,
                "receipt_boundary": metadata.get("receipt_boundary", "application boundary; exact definition absent"),
                "clock_precision": metadata.get("local_clock_precision", "accuracy unverified"),
                "source_fixture_type": metadata.get("fixture_type"),
                "publication_ns": published, "publication_precision_ns": first["data"].get("published_precision_ns"),
                "publication_meaning": first["data"].get("publication_meaning", "vendor field meaning unknown"),
                "earliest_public_availability_established": False,
                "entry_targets_ns": [pin[2], pin[2] + 5 * SECOND], "scheduled_exit_ns": exit_ns,
                "publication_session": publication_session, "receipt_session": receipt_session,
                "inclusion_policy": "deterministic observed primary issuer/current quarter relevance; independent of decision or prices",
                "qualified_trade": False, "entry_authority": False, "limitations": LIMITATIONS}, []

    def _bind_update(self, c, row):
        b = row["data"]
        if not isinstance(b, dict):
            return
        aliases = b.get("identity_aliases", [])
        if not isinstance(aliases, list):
            return
        aliases = [a for a in [*aliases, "event:" + str(b.get("event_id"))] if isinstance(a, str)]
        if not aliases:
            return
        matches = list(c.execute("SELECT DISTINCT case_id FROM aliases WHERE alias IN (" + ",".join("?" for _ in aliases) + ")", aliases))
        for (case_id,) in matches:
            case = c.execute("SELECT * FROM cases WHERE case_id=?", (case_id,)).fetchone()
            plan = json.loads(case["plan"])
            blocks = b.get("block_reasons", [])
            conflict = (self._pin(b) != plan["source_pin"] or b.get("publication_ns") != plan["publication_ns"]
                        or b.get("relevant") is not True or not isinstance(blocks, list)
                        or any(not isinstance(x, str) for x in blocks) or set(blocks) - ALLOWED_BLOCKS
                        or b.get("first_ticker") != plan["ticker"])
            status = "clock_conflict" if conflict else "source_version_appended"
            c.execute("INSERT OR IGNORE INTO evidence VALUES (?,?,?,?,?,?)",
                      (row["seq"], row["kind"], case_id, status,
                       _json(["original_clock_changed_or_unresolved"] if conflict else []), _json(b)))
            if conflict:
                c.execute("UPDATE cases SET status='clock_conflict' WHERE case_id=?", (case_id,))
                c.execute("UPDATE jobs SET status='cancelled_clock_conflict' WHERE case_id=? AND status IN ('pending','retry')", (case_id,))
            else:
                for alias in aliases:
                    c.execute("INSERT OR IGNORE INTO aliases VALUES (?,?)", (alias, case_id))

    def _scan(self):
        self._ensure()
        with self._connect() as c:
            cursor = int(c.execute("SELECT value FROM meta WHERE key='cursor'").fetchone()[0])
            rows, details, runs = self._source_batch(cursor)
            registered = skipped = 0
            for row in rows:
                if row["kind"] == "announcement_clock_binding":
                    self._bind_update(c, row)
                    continue
                binding, first = details[row["seq"]]
                try:
                    plan, reasons = self._plan(row, binding, first, runs)
                except (ValueError, TypeError, KeyError, OverflowError, OSError) as exc:
                    plan, reasons = None, ["invalid_source_or_calendar:" + type(exc).__name__]
                if plan is None:
                    skipped += 1
                    c.execute("INSERT OR IGNORE INTO evidence VALUES (?,?,?,?,?,?)",
                              (row["seq"], row["kind"], None, "skipped", _json(reasons), _json(row["data"])))
                    continue
                case_id = _hash(plan["canonical_key"])
                aliases = [*binding["identity_aliases"], "event:"+row["data"]["event_id"]]
                linked = {r[0] for r in c.execute("SELECT DISTINCT case_id FROM aliases WHERE alias IN (" +
                            ",".join("?" for _ in aliases) + ")", aliases)}
                if linked - {case_id}:
                    # A shared event/quarter alias cannot create a second set of
                    # jobs merely because a later source names a different issuer.
                    for conflict_id in linked:
                        c.execute("UPDATE cases SET status='clock_conflict' WHERE case_id=?", (conflict_id,))
                        c.execute("UPDATE jobs SET status='cancelled_clock_conflict' WHERE case_id=? AND status IN ('pending','retry')", (conflict_id,))
                    c.execute("INSERT OR IGNORE INTO evidence VALUES (?,?,?,?,?,?)",
                              (row["seq"], row["kind"], None, "identity_conflict", '["canonical_alias_conflicts_with_registered_issuer"]', _json(row["data"])))
                    skipped += 1
                    continue
                existing = c.execute("SELECT * FROM cases WHERE case_id=?", (case_id,)).fetchone()
                if existing:
                    original = json.loads(existing["plan"])
                    if original["source_pin"] != plan["source_pin"] or original["publication_ns"] != plan["publication_ns"]:
                        c.execute("UPDATE cases SET status='clock_conflict' WHERE case_id=?", (case_id,))
                        c.execute("UPDATE jobs SET status='cancelled_clock_conflict' WHERE case_id=? AND status IN ('pending','retry')", (case_id,))
                    status = "source_version_appended"
                else:
                    registered += 1
                    now = self.clock().utc_ns
                    c.execute("INSERT INTO cases VALUES (?,?,?,?,?,?)", (case_id, plan["canonical_key"], plan["ticker"], "included", _json(plan), now))
                    # Case and both immutable plans commit together before any HTTP.
                    entry = plan["entry_targets_ns"][0]
                    for kind, start, end, target in (("entry", entry-SECOND, entry+6*SECOND, entry),
                                                     ("exit", plan["scheduled_exit_ns"]-2*SECOND,
                                                      plan["scheduled_exit_ns"]+2*SECOND, plan["scheduled_exit_ns"])):
                        due = end + MIN_AGE_NS
                        c.execute("INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                                  (case_id+":"+kind, case_id, kind, start, end, target, due, "pending", 0, due, None))
                    status = "preregistered_before_price_fetch"
                c.execute("INSERT OR IGNORE INTO evidence VALUES (?,?,?,?,?,?)",
                          (row["seq"], row["kind"], case_id, status, "[]", _json(row["data"])))
                for alias in aliases:
                    c.execute("INSERT OR IGNORE INTO aliases VALUES (?,?)", (alias, case_id))
            if rows:
                c.execute("UPDATE meta SET value=? WHERE key='cursor'", (str(rows[-1]["seq"]),))
            return {"mode": MODE, "registered": registered, "skipped": skipped, "source_rows": len(rows),
                "case_count": c.execute("SELECT count(*) FROM cases").fetchone()[0],
                "case_status_counts": dict(c.execute("SELECT status,count(*) FROM cases GROUP BY status")),
                "job_status_counts": dict(c.execute("SELECT status,count(*) FROM jobs GROUP BY status"))}

    def _claim(self):
        self._ensure()
        now = self.clock().utc_ns
        with self._connect() as c:
            c.execute("BEGIN IMMEDIATE")
            row = c.execute("""SELECT jobs.*,cases.plan FROM jobs JOIN cases USING(case_id)
                WHERE jobs.status IN ('pending','retry') AND cases.status='included'
                AND due_ns<=? AND next_attempt_ns<=? AND attempt_count<? ORDER BY due_ns,job_id LIMIT 1""",
                (now, now, self.max_attempts)).fetchone()
            if row is None:
                return None
            job = dict(row)
            ordinal, attempt_id = job["attempt_count"] + 1, uuid4().hex
            path = self.root / job["case_id"] / f"{job['kind']}-{ordinal}-{attempt_id}.sqlite"
            c.execute("UPDATE jobs SET status='running',attempt_count=? WHERE job_id=?", (ordinal, job["job_id"]))
            c.execute("INSERT INTO attempts VALUES (?,?,?,?,?,?,?,?)",
                      (attempt_id, job["job_id"], ordinal, str(path), "running", now, None, None))
            return job | {"plan": json.loads(job["plan"]), "attempt_id": attempt_id, "ordinal": ordinal, "path": str(path)}

    def _finish(self, job, result):
        with self._connect() as c:
            previous = c.execute("SELECT status FROM attempts WHERE attempt_id=?", (job["attempt_id"],)).fetchone()
            if not previous or previous[0] != "running":
                raise RuntimeError("followup_attempt_cannot_be_overwritten")
            retry = result.get("retryable") is True and job["ordinal"] < self.max_attempts
            status = ("cancelled_clock_conflict" if result.get("status") == "clock_conflict" else
                      "complete" if result.get("complete") is True else "retry" if retry else "incomplete")
            c.execute("UPDATE attempts SET status=?,finished_ns=?,result=? WHERE attempt_id=?",
                      (status, self.clock().utc_ns, _json(result), job["attempt_id"]))
            c.execute("UPDATE jobs SET status=?,next_attempt_ns=?,result=? WHERE job_id=?",
                      (status, self.clock().utc_ns + self.retry_ns, _json(result), job["job_id"]))

    @staticmethod
    def _summaries(job, quotes, complete):
        from .history import source_quote_at
        targets = ([job["target_ns"], job["target_ns"]+5*SECOND] if job["kind"] == "entry" else [job["target_ns"]])
        return {iso_utc(t): (source_quote_at(quotes, t, job["start_ns"], True) if complete else
                {"status": "missing_incomplete_archive", "target_source_utc": iso_utc(t),
                 "source_pages_complete": False, "known_to_system_at_target": False, "live_execution_eligible": False})
                for t in targets}

    def _case_included(self, job):
        with self._connect() as c:
            row = c.execute("SELECT status,plan FROM cases WHERE case_id=?", (job["case_id"],)).fetchone()
            if row is None or row[0] != "included":
                return False
            plan = json.loads(row[1])
            alias = f"period:{plan['ticker']}:QUARTER:{plan['period_end']}"
            # Check the latest canonical evidence directly, even if a large
            # capture backlog exceeds this polling pass's incremental batch.
            with closing(sqlite3.connect(self.journal_path.as_uri()+"?mode=ro", uri=True, timeout=2)) as source:
                latest = source.execute("""SELECT * FROM journal WHERE kind='announcement_clock_binding'
                    AND EXISTS (SELECT 1 FROM json_each(journal.data,'$.identity_aliases') WHERE value=?)
                    ORDER BY seq DESC LIMIT 1""", (alias,)).fetchone()
            if latest:
                self._bind_update(c, row_record(latest))
            return c.execute("SELECT status FROM cases WHERE case_id=?", (job["case_id"],)).fetchone()[0] == "included"

    @staticmethod
    def _valid_quotes(values, job):
        if (not isinstance(values, dict) or set(values) - {job["plan"]["ticker"]}
                or any(not isinstance(v, list) for v in values.values())):
            return False
        try:
            return all(isinstance(q, dict) and not set(q) - QUOTE_FIELDS
                       and job["start_ns"] <= parse_rfc3339(q.get("t", ""))[0] <= job["end_ns"]
                       for qs in values.values() for q in qs)
        except (ValueError, TypeError, AttributeError):
            return False

    @classmethod
    def _decode_page(cls, raw, job):
        try:
            body = json.loads(raw, parse_float=str)
        except ValueError:
            return None, False
        values = body.get("quotes") if isinstance(body, dict) else None
        valid = isinstance(body, dict) and "next_page_token" in body and cls._valid_quotes(values, job)
        return body, valid

    async def _get_page(self, params):
        from .collectors import alpaca_headers
        async with asyncio.timeout(15), self.client.stream("GET", ENDPOINT, headers=alpaca_headers(), params=params,
                                                           timeout=httpx.Timeout(10, connect=5), follow_redirects=False) as response:
            if response.status_code != 200:
                return response.status_code, None, 0, False
            raw = bytearray()
            async for chunk in response.aiter_bytes(64 * 1024):
                if len(raw) + len(chunk) > MAX_PAGE_BYTES:
                    return response.status_code, None, len(raw)+len(chunk), True
                raw.extend(chunk)
            return response.status_code, raw.decode("utf-8", errors="replace"), len(raw), False

    async def _collect(self, job, archive):
        request = {"mode": MODE, "followup_version": VERSION, "job_id": job["job_id"], "kind": job["kind"],
                   "ticker": job["plan"]["ticker"], "feed": "sip", "start_utc": iso_utc(job["start_ns"]),
                   "end_utc": iso_utc(job["end_ns"]), "target_utc": iso_utc(job["target_ns"]),
                   "max_pages": self.max_pages, "minimum_age_ns": MIN_AGE_NS,
                   "anchor_meaning": "actual observed original wire receipt UTC" if job["kind"] == "entry" else "preregistered calendar open plus thirty minutes",
                   "retrieval_started_utc": iso_utc(self.clock().utc_ns), "live_execution_eligible": False}
        archive.emit("followup_historical_request", request, self.clock())
        quotes, cursor, seen, retrieval_times = [], None, set(), []
        result = {"mode": MODE, "status": "incomplete", "complete": False, "retryable": False,
                  "qualified_trade": False, "entry_authority": False, "feed": "sip", "pages": 0,
                  "price_comparison_computed": False, "limitations": LIMITATIONS}
        asof = datetime.fromtimestamp(job["target_ns"] // SECOND, ZoneInfo("America/New_York")).date().isoformat()
        try:
            for page in range(self.max_pages):
                params = {"symbols": request["ticker"], "start": request["start_utc"], "end": request["end_utc"],
                          "feed": "sip", "limit": 10000, "sort": "asc", "asof": asof}
                if cursor:
                    params["page_token"] = cursor
                started = self.clock()
                status, raw, size, too_large = await self._get_page(params)
                received = self.clock()
                retrieval_times.append(iso_utc(received.utc_ns))
                record = {"page": page+1, "http_status": status, "feed": "sip", "received_bytes": size,
                          "body_complete": not too_large, "maximum_page_bytes": MAX_PAGE_BYTES,
                          "request_started_utc": iso_utc(started.utc_ns), "raw": None}
                if status != 200 or too_large:
                    archive.emit("historical_page", record, received)
                    result.update(status="response_budget_exhausted" if too_large else "access_denied" if status in (401, 403) else "http_error",
                                  http_status=status, retryable=status in (408, 429, 500, 502, 503, 504))
                    break
                body, valid = await asyncio.to_thread(self._decode_page, raw, job)
                archive.emit("historical_page", record | {"raw": raw, "schema_valid": valid}, received)
                if not valid:
                    result["status"] = "unexpected_schema"
                    break
                quotes.extend(body["quotes"].get(request["ticker"], []))
                result["pages"] += 1
                cursor = body["next_page_token"]
                if cursor is None:
                    result.update(status="complete" if quotes else "empty_window", complete=True)
                    break
                if not isinstance(cursor, str) or not cursor or cursor in seen:
                    result["status"] = "invalid_or_repeated_page_token"
                    break
                seen.add(cursor)
            else:
                result["status"] = "page_budget_exhausted"
        except (httpx.TransportError, TimeoutError) as exc:
            result.update(status="network_error", retryable=True, error_class=type(exc).__name__)
        except Exception as exc:
            # Configuration/parse errors are visible and bounded, never a fallback.
            result.update(status="job_error", error_class=type(exc).__name__)
        result.update(quote_count=len(quotes), more_pages=bool(cursor), retrieval_times_utc=retrieval_times,
                      finished_utc=iso_utc(self.clock().utc_ns))
        return result, quotes

    def _health(self, status, **data):
        result = {"mode": MODE, "followup_version": VERSION, "status": status,
                  "entry_authority": False, **data}
        if self.journal is not None:
            self.journal.emit("research_followup_health", result, self.clock())
        return result

    async def scan_once(self):
        async with self._io_lock:
            result = await asyncio.to_thread(self._scan)
        self._health("scan_complete", **{k: v for k, v in result.items() if k != "mode"})
        return result

    async def run_due_once(self):
        # Recheck source changes before reserving a window, even when a caller
        # invokes this method without the usual polling-loop scan.
        await self.scan_once()
        results = []
        async with self._fetch_lock:
            for _ in range(2):
                async with self._io_lock:
                    job = await asyncio.to_thread(self._claim)
                if job is None:
                    break
                archive = None
                try:
                    # State registration/attempt reservation is already committed.
                    archive = await asyncio.to_thread(Journal, job["path"],
                        {"mode": MODE, "followup_version": VERSION, "feed": "sip",
                         "implementation_sha256": self.implementation_sha256,
                         "implementation_source": self.implementation_source,
                         "preregistered_plan": job["plan"], "job_kind": job["kind"], "attempt": job["ordinal"],
                         "qualified_trade": False, "orders_enabled": False}, started=self.clock())
                    await self.scan_once()
                    included = await asyncio.to_thread(self._case_included, job)
                    if included:
                        result, quotes = await self._collect(job, archive)
                        # A correction can arrive while HTTP is in flight. Keep
                        # exact pages but do not publish snapshots for old pins.
                        await self.scan_once()
                        included = await asyncio.to_thread(self._case_included, job)
                    else:
                        result, quotes = {"mode": MODE, "complete": False, "retryable": False}, []
                    if not included:
                        result.update(status="clock_conflict", archive_pages_complete=result["complete"],
                                      complete=False, case_research_eligible=False, snapshots={}, qualified_trade=False)
                    else:
                        result.update(case_research_eligible=True,
                            snapshots=await asyncio.to_thread(self._summaries, job, quotes, result["complete"]))
                    archive.emit("followup_historical_result", result, self.clock())
                    archive.emit("run_end", {"reason": "historical_followup_finished", "complete": result["complete"]}, self.clock())
                    await asyncio.to_thread(archive.close)
                    archive = None
                except asyncio.CancelledError:
                    async with self._io_lock:
                        await asyncio.shield(asyncio.to_thread(self._finish, job,
                            {"mode": MODE, "status": "interrupted", "complete": False, "retryable": True}))
                    raise
                except Exception as exc:
                    result = {"mode": MODE, "status": "archive_or_job_error", "complete": False,
                              "retryable": False, "error_class": type(exc).__name__, "qualified_trade": False}
                finally:
                    if archive is not None:
                        try:
                            await asyncio.to_thread(archive.close)
                        except Exception as exc:
                            result = {"mode": MODE, "status": "archive_error", "complete": False,
                                      "retryable": False, "error_class": type(exc).__name__, "qualified_trade": False}
                async with self._io_lock:
                    await asyncio.to_thread(self._finish, job, result)
                self._health("job_finished", job_id=job["job_id"], job_status=result["status"], complete=result["complete"])
                results.append(result | {"job_id": job["job_id"], "archive_path": job["path"]})
        return results

    def _snapshot(self):
        self._ensure()
        with self._connect() as c:
            output = {"mode": MODE, "followup_version": VERSION, "qualified_trade": False, "entry_authority": False,
                      "limitations": LIMITATIONS, "cases": [], "evidence": [], "jobs": [], "attempts": []}
            for table in ("cases", "evidence", "jobs", "attempts"):
                for row in c.execute("SELECT * FROM " + table):
                    value = dict(row)
                    for key in ("plan", "reasons", "data", "result"):
                        if value.get(key) is not None:
                            value[key] = json.loads(value[key])
                    output[table].append(value)
            return output

    async def snapshot(self):
        async with self._io_lock:
            return await asyncio.to_thread(self._snapshot)

    async def run(self):
        while not self._closed:
            try:
                await self.run_due_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Research fails closed; retain state and keep the capture alive.
                self._health("research_paused", error_class=type(exc).__name__)
            await asyncio.sleep(self.interval_s)

    async def aclose(self):
        self._closed = True
        async with self._fetch_lock, self._io_lock:
            if self._lock is not None:
                await asyncio.to_thread(self._lock.close)
                self._lock = None
