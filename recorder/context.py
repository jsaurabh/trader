"""Point-in-time preparation. Receipt/derivation times cannot be supplied by an import."""
from contextlib import contextmanager
from datetime import date, datetime, time, timezone
import calendar as month_calendar
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
from urllib.parse import parse_qsl, urlsplit
from uuid import uuid4
from zoneinfo import ZoneInfo

import httpx

from .timebase import SECOND, MarketCalendar, Stamp, iso_utc, parse_rfc3339

VERSION = "prepared-context-v1"
LIQUIDITY_POLICY = "prior20_full_day_vwap_volume_proxy_excluding_announcement_day_v1"
KINDS = {"company", "liquidity", "reference", "eps", "revenue", "prior_guidance", "event_plan", "expectation_observation"}
UNITS = {"USD_PER_SHARE", "USD", "THOUSAND_USD", "MILLION_USD", "BILLION_USD", "UNSPECIFIED_CURRENCY_PER_SHARE",
         "UNSPECIFIED_CURRENCY", "THOUSAND_UNSPECIFIED_CURRENCY", "MILLION_UNSPECIFIED_CURRENCY", "BILLION_UNSPECIFIED_CURRENCY"}
SYMBOL = re.compile(r"^[A-Z][A-Z0-9./-]{0,14}$")
MAX_SOURCE_BYTES = 8*1024*1024
NY = ZoneInfo("America/New_York")


def exact_number(value):
    if not isinstance(value, str) or not re.fullmatch(r"-?\d+(?:\.\d+)?", value) or len(value) > 40:
        raise ValueError("exact_decimal_string_required")
    result = Decimal(value)
    if not result.is_finite():
        raise ValueError("finite_decimal_required")
    return result


def sha(value):
    raw = value if isinstance(value, bytes) else json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()


class VisibleText(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.hidden, self.parts = 0, []
    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self.hidden += 1
        if tag in ("p", "div", "tr", "br", "h1", "h2", "h3", "li"):
            self.parts.append("\n")
        if tag in ("td", "th"):
            self.parts.append("\t")
    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self.hidden = max(0, self.hidden-1)
    def handle_data(self, value):
        if not self.hidden:
            self.parts.append(value)


def document_text(body, content_type):
    text = body.decode("utf-8", errors="replace")
    if "html" in content_type:
        parser = VisibleText(); parser.feed(text); parser.close()
        return "\n".join(line.rstrip() for line in "".join(parser.parts).splitlines() if line.strip())
    return text


def json_pointer(value, pointer):
    if not isinstance(pointer, str) or (pointer and not pointer.startswith("/")):
        raise ValueError("invalid_json_pointer")
    for token in pointer.split("/")[1:] if pointer else []:
        token = token.replace("~1", "/").replace("~0", "~")
        value = value[int(token)] if isinstance(value, list) else value[token]
    return value


class InlineCover(HTMLParser):
    """Bind SEC cover security title, symbol and exchange by the same XBRL context."""
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.active, self.facts = [], []
    def handle_starttag(self, tag, attrs):
        if tag == "ix:nonnumeric":
            a=dict(attrs)
            self.active.append({"name":a.get("name","").lower(),"context":a.get("contextref"),"parts":[]})
    def handle_data(self, data):
        for current in self.active:
            current["parts"].append(data)
    def handle_endtag(self, tag):
        if tag == "ix:nonnumeric" and self.active:
            current=self.active.pop()
            self.facts.append({"name":current["name"],"context":current["context"],"value":" ".join("".join(current["parts"]).split())})


def inline_cover_facts(raw):
    parser=InlineCover();parser.feed(raw.decode("utf-8",errors="replace"));parser.close()
    return parser.facts


def sec_reporting_currency_proof(raw, cik):
    """Observe issuer-bound consolidated revenue/EPS USD units in a prior filing.

    This archives exact source proof, not a policy that converts a new release or
    consensus '$' into USD. Both facts must share one undimensioned CIK context.
    """
    source=raw.decode("utf-8",errors="replace")
    def attr(fragment,name):
        match=re.search(r"\b"+name+r'\s*=\s*[\"\x27]([^\"\x27]+)[\"\x27]',fragment,re.I)
        return match[1] if match else None
    contexts={}
    for match in re.finditer(r"<xbrli:context\b[^>]*>.*?</xbrli:context>",source,re.I|re.S):
        text=match.group();identifier=attr(text,"id")
        entity=re.search(r'<xbrli:identifier\s+scheme=[\"\x27]http://www.sec.gov/CIK[\"\x27]\s*>(\d+)</xbrli:identifier>',text,re.I)
        start=re.search(r"<xbrli:startDate>(\d{4}-\d{2}-\d{2})</xbrli:startDate>",text,re.I)
        end=re.search(r"<xbrli:endDate>(\d{4}-\d{2}-\d{2})</xbrli:endDate>",text,re.I)
        if not (identifier and entity and str(int(entity[1]))==str(int(cik)) and start and end) or re.search(r"xbrldi:(?:explicitMember|typedMember)",text,re.I):continue
        date.fromisoformat(start[1]);date.fromisoformat(end[1])
        if identifier in contexts and contexts[identifier]["raw"]!=text:return None
        contexts[identifier]={"start":start[1],"end":end[1],"raw":text,"span":[match.start(),match.end()]}
    units={}
    for match in re.finditer(r"<xbrli:unit\b[^>]*>.*?</xbrli:unit>",source,re.I|re.S):
        text=match.group();identifier=attr(text,"id")
        compact=re.sub(r"\s+","",text).lower()
        simple=re.fullmatch(r'<xbrli:unitid=[\"\x27][^\"\x27]+[\"\x27]><xbrli:measure>iso4217:usd</xbrli:measure></xbrli:unit>',compact)
        divided=re.fullmatch(r'<xbrli:unitid=[\"\x27][^\"\x27]+[\"\x27]><xbrli:divide><xbrli:unitnumerator><xbrli:measure>iso4217:usd</xbrli:measure></xbrli:unitnumerator><xbrli:unitdenominator><xbrli:measure>xbrli:shares</xbrli:measure></xbrli:unitdenominator></xbrli:divide></xbrli:unit>',compact)
        if simple or divided:
            if identifier in units and units[identifier]["raw"]!=text:return None
            units[identifier]={"currency_unit":"USD_PER_SHARE" if divided else "USD","raw":text,"span":[match.start(),match.end()]}
    facts={}
    for match in re.finditer(r"<ix:nonFraction\b[^>]*>.*?</ix:nonFraction>",source,re.I|re.S):
        text=match.group();opening=text.split(">",1)[0];name=attr(opening,"name");context=attr(opening,"contextRef");unit=attr(opening,"unitRef")
        expected={"us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax":"USD","us-gaap:Revenues":"USD","us-gaap:EarningsPerShareDiluted":"USD_PER_SHARE"}
        if name not in expected or context not in contexts or unit not in units or units[unit]["currency_unit"]!=expected[name]:continue
        if re.search(r"\bnil\s*=\s*[\"\x27]true",opening,re.I):continue
        metric="diluted_eps" if name.endswith("EarningsPerShareDiluted") else "revenue"
        facts.setdefault(context,{}).setdefault(metric,{"metric":metric,"name":name,"context":context,"unit_id":unit,"raw":text,"span":[match.start(),match.end()]})
    candidates=[(contexts[c]["end"],contexts[c]["start"],c,f) for c,f in facts.items() if set(f)=={"revenue","diluted_eps"}]
    if not candidates:return None
    end,start,context,selected=max(candidates)
    proof=[contexts[context]]+[units[selected[k]["unit_id"]] for k in ("revenue","diluted_eps")]+[selected[k] for k in ("revenue","diluted_eps")]
    return {"currency":"USD","issuer_cik":str(int(cik)),"period_start":start,"period_end":end,"context":context,
            "method":"same_issuer_undimensioned_SEC_revenue_and_diluted_EPS_unit_v1","proof":proof,
            "scope":"Historical filed consolidated financial facts only; not new release or consensus currency mapping",
            "automatic_comparison_currency_resolution":False}


class ContextStore:
    """Append-only local observations. Mode isolation prevents synthetic contexts going live.

    `available_at` is the later of document receipt and local derivation. A source's
    publication/as-of field remains a separate claim. A retrospective page retrieved
    today cannot satisfy yesterday's live knowledge cutoff.
    """
    def __init__(self, path, *, mode="OBSERVED", clock=Stamp.now):
        if mode not in ("OBSERVED", "SYNTHETIC"):
            raise ValueError("context_mode_must_be_explicit")
        self.path, self.mode, self.clock = Path(path), mode, clock
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600); os.close(fd)
        os.chmod(self.path, 0o600)
        with self.connection() as c:
            c.executescript("""CREATE TABLE IF NOT EXISTS context_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS context_documents(id TEXT PRIMARY KEY,source_url TEXT NOT NULL,
                source_kind TEXT NOT NULL,http_status INTEGER NOT NULL,content_type TEXT NOT NULL,raw BLOB NOT NULL,
                text TEXT NOT NULL,raw_sha256 TEXT NOT NULL,text_sha256 TEXT NOT NULL,received_utc_ns INTEGER NOT NULL,
                received_mono_ns INTEGER NOT NULL,source_claim TEXT NOT NULL,request_meta TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS context_facts(seq INTEGER PRIMARY KEY,id TEXT UNIQUE NOT NULL,
                ticker TEXT NOT NULL,kind TEXT NOT NULL,payload TEXT NOT NULL,document_ids TEXT NOT NULL,
                evidence TEXT NOT NULL,available_utc_ns INTEGER NOT NULL,derived_utc_ns INTEGER NOT NULL,
                verification TEXT NOT NULL,record_sha256 TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS context_selection ON context_facts(ticker,kind,available_utc_ns,seq);""")
            row = c.execute("SELECT value FROM context_meta WHERE key='mode'").fetchone()
            if row and row[0] != mode:
                raise ValueError("observed_and_synthetic_contexts_must_be_separate")
            c.execute("INSERT OR IGNORE INTO context_meta VALUES('mode',?)", (mode,))

    @contextmanager
    def connection(self):
        c = sqlite3.connect(self.path, timeout=1)
        c.row_factory = sqlite3.Row
        try:
            c.execute("PRAGMA journal_mode=WAL"); c.execute("PRAGMA synchronous=FULL")
            yield c
            c.commit()
        except BaseException:
            c.rollback(); raise
        finally:
            c.close()

    def add_document(self, source_url, body, *, source_kind, content_type="text/plain", http_status=200,
                     source_claim=None, request_meta=None):
        parsed = urlsplit(source_url)
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password
                or any(re.search(r"key|token|secret|signature|authorization", name, re.I) for name, _ in parse_qsl(parsed.query))):
            raise ValueError("public_https_source_without_credential_query_required")
        if not isinstance(body, bytes) or len(body) > MAX_SOURCE_BYTES:
            raise ValueError("source_byte_limit")
        received, identifier = self.clock(), str(uuid4())
        text = document_text(body, content_type)
        with self.connection() as c:
            c.execute("INSERT INTO context_documents VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", (identifier, source_url, source_kind,
                http_status, content_type, body, text, sha(body), sha(text.encode()), received.utc_ns, received.mono_ns,
                json.dumps(source_claim or {}), json.dumps(request_meta or {})))
        return identifier

    def document(self, identifier):
        with self.connection() as c:
            row = c.execute("SELECT * FROM context_documents WHERE id=?", (identifier,)).fetchone()
        if row is None:
            raise ValueError("unknown_context_document")
        return dict(row)

    def _verify_evidence(self, evidence, documents):
        if not isinstance(evidence, list) or not evidence:
            raise ValueError("context_evidence_required")
        for item in evidence:
            doc = documents.get(item.get("document_id"))
            if not doc or doc["http_status"] != 200:
                raise ValueError("evidence_requires_successful_known_document")
            if item.get("type") == "text":
                if not isinstance(item.get("passage"), str) or not item["passage"] or item["passage"] not in doc["text"]:
                    raise ValueError("context_passage_not_in_source")
            elif item.get("type") == "raw_text":
                raw=doc["raw"].decode("utf-8",errors="replace")
                if not isinstance(item.get("passage"),str) or not item["passage"] or item["passage"] not in raw:
                    raise ValueError("context_raw_passage_not_in_source")
            elif item.get("type") == "json_pointer":
                try:
                    actual = json_pointer(json.loads(doc["raw"], parse_float=str), item["pointer"])
                except (ValueError, TypeError, KeyError, IndexError):
                    raise ValueError("invalid_context_json_evidence") from None
                if actual != item.get("value"):
                    raise ValueError("context_json_value_not_in_source")
            elif item.get("type") == "derived":
                if not item.get("method_version") or not item.get("input_sha256"):
                    raise ValueError("derived_evidence_requires_recipe_and_inputs")
            elif item.get("type") == "inline_xbrl":
                required={key:item.get(key) for key in ("name","context","value")}
                if required not in inline_cover_facts(doc["raw"]):
                    raise ValueError("context_inline_xbrl_fact_not_in_source")
            else:
                raise ValueError("unknown_context_evidence_type")

    def add_fact(self, ticker, kind, payload, *, document_ids, evidence, verification=None):
        if not SYMBOL.fullmatch(ticker) or kind not in KINDS or not isinstance(payload, dict):
            raise ValueError("invalid_context_fact")
        if any(key in payload for key in ("available_at", "system_available_at", "comparability_verified")):
            raise ValueError("availability_and_verification_are_derived_not_imported")
        documents = {identifier: self.document(identifier) for identifier in document_ids}
        if not documents or any(doc["http_status"] != 200 for doc in documents.values()):
            raise ValueError("facts_require_successful_source_documents")
        self._verify_evidence(evidence, documents)
        if kind in ("eps", "revenue", "prior_guidance"):
            if not isinstance(payload.get("provider_series_id"), str) or not payload["provider_series_id"]:
                raise ValueError("financial_provider_series_id_required")
            date.fromisoformat(payload["period_end"])
            if payload.get("period_kind") not in ("QUARTER", "FISCAL_YEAR") or payload.get("basis") not in ("GAAP", "NON_GAAP", "UNSPECIFIED") or payload.get("unit") not in UNITS:
                raise ValueError("financial_period_basis_unit_required")
            fields = ("low", "high") if kind == "prior_guidance" else ("value",)
            values = [exact_number(payload[key]) for key in fields]
            if kind != "eps" and min(values) < 0 or kind == "prior_guidance" and values[0] > values[1]:
                raise ValueError("invalid_revenue_or_guidance_range")
            passages = "\n".join(item["passage"] if item["type"] == "text" else json.dumps(item.get("value", "")) for item in evidence)
            tokens = set()
            for raw in re.findall(r"(?<![\w.])\(?-?\$?\d[\d,]*(?:\.\d+)?\)?(?!\d|\.\d)", passages):
                clean = raw.replace("$", "").replace(",", "")
                if clean.startswith("(") and clean.endswith(")"):
                    clean = "-"+clean[1:-1]
                try:
                    tokens.add(exact_number(clean))
                except ValueError:
                    pass
            if not set(values) <= tokens:
                calculation = payload.get("calculation", {})
                if (kind == "eps" and calculation.get("method_version") == "cents_to_per_share_v1"
                        and exact_number(calculation["reported_cents"]) in tokens and re.search(r"\bcents\b", passages, re.I)
                        and values == [exact_number(calculation["reported_cents"])/100]):
                    pass
                elif (kind == "prior_guidance" and calculation.get("method_version") == "symmetric_guidance_range_v1"
                      and all(exact_number(calculation[k]) in tokens for k in ("midpoint", "tolerance_pct"))
                      and re.search(r"plus or minus|\+/-|±", passages, re.I)
                      and values == [exact_number(calculation["midpoint"])*(1-exact_number(calculation["tolerance_pct"])/100),
                                     exact_number(calculation["midpoint"])*(1+exact_number(calculation["tolerance_pct"])/100)]):
                    pass
                else:
                    raise ValueError("context_amount_not_in_selected_evidence")
            compact = re.sub(r"[^a-z0-9]", "", passages.lower())
            ending = date.fromisoformat(payload["period_end"])
            dates = (ending.isoformat(), f"{ending.strftime('%B')} {ending.day} {ending.year}", f"{ending.strftime('%b')} {ending.day} {ending.year}")
            mapped_period = False
            if not any(re.sub(r"[^a-z0-9]", "", item.lower()) in compact for item in dates):
                mapping = payload.get("period_mapping", {})
                try:
                    calendar_doc = documents[mapping["calendar_document_id"]]
                    master = json.loads(calendar_doc["raw"])
                    year = int(mapping["fiscal_year"])
                    quarter = mapping.get("fiscal_quarter")
                    recent = master["filings"]["recent"]
                    old_quarter_dates = {day for form, day in zip(recent["form"], recent["reportDate"]) if form == "10-Q"}
                    mapped_month = 12 if payload["period_kind"] == "FISCAL_YEAR" else int(quarter)*3
                    mapped = date(year, mapped_month, month_calendar.monthrange(year, mapped_month)[1])
                    quarter_words = {1:"first",2:"second",3:"third",4:"fourth"}
                    label_pattern = ((r"(?:fiscal|full)\s+year\s+"+str(year)+r"\b|\bFY\s*"+str(year)+r"\b") if payload["period_kind"] == "FISCAL_YEAR"
                                     else r"(?:"+quarter_words[int(quarter)]+r"[- ]quarter|Q"+str(quarter)+r").{0,12}?"+str(year)+r"\b|\b"+str(year)+r".{0,12}?(?:"+quarter_words[int(quarter)]+r"[- ]quarter|Q"+str(quarter)+r")\b")
                    # This is an expected calendar-period binding. The actual release still
                    # has to independently report the same end date; changed calendars skip.
                    valid_mapping = (mapping["method_version"] == "issuer_calendar_period_target_v1"
                        and ticker in master["tickers"] and master["fiscalYearEnd"] == "1231" and mapped == ending
                        and re.search(label_pattern, passages, re.I) and (payload["period_kind"] == "FISCAL_YEAR"
                            or int(quarter) == 3 and {f"{year}-03-31", f"{year}-06-30"} <= old_quarter_dates))
                except (KeyError, TypeError, ValueError, IndexError):
                    valid_mapping = False
                if not valid_mapping:
                    raise ValueError("context_period_end_not_in_selected_evidence")
                mapped_period = True
            if not mapped_period:
                date_pattern = "(?:"+re.escape(ending.isoformat())+"|"+ending.strftime("%B")+r"\s+0?"+str(ending.day)+r",?\s+"+str(ending.year)+"|"+ending.strftime("%b")+r"\s+0?"+str(ending.day)+r",?\s+"+str(ending.year)+")"
                label_pattern = r"(?:quarter|three months)" if payload["period_kind"] == "QUARTER" else r"(?:fiscal year|full year|year)"
                text_bound = re.search(label_pattern+r"[^.;]{0,80}?end(?:ed|ing)\s+"+date_pattern, passages, re.I)
                date_fields = [item for item in evidence if item["type"] == "json_pointer" and item.get("value") == payload["period_end"]]
                kind_fields = [item for item in evidence if item["type"] == "json_pointer" and item.get("value") == payload["period_kind"]]
                json_bound = any(a["document_id"] == b["document_id"] and a["pointer"].rsplit("/",1)[0] == b["pointer"].rsplit("/",1)[0] for a in date_fields for b in kind_fields)
                if not text_bound and not json_bound:
                    raise ValueError("context_period_kind_and_end_not_bound")
            if payload["period_kind"] == "QUARTER" and not re.search(r"quarter|three months|\bQ[1-4]\b", passages, re.I):
                raise ValueError("context_quarter_kind_not_supported")
            if payload["period_kind"] == "FISCAL_YEAR" and not re.search(r"fiscal year|year end|full year|twelve months|\bFY\s*\d{4}", passages, re.I):
                raise ValueError("context_year_kind_not_supported")
            if "UNSPECIFIED_CURRENCY" not in payload["unit"] and not re.search(r"\bUSD\b|U\.?S\.? dollars|United States dollars", passages, re.I):
                raise ValueError("context_usd_currency_not_supported")
            scale = next((word for prefix, word in (("THOUSAND_", "thousand"), ("MILLION_", "million"), ("BILLION_", "billion")) if payload["unit"].startswith(prefix)), None)
            if scale and not re.search(scale, passages, re.I):
                raise ValueError("context_scale_not_supported")
            if payload["basis"] == "GAAP" and not re.search(r"(?<!non-)(?<!non )\bGAAP\b", passages, re.I):
                raise ValueError("context_gaap_basis_not_supported")
            if payload["basis"] == "NON_GAAP" and not re.search(r"non[- ]GAAP", passages, re.I):
                raise ValueError("context_non_gaap_basis_not_supported")
            # A mapping is source evidence, not the assumption that Street/BNRI equals generic non-GAAP.
            mapping = verification or {}
            methodology_evidence = mapping.get("methodology_evidence", [])
            if methodology_evidence:
                self._verify_evidence(methodology_evidence, documents)
            verified = (mapping.get("comparability_verified") is True and bool(mapping.get("verified_by"))
                        and bool(mapping.get("methodology_id")) and bool(mapping.get("issuer_metric_definition_id"))
                        and set(mapping.get("definition_document_ids", [])) <= set(documents)
                        and len(set(mapping.get("definition_document_ids", []))) >= 2
                        and len({documents[i]["raw_sha256"] for i in mapping.get("definition_document_ids", []) if i in documents}) >= 2
                        and set(mapping.get("definition_document_ids", [])) <= {e["document_id"] for e in methodology_evidence})
            verification = {**mapping, "comparability_verified": verified}
        derived = self.clock()
        available = max(derived.utc_ns, *(doc["received_utc_ns"] for doc in documents.values()))
        identifier = str(uuid4())
        record = {"ticker": ticker, "kind": kind, "payload": payload, "document_ids": list(documents),
                  "evidence": evidence, "available_at": iso_utc(available), "verification": verification or {}, "version": VERSION}
        with self.connection() as c:
            c.execute("INSERT INTO context_facts(id,ticker,kind,payload,document_ids,evidence,available_utc_ns,derived_utc_ns,verification,record_sha256) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (identifier, ticker, kind, json.dumps(payload), json.dumps(list(documents)), json.dumps(evidence),
                 available, derived.utc_ns, json.dumps(verification or {}), sha(record)))
        return identifier

    def _records(self, ticker, publication_ns):
        with self.connection() as c:
            rows = list(c.execute("""WITH ranked AS (
                SELECT *,ROW_NUMBER() OVER (PARTITION BY kind,json_extract(payload,'$.period_end'),
                json_extract(payload,'$.period_kind'),json_extract(payload,'$.basis'),
                json_extract(payload,'$.provider_series_id'),json_extract(verification,'$.methodology_id')
                ORDER BY available_utc_ns DESC,seq DESC) AS version_rank
                FROM context_facts WHERE ticker=? AND available_utc_ns<?)
                SELECT * FROM ranked WHERE version_rank=1 ORDER BY available_utc_ns DESC,seq DESC""", (ticker, publication_ns)))
            identifiers = {i for row in rows for i in json.loads(row["document_ids"])}
            docs = {r["id"]: dict(r) for r in c.execute("SELECT id,source_url,raw_sha256,received_utc_ns FROM context_documents WHERE id IN ("+",".join("?" for _ in identifiers)+")", tuple(identifiers))} if identifiers else {}
        records = []
        for row in rows:
            payload, verification = json.loads(row["payload"]), json.loads(row["verification"])
            identifiers = json.loads(row["document_ids"])
            record = {**payload, "available_at": iso_utc(row["available_utc_ns"]), "source": "; ".join(docs[i]["source_url"] for i in identifiers),
                "context_record_id": row["id"], "context_record_sha256": row["record_sha256"], "source_documents": identifiers,
                "source_versions": {i: docs[i]["raw_sha256"] for i in identifiers}, "evidence": json.loads(row["evidence"]),
                "verification": verification, "kind": row["kind"], "mode": self.mode}
            if row["kind"] in ("eps", "revenue", "prior_guidance"):
                record.update(comparability_verified=verification.get("comparability_verified", False),
                              methodology_id=verification.get("methodology_id"), issuer_metric_definition_id=verification.get("issuer_metric_definition_id"),
                              methodology_evidence=verification.get("methodology_evidence", []))
            records.append(record)
        return records

    def prepare(self, ticker, publication_ns, *, actual_period_end=None, actual_period_kind=None,
                guidance_period_end=None, guidance_period_kind=None, mode="OBSERVED"):
        if mode != self.mode:
            raise ValueError("context_mode_does_not_match_consumer")
        records, gaps = self._records(ticker, publication_ns), []
        def latest(kind, period=None, period_kind=None, methodology=None, provider=None):
            candidates = [r for r in records if r["kind"] == kind and (period is None or r.get("period_end") == period)
                          and (period_kind is None or r.get("period_kind") == period_kind)
                          and (methodology is None or r.get("methodology_id") == methodology)
                          and (provider is None or r.get("provider_series_id") == provider)]
            if not candidates:
                gaps.append("missing_point_in_time:"+kind)
                return None
            # Each methodology is a separate series. A newer observation supersedes an older one,
            # but two different providers/methodologies are not silently merged or cherry-picked.
            series = {(r.get("provider_series_id"), r.get("methodology_id"), r.get("basis"), r.get("period_end"), r.get("period_kind")) for r in candidates}
            if kind in ("eps", "revenue", "prior_guidance") and len(series) > 1:
                gaps.append("ambiguous_prepared_series:"+kind)
                return None
            selected = candidates[0]
            if selected.get("expires_at") and parse_rfc3339(selected["expires_at"])[0] <= publication_ns:
                gaps.append("expired_prepared_record:"+kind)
                return None
            return selected
        plan = next((r for r in records if r["kind"] == "event_plan"), {})
        if plan.get("expires_at") and parse_rfc3339(plan["expires_at"])[0] <= publication_ns:
            gaps.append("expired_prepared_record:event_plan")
            plan = {}
        if plan.get("scheduled_date") and plan["scheduled_date"] != datetime.fromtimestamp(publication_ns//SECOND, NY).date().isoformat():
            gaps.append("publication_day_does_not_match_prepared_event_plan")
            plan = {}
        explicit_targets={"actual_period_end":actual_period_end,"actual_period_kind":actual_period_kind,
                          "guidance_period_end":guidance_period_end,"guidance_period_kind":guidance_period_kind}
        resolved={}
        for field,explicit in explicit_targets.items():
            planned=plan.get(field)
            if explicit is not None and planned is not None and explicit!=planned:
                raise ValueError("prepared_event_plan_target_conflict:"+field)
            resolved[field]=planned if planned is not None else explicit
        if resolved["actual_period_kind"] is None:
            resolved["actual_period_kind"]="QUARTER"
        for field,value in resolved.items():
            if value is None:continue
            if field.endswith("_end"):
                date.fromisoformat(value)
            elif value not in ("QUARTER","FISCAL_YEAR"):
                raise ValueError("invalid_prepared_period_kind:"+field)
        actual_period_end,actual_period_kind=resolved["actual_period_end"],resolved["actual_period_kind"]
        guidance_period_end,guidance_period_kind=resolved["guidance_period_end"],resolved["guidance_period_kind"]
        context = {"ticker": ticker, "context_version": VERSION, "mode": self.mode, "prepared_for_publication_utc": iso_utc(publication_ns),
                   **resolved,"event_plan":plan or None}
        if actual_period_end:
            for kind in ("eps", "revenue"):
                selected = latest(kind, actual_period_end, actual_period_kind, plan.get(kind+"_methodology_id"), plan.get(kind+"_provider_series_id"))
                if selected:
                    context[kind] = selected
        else:
            gaps.append("missing_explicit_actual_period_target")
        if guidance_period_end and guidance_period_kind:
            selected = latest("prior_guidance", guidance_period_end, guidance_period_kind, plan.get("guidance_methodology_id"), plan.get("guidance_provider_series_id"))
            if selected:
                context["prior_guidance"] = selected
        else:
            gaps.append("missing_explicit_guidance_period_target")
        for kind in ("company", "liquidity", "reference"):
            selected = latest(kind)
            if selected:
                context[kind] = selected
        for kind in ("eps", "revenue", "prior_guidance"):
            record = context.get(kind)
            if record and record["basis"] == "NON_GAAP" and not record["comparability_verified"]:
                gaps.append("unverified_metric_comparability:"+kind)
            if record and (record["basis"] == "UNSPECIFIED" or "UNSPECIFIED_CURRENCY" in record["unit"]):
                gaps.append("unverified_basis_or_currency:"+kind)
        if context.get("company", {}).get("classification_verified") is not True:
            gaps.append("common_stock_classification_not_verified")
        if context.get("company", {}).get("broker_precision_verified") is not True:
            gaps.append("broker_limit_precision_not_verified")
        if context.get("liquidity", {}).get("measurement_policy_selected") is not True:
            gaps.append("liquidity_measurement_policy_not_selected")
        liquidity = context.get("liquidity")
        if liquidity:
            expected_end = expected_liquidity_end(MarketCalendar(publication_ns), publication_ns)
            if parse_rfc3339(liquidity["last_session_close"])[0] != expected_end:
                gaps.append("liquidity_window_does_not_end_before_announcement_day")
        context["announcement_key"] = plan.get("announcement_key")
        if not context["announcement_key"]:
            gaps.append("missing_prepared_canonical_announcement_identity")
        context.update(ready=not gaps, gaps=list(dict.fromkeys(gaps)))
        context["snapshot_sha256"] = sha(context)
        return context


def company_from_asset(asset, *, tick_source=None):
    name = asset.get("name", "")
    explicit_common = bool(re.search(r"\bcommon (?:stock|shares)\b", name, re.I))
    excluded = bool(re.search(r"depositary|depository|\bADR\b|\bADS\b|preferred|\bETF\b|\bETN\b|\btrust\b|\bfund\b|\bwarrant|\bunits?\b|\bright[s]?\b", name, re.I))
    verified = (asset.get("class") == "us_equity" and asset.get("exchange") in ("NYSE", "NASDAQ", "AMEX", "ARCA", "BATS")
                and explicit_common and not excluded)
    return {"ticker": asset["symbol"], "asset_id": asset.get("id"), "name": name, "exchange": asset.get("exchange"),
        "security_type": "US_COMMON_STOCK" if verified else "UNVERIFIED", "classification_verified": verified,
        "classification_method": "explicit broker security name + US primary exchange, not asset class alone",
        "tradable": asset.get("tradable") is True and asset.get("status") == "active",
        "price_tick": "0.01" if tick_source else None, "tick_source": tick_source, "broker_precision_verified": bool(tick_source),
        "tick_meaning": "Conservative broker limit-price increment for shares >= $1, not asserted venue minimum tick"}


def company_from_sec_cover(asset, raw, *, tick_source=None):
    company=company_from_asset(asset,tick_source=tick_source)
    facts=inline_cover_facts(raw)
    excluded=r"depositary|depository|\bADR\b|\bADS\b|preferred|\bETF\b|\bETN\b|\btrust\b|\bfund\b|\bwarrant|\bunits?\b|\bright[s]?\b"
    def exchange(value):
        value=value.lower()
        if "nasdaq" in value:return "NASDAQ"
        if "arca" in value:return "ARCA"
        if value=="nyse" or "new york stock exchange" in value:return "NYSE"
        if "american" in value or value=="amex":return "AMEX"
        if "bats" in value or "cboe bzx" in value:return "BATS"
        return None
    matching=[]
    for symbol in facts:
        if symbol["name"]!="dei:tradingsymbol" or symbol["value"]!=asset["symbol"]:continue
        context=symbol["context"]
        titles=[f for f in facts if f["name"]=="dei:security12btitle" and f["context"]==context]
        exchanges=[f for f in facts if f["name"]=="dei:securityexchangename" and f["context"]==context and exchange(f["value"])==asset.get("exchange")]
        for title in titles:
            if (exchanges and re.search(r"common (?:stock|shares)",title["value"],re.I)
                    and not re.search(excluded,title["value"]+" "+asset.get("name",""),re.I)):
                matching.append([symbol,title,exchanges[0]])
    unique={json.dumps(group,sort_keys=True) for group in matching}
    if asset.get("class")=="us_equity" and len(unique)==1:
        company.update(security_type="US_COMMON_STOCK",classification_verified=True,
            classification_method="SEC inline XBRL cover title/symbol/exchange bound to one context",classification_proof=matching[0])
        registrants=[f for f in facts if f["name"]=="dei:entityregistrantname" and f["context"]==matching[0][0]["context"]]
        if len({f["value"] for f in registrants})==1:
            company.update(legal_issuer_name=registrants[0]["value"],issuer_identity_verified=True,
                           verified_issuer_aliases=[registrants[0]["value"]],issuer_name_proof=[registrants[0]])
        company["exchange_ticker_verified"]=True
    return company


def expected_liquidity_end(calendar, publication_ns):
    """Prior session close, excluding the announcement's New York trading date.

    This development policy was selected on data feasibility before holdout: full-day
    volume is not final at a near-16:00 earnings release. The same prior-day window
    applies to premarket and after-close announcements, without using future volume.
    """
    local_date = datetime.fromtimestamp(publication_ns//SECOND, NY).date()
    eligible = [int(row["close"].value) for session,row in calendar.calendar.schedule.iterrows() if session.date() < local_date]
    if len(eligible) < 20:
        raise ValueError("insufficient_prior_liquidity_sessions")
    return eligible[-1]


def daily_liquidity(bars, calendar, as_of_ns, *, publication_ns=None):
    """Declared full-day VWAP*volume proxy, including extended-hours volume.

    Daily bars are only final for this definition after 20:00 New York. It is not
    silently substituted for regular-session turnover or an executable-size model.
    """
    schedule = calendar.calendar.schedule
    eligible = []
    for session, row in schedule.iterrows():
        regular_close = int(row["close"].value)
        day = session.date()
        extended_end = int(datetime.combine(day, time(20), NY).timestamp())*SECOND
        if extended_end < as_of_ns:
            eligible.append((day.isoformat(), regular_close, extended_end))
    if publication_ns is not None:
        last_close = expected_liquidity_end(calendar, publication_ns)
        eligible = [row for row in eligible if row[1] <= last_close]
        if not eligible or eligible[-1][1] != last_close:
            raise ValueError("required_prior_session_full_day_not_final_or_available")
    expected = eligible[-20:]
    if len(expected) != 20:
        raise ValueError("insufficient_calendar_sessions")
    by_date = {}
    for bar in bars:
        timestamp = parse_rfc3339(bar["t"])[0]
        day = datetime.fromtimestamp(timestamp//SECOND, NY).date().isoformat()
        if day in by_date and by_date[day] != bar:
            raise ValueError("conflicting_daily_bar_versions")
        by_date[day] = bar
    values, inputs = [], []
    for day, closing, extended_end in expected:
        if day not in by_date:
            raise ValueError("missing_completed_session_bar")
        bar = by_date[day]
        vwap, volume = exact_number(str(bar["vw"])), exact_number(str(bar["v"]))
        if vwap <= 0 or volume < 0 or volume != volume.to_integral_value():
            raise ValueError("invalid_daily_vwap_or_volume")
        values.append(vwap*volume)
        inputs.append({"date": day, "bar": bar})
    return {"session_count": 20, "last_session_close": iso_utc(expected[-1][1]),
        "last_source_session_end": iso_utc(expected[-1][2]), "average_daily_dollar_volume": str(sum(values)/20),
        "measurement_method": "full_day_daily_vwap_times_volume_v1", "session_scope": "provider full-day bars, including extended-hours volume",
        "dollar_volume_is_proxy": True, "measurement_policy_selected": True, "liquidity_policy": LIQUIDITY_POLICY,
        "definition_note": "Daily price/volume eligibility differs by trade condition; not exact RTH dollar turnover",
        "input_sha256": sha(inputs), "input_sessions": inputs}


async def fetch_source(store, client, url, *, kind, params=None, headers=None):
    response = await client.get(url, params=params, headers=headers, timeout=15, follow_redirects=False)
    identifier = store.add_document(url, response.content, source_kind=kind, content_type=response.headers.get("content-type", ""),
                                    http_status=response.status_code, request_meta={"params": params or {}})
    return identifier, response


async def prefetch_alpaca(store, ticker, *, client=None, tick_source="https://docs.alpaca.markets/us/docs/orders-at-alpaca"):
    """Read-only metadata/daily bars. Missing access and pagination remain explicit."""
    if not SYMBOL.fullmatch(ticker):
        raise ValueError("invalid_symbol")
    key, secret = os.environ.get("ALPACA_API_KEY"), os.environ.get("ALPACA_API_SECRET")
    if not key or not secret:
        raise ValueError("missing_alpaca_bindings")
    headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}
    owned = client is None
    client = client or httpx.AsyncClient(timeout=15, follow_redirects=False)
    findings = {"ticker": ticker, "documents": [], "facts": [], "gaps": []}
    try:
        precision_url = tick_source.rstrip("/")+".md"
        tick_doc, tick_response = await fetch_source(store, client, precision_url, kind="broker_limit_precision_documentation")
        findings["documents"].append(tick_doc)
        tick_text = store.document(tick_doc)["text"]
        tick_match = re.search(r"Limit price\s*(?:>=|≥)\s*\$1\.00\s*:\s*Max Decimals\s*=\s*2", tick_text, re.I)
        tick_verified_source = precision_url+"#sha256="+store.document(tick_doc)["raw_sha256"] if tick_response.status_code == 200 and tick_match else None
        asset_doc, response = await fetch_source(store, client, "https://paper-api.alpaca.markets/v2/assets/"+ticker, kind="broker_asset", headers=headers)
        findings["documents"].append(asset_doc)
        if response.status_code == 200:
            asset = json.loads(response.text, parse_float=str)
            company = company_from_asset(asset, tick_source=tick_verified_source)
            evidence = [{"document_id": asset_doc, "type": "json_pointer", "pointer": "/"+k, "value": asset[k]}
                        for k in ("symbol", "name", "class", "exchange", "status", "tradable")]
            documents = [asset_doc]
            if tick_verified_source:
                documents.append(tick_doc)
                evidence.append({"document_id": tick_doc, "type": "text", "passage": tick_match.group()})
            else:
                findings["gaps"].append("broker_limit_precision_not_verified_from_current_documentation")
            findings["facts"].append(store.add_fact(ticker, "company", company, document_ids=documents, evidence=evidence))
            if not company["classification_verified"]:
                findings["gaps"].append("common_stock_type_not_explicit_in_broker_name")
        else:
            findings["gaps"].append("broker_asset_http_"+str(response.status_code))
        now = store.clock()
        calendar = MarketCalendar(now.utc_ns)
        start = iso_utc(now.utc_ns-60*86400*SECOND)
        params = {"timeframe": "1Day", "start": start, "end": iso_utc(now.utc_ns-16*60*SECOND),
                  "feed": "sip", "adjustment": "raw", "limit": 1000, "sort": "asc"}
        bars_doc, response = await fetch_source(store, client, "https://data.alpaca.markets/v2/stocks/"+ticker+"/bars", kind="historical_daily_bars", params=params, headers=headers)
        findings["documents"].append(bars_doc)
        if response.status_code == 200:
            body = json.loads(response.text, parse_float=str)
            if not isinstance(body.get("bars"), list) or "next_page_token" not in body or body["next_page_token"] is not None:
                findings["gaps"].append("daily_bars_schema_or_pagination_incomplete")
            else:
                liquidity = daily_liquidity(body["bars"], calendar, now.utc_ns)
                findings["facts"].append(store.add_fact(ticker, "liquidity", liquidity, document_ids=[bars_doc], evidence=[
                    {"document_id": bars_doc, "type": "derived", "method_version": liquidity["measurement_method"], "input_sha256": liquidity["input_sha256"]}]))
                findings["liquidity_policy"] = LIQUIDITY_POLICY
        else:
            findings["gaps"].append("historical_daily_bars_http_"+str(response.status_code))
    finally:
        if owned:
            await client.aclose()
    return findings


async def prefetch_sec_identity(store, ticker, *, cik=None, client=None):
    """Supplement an existing broker asset snapshot with explicit SEC cover evidence.

    This does not turn a reporting-framework declaration into a verified Street-EPS
    mapping. Any framework metadata is labelled as the filed historical statement's.
    """
    if not SYMBOL.fullmatch(ticker):
        raise ValueError("invalid_symbol")
    owned = client is None
    client = client or httpx.AsyncClient(timeout=15, follow_redirects=False)
    headers = {"User-Agent": "AnnouncementResearch/0.1 paper-only research"}
    findings = {"ticker": ticker, "documents": [], "facts": [], "gaps": []}
    try:
        if cik is None:
            master_id, response = await fetch_source(store, client, "https://www.sec.gov/files/company_tickers.json", kind="SEC_ticker_master", headers=headers)
            findings["documents"].append(master_id)
            if response.status_code != 200:
                findings["gaps"].append("SEC_ticker_master_http_"+str(response.status_code));return findings
            rows = [row for row in response.json().values() if row.get("ticker") == ticker]
            if len(rows) != 1:
                findings["gaps"].append("SEC_ticker_identity_not_unique");return findings
            cik = str(rows[0]["cik_str"])
        if not str(cik).isdigit():
            raise ValueError("invalid_SEC_cik")
        cik = str(int(cik))
        submissions_id, response = await fetch_source(store, client, "https://data.sec.gov/submissions/CIK"+cik.zfill(10)+".json", kind="SEC_company_submissions", headers=headers)
        findings["documents"].append(submissions_id)
        if response.status_code != 200:
            findings["gaps"].append("SEC_company_submissions_http_"+str(response.status_code));return findings
        master = response.json()
        if ticker not in master.get("tickers", []):
            findings["gaps"].append("SEC_submissions_ticker_does_not_match");return findings
        recent = master["filings"]["recent"]
        eligible = [(i, form) for i,form in enumerate(recent["form"]) if form in ("10-Q", "10-K")]
        if not eligible:
            findings["gaps"].append("no_recent_domestic_10Q_or_10K");return findings
        index = eligible[0][0]
        accession, filename = recent["accessionNumber"][index], recent["primaryDocument"][index]
        if not re.fullmatch(r"\d{10}-\d{2}-\d{6}", accession) or not re.fullmatch(r"[A-Za-z0-9_.-]+", filename):
            raise ValueError("invalid_SEC_archive_reference")
        filing_url = "https://www.sec.gov/Archives/edgar/data/"+cik+"/"+accession.replace("-", "")+"/"+filename
        filing_id, response = await fetch_source(store, client, filing_url, kind="SEC_company_cover", headers=headers)
        findings["documents"].append(filing_id)
        if response.status_code != 200:
            findings["gaps"].append("SEC_company_cover_http_"+str(response.status_code));return findings
        records = store._records(ticker, store.clock().utc_ns+1)
        previous = next((r for r in records if r["kind"] == "company"), None)
        if previous is None:
            findings["gaps"].append("broker_asset_snapshot_required_before_SEC_supplement");return findings
        asset_docs = [store.document(i) for i in previous["source_documents"]]
        asset_docs = [d for d in asset_docs if d["source_kind"] == "broker_asset"]
        if not asset_docs:
            findings["gaps"].append("broker_asset_document_missing");return findings
        asset_doc = max(asset_docs,key=lambda d:d["received_utc_ns"])
        asset = json.loads(asset_doc["raw"])
        company = company_from_sec_cover(asset, response.content, tick_source=previous.get("tick_source"))
        company.update(sec_cik=cik, canonical_company_id="SEC_CIK:"+cik, fiscal_year_end_month_day=master.get("fiscalYearEnd"),
                       fiscal_calendar_source_document=submissions_id, latest_filing_report_date=recent["reportDate"][index])
        evidence = [{"document_id":asset_doc["id"],"type":"json_pointer","pointer":"/symbol","value":ticker},
                    {"document_id":submissions_id,"type":"json_pointer","pointer":"/tickers","value":master["tickers"]},
                    {"document_id":submissions_id,"type":"json_pointer","pointer":"/fiscalYearEnd","value":master["fiscalYearEnd"]}]
        evidence += [{"document_id":filing_id,"type":"inline_xbrl",**f} for f in company.get("classification_proof",[])]
        evidence += [{"document_id":filing_id,"type":"inline_xbrl",**f} for f in company.get("issuer_name_proof",[])]
        def name_key(value):return re.sub(r"[^a-z0-9&]","",value.casefold())
        if company.get("issuer_identity_verified") and name_key(master.get("name",""))==name_key(company["legal_issuer_name"]):
            company["verified_issuer_aliases"]=list(dict.fromkeys(company["verified_issuer_aliases"]+[master["name"]]))
            evidence.append({"document_id":submissions_id,"type":"json_pointer","pointer":"/name","value":master["name"]})
        text = store.document(filing_id)["text"]
        framework = re.search(r"accounting principles generally accepted in the United States of America[^\n]{0,40}",text,re.I)
        if framework:
            company.update(historical_reporting_framework="US_GAAP",
                           framework_scope="Prior filed consolidated statements; no automatic mapping to forecast or issuer-adjusted EPS")
            evidence.append({"document_id":filing_id,"type":"text","passage":framework.group()})
        currency=sec_reporting_currency_proof(response.content,cik)
        if currency:
            company.update(historical_reporting_currency="USD",reporting_currency_observation=currency)
            evidence += [{"document_id":filing_id,"type":"raw_text","passage":proof["raw"]} for proof in currency["proof"]]
        doc_ids = list(dict.fromkeys(previous["source_documents"]+[submissions_id,filing_id]))
        if previous.get("tick_source") and "#sha256=" in previous["tick_source"]:
            with store.connection() as c:
                tick_row=c.execute("SELECT id FROM context_documents WHERE raw_sha256=? AND http_status=200 LIMIT 1",(previous["tick_source"].split("#sha256=")[-1],)).fetchone()
            if tick_row:
                tick_document=store.document(tick_row["id"])
                match=re.search(r"Limit price\s*(?:>=|≥)\s*\$1\.00\s*:\s*Max Decimals\s*=\s*2",tick_document["text"],re.I)
                if match:
                    doc_ids.append(tick_row["id"])
                    evidence.append({"document_id":tick_row["id"],"type":"text","passage":match.group()})
        findings["facts"].append(store.add_fact(ticker,"company",company,document_ids=doc_ids,evidence=evidence))
        if not company["classification_verified"]:
            findings["gaps"].append("SEC_security_title_symbol_exchange_binding_unverified")
    finally:
        if owned:
            await client.aclose()
    return findings
