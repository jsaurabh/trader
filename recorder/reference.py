"""Sampled SIP midpoint references prepared outside the five-second entry path.

Only an explicit watchlist of at most three prepared issuers is sampled, at most
once per ten seconds each (18 REST quote requests/minute maximum). Existing
MarketInputs rate limits remain authoritative. This is a sampled historical price
reference, not the latest possible prepublication quote or a current entry price.
ContextStore selects only facts actually available before a later publication.

A regular open quote can support a reference while Trading Action is unknown,
provided there is no known halt/restriction. It NEVER grants entry authority: the
separate current market/status guard remains mandatory. No orders or subscriptions.
"""
import asyncio
from collections import Counter
from copy import deepcopy
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math

from .context import SYMBOL
from .market_inputs import MarketInputs
from .timebase import SECOND, iso_utc, parse_rfc3339

VERSION = "sampled_prepublication_sip_midpoint_v1"
ENDPOINT = "https://data.alpaca.markets/v2/stocks/{ticker}/quotes/latest"
SESSIONS = {"pre_market", "regular", "after_hours"}
RAW_KEYS = {"t", "bp", "ap", "bs", "as", "bx", "ax", "c", "z", "S", "T"}
METADATA = ("adapter_version", "feed", "quote_scope", "observation_method", "http_status", "source_utc",
            "source_precision_ns", "available_utc", "available_mono_ns", "request_started_utc",
            "download_completed_utc", "request_duration_ns", "coverage_verified", "conditions_eligible",
            "size_unit_verified", "size_unit", "size_provenance", "condition_provenance", "calendar_session",
            "broker_session_supported", "broker_session_source", "market_status", "halt_state",
            "open_quote_asof_timestamp", "status_provenance")


def _number(value):
    if isinstance(value,bool) or not isinstance(value,(str,int)):
        raise ValueError("exact_quote_number_required")
    number=Decimal(str(value))
    if not number.is_finite():raise ValueError("finite_quote_number_required")
    return number


class ContextReferenceRecorder:
    def __init__(self,store,market_inputs,journal=None,*,symbols,interval_s=10):
        if (not isinstance(symbols,(list,tuple)) or not 1<=len(symbols)<=3
                or any(not isinstance(s,str) or not SYMBOL.fullmatch(s) for s in symbols) or len(set(symbols))!=len(symbols)):
            raise ValueError("one_to_three_explicit_unique_reference_symbols_required")
        if isinstance(interval_s,bool) or not isinstance(interval_s,(int,float)) or not math.isfinite(interval_s) or interval_s<10:
            raise ValueError("reference_interval_must_be_at_least_ten_seconds")
        declared_mode=getattr(market_inputs,"mode",None)
        if store.mode=="OBSERVED":
            if type(market_inputs) is not MarketInputs or declared_mode not in (None,"OBSERVED"):
                raise ValueError("observed_reference_requires_observed_MarketInputs_adapter")
        elif store.mode!="SYNTHETIC" or declared_mode!="SYNTHETIC":
            raise ValueError("reference_provider_and_store_mode_must_match")
        self.store,self.provider,self.journal=store,market_inputs,journal
        self.mode,self.symbols,self.interval_s=store.mode,tuple(symbols),float(interval_s)
        self.clock,self.calendar=store.clock,market_inputs.calendar
        self.counters,self.gap_counts=Counter(),Counter()
        self._semaphore=asyncio.Semaphore(3)
        self._locks={s:asyncio.Lock() for s in symbols}
        self._next_attempt={}

    def _emit(self,result):
        if self.journal:self.journal.emit("context_reference",result,self.clock())
        return result

    def _gap(self,ticker,reasons,**extra):
        self.counters["gaps"]+=1
        self.gap_counts.update(reasons)
        return self._emit({"reference_version":VERSION,"mode":self.mode,"ticker":ticker,"status":"gap",
                           "reason_codes":list(dict.fromkeys(reasons)),"entry_authority":False,**extra})

    def _prepared(self,ticker,now):
        company=next((r for r in self.store._records(ticker,now.utc_ns+1) if r["kind"]=="company"),None)
        return bool(company and company.get("ticker")==ticker and company.get("classification_verified") is True
                    and company.get("canonical_company_id") and company.get("tradable") is True
                    and (not company.get("expires_at") or parse_rfc3339(company["expires_at"])[0]>now.utc_ns))

    def _validate(self,ticker,quote,now):
        reasons=[]
        if not isinstance(quote,dict):return ["reference_quote_missing"]
        if quote.get("mode",self.mode)!=self.mode:reasons.append("reference_quote_mode_mismatch")
        if quote.get("ticker")!=ticker:reasons.append("reference_quote_symbol_mismatch")
        if quote.get("feed")!="sip" or quote.get("quote_scope")!="consolidated_SIP":reasons.append("reference_current_sip_required")
        if quote.get("observation_method")!="rest_latest_snapshot" or quote.get("http_status")!=200:reasons.append("reference_successful_rest_snapshot_required")
        for key in ("coverage_verified","conditions_eligible","size_unit_verified"):
            if quote.get(key) is not True:reasons.append("reference_"+key+"_required")
        session=self.calendar.session(now.utc_ns)
        if session not in SESSIONS or quote.get("calendar_session")!=session or quote.get("broker_session_supported") is not True:
            reasons.append("reference_calendar_coverage_not_verified")
        state,halt=quote.get("market_status"),quote.get("halt_state")
        if state not in ("tradable","unknown","open_quote_asof_timestamp") or halt not in ("unknown","tradable","not_halted_as_last_trading_action"):
            reasons.append("reference_known_halt_or_restriction")
        if state!="tradable" and quote.get("open_quote_asof_timestamp") is not True:
            reasons.append("reference_open_quote_timestamp_required_when_trading_action_unknown")
        raw=quote.get("raw_quote")
        if not isinstance(raw,dict) or set(raw)-RAW_KEYS:return reasons+["reference_raw_quote_schema_not_verified"]
        try:
            source_ns,_=parse_rfc3339(quote["source_utc"])
            available_ns,_=parse_rfc3339(quote["available_utc"])
            if (type(quote["available_mono_ns"]) is not int or not 0<=now.utc_ns-source_ns<=SECOND
                    or not 0<=now.utc_ns-available_ns<=SECOND or not 0<=now.mono_ns-quote["available_mono_ns"]<=SECOND):
                reasons.append("reference_quote_not_fresh")
            if parse_rfc3339(raw["t"])[0]!=source_ns or self.calendar.session(source_ns) not in SESSIONS:
                reasons.append("reference_source_timestamp_or_session_not_verified")
            bid,ask=_number(raw["bp"]),_number(raw["ap"])
            if bid<=0 or ask<bid or _number(quote["bid"])!=bid or _number(quote["ask"])!=ask:
                reasons.append("reference_invalid_or_conflicting_bid_ask")
            if raw.get("S",ticker)!=ticker or raw.get("z") not in ("A","B","C"):
                reasons.append("reference_raw_quote_identity_not_verified")
            if (any(not isinstance(raw.get(k),str) or len(raw[k])!=1 or not raw[k].strip() for k in ("bx","ax"))
                    or not isinstance(raw.get("c"),list) or len(raw["c"]) not in (1,2) or any(c!="R" for c in raw["c"])):
                reasons.append("reference_raw_open_quote_conditions_not_verified")
            for raw_key,key in (("bs","bid_size_shares"),("as","ask_size_shares")):
                if type(raw.get(raw_key)) is not int or raw[raw_key]<=0 or _number(quote[key])!=raw[raw_key]:
                    reasons.append("reference_displayed_share_size_not_verified")
        except (KeyError,TypeError,ValueError,InvalidOperation,OverflowError):
            reasons.append("reference_missing_or_invalid_quote_fields")
        return list(dict.fromkeys(reasons))

    def _persist(self,ticker,quote):
        now=self.clock()
        reasons=self._validate(ticker,quote,now)
        if reasons:return {"reasons":reasons}
        raw=deepcopy(quote["raw_quote"])
        body={"mode":self.mode,"symbol":ticker,"quote":raw,"requested_feed":"sip","requested_currency":"USD",
              "provider_metadata":{key:deepcopy(quote[key]) for key in METADATA if key in quote},
              "reference_validation":{"utc":iso_utc(now.utc_ns),"mono_ns":now.mono_ns,
                  "source_age_ns":now.utc_ns-parse_rfc3339(quote["source_utc"])[0],
                  "provider_local_age_ns":now.mono_ns-quote["available_mono_ns"],"maximum_age_ns":SECOND}}
        encoded=json.dumps(body,sort_keys=True,separators=(",",":"),allow_nan=False).encode()
        if len(encoded)>65536:return {"reasons":["reference_snapshot_size_limit"]}
        source_hash=hashlib.sha256(encoded).hexdigest()
        document_id=self.store.add_document(ENDPOINT.format(ticker=ticker.replace("/","%2F")),encoded,
            source_kind="preannouncement_reference_quote",content_type="application/json",
            request_meta={"params":{"feed":"sip","currency":"USD"}})
        midpoint=(_number(raw["bp"])+_number(raw["ap"]))/2
        payload={"price":str(midpoint),"currency":"USD","observed_at":quote["source_utc"],"reliable":True,"reference_method":VERSION,
                 "quote_available_utc":quote["available_utc"],"quote_available_mono_ns":quote["available_mono_ns"],
                 "reference_validation_utc":iso_utc(now.utc_ns),"source_document_sha256":source_hash,
                 "reference_sampling_interval_s":self.interval_s,"entry_authority":False,
                 "unknown_trading_action_accepted_for_reference_only":quote.get("market_status")!="tradable",
                 "reference_scope":"Sampled source-time SIP midpoint; not latest possible prepublication quote or current executable entry price"}
        evidence=[{"document_id":document_id,"type":"json_pointer","pointer":"/quote/"+k,"value":raw[k]} for k in ("bp","ap","t")]
        evidence += [{"document_id":document_id,"type":"json_pointer","pointer":"/provider_metadata/"+k,"value":True} for k in ("coverage_verified","conditions_eligible","size_unit_verified")]
        evidence.append({"document_id":document_id,"type":"derived","method_version":VERSION,"input_sha256":source_hash,
                         "recipe":"Exact Decimal(raw quote bid + raw quote ask) / 2; no imported reference value"})
        fact_id=self.store.add_fact(ticker,"reference",payload,document_ids=[document_id],evidence=evidence)
        return {"document_id":document_id,"fact_id":fact_id,"source_sha256":source_hash,"price":str(midpoint),"observed_at":quote["source_utc"]}

    async def record_once(self,ticker):
        if not isinstance(ticker,str) or ticker not in self._locks:return self._gap(None,["reference_symbol_not_in_explicit_watchlist"])
        async with self._locks[ticker],self._semaphore:
            now=self.clock()
            if self.calendar.session(now.utc_ns) not in SESSIONS:
                self.counters["skipped_closed"]+=1
                return self._gap(ticker,["reference_calendar_session_not_covered"])
            if getattr(self.provider,"feed",None)!="sip":return self._gap(ticker,["reference_current_sip_required"])
            if now.mono_ns<self._next_attempt.get(ticker,0):return self._gap(ticker,["reference_sampling_interval_not_elapsed"])
            try:
                prepared=await asyncio.to_thread(self._prepared,ticker,now)
            except Exception as exc:
                return self._gap(ticker,["reference_context_read_failed"],error_class=type(exc).__name__)
            if not prepared:return self._gap(ticker,["reference_prepared_company_identity_missing"])
            now=self.clock()
            if self.calendar.session(now.utc_ns) not in SESSIONS:return self._gap(ticker,["reference_calendar_session_not_covered"])
            self._next_attempt[ticker]=now.mono_ns+int(self.interval_s*SECOND)
            self.counters["provider_requests"]+=1
            try:
                async with asyncio.timeout(3):quote=await self.provider(ticker)
            except asyncio.CancelledError:raise
            except Exception as exc:
                return self._gap(ticker,["reference_provider_failed"],error_class=type(exc).__name__)
            finally:
                # At least ten seconds from completion to the next request, even
                # if record_once is invoked independently of the background loop.
                self._next_attempt[ticker]=self.clock().mono_ns+int(self.interval_s*SECOND)
            try:
                result=await asyncio.to_thread(self._persist,ticker,quote)
            except asyncio.CancelledError:raise
            except Exception as exc:
                return self._gap(ticker,["reference_persistence_failed"],error_class=type(exc).__name__)
            if result.get("reasons"):return self._gap(ticker,result["reasons"])
            self.counters["recorded"]+=1
            return self._emit({"reference_version":VERSION,"mode":self.mode,"ticker":ticker,"status":"recorded",
                               "entry_authority":False,**result})

    async def run(self):
        """Await as a background task until cancellation; never join an entry deadline."""
        try:
            while True:
                self.counters["rounds"]+=1
                await asyncio.gather(*(self.record_once(ticker) for ticker in self.symbols))
                await asyncio.sleep(self.interval_s)
        finally:
            self._emit({"reference_version":VERSION,"mode":self.mode,"status":"stopped","entry_authority":False,
                        "counters":dict(self.counters),"gap_counts":dict(self.gap_counts)})
