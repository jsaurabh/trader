"""Reconstruct observations without using a quote received after a target time."""
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
import html
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile

from .journal import quote_window, read_rows, record_counts
from .history import historical_reports
from .market_inputs import SIZE_CHANGE_SOURCE, SIZE_SOURCE, verified_shares
from .timebase import DEADLINE_NS, SECOND, Stamp, iso_utc, local_time, parse_rfc3339

PAPER_KINDS = ("paper_order_dispatch", "paper_order_dispatch_aborted", "paper_order_ack", "paper_order_snapshot",
               "paper_order_request_failed", "paper_trade_update", "paper_cancel_requested", "paper_cancel_gap",
               "paper_cancel_waiting_for_identity", "paper_exit_decision", "paper_exit_overdue", "paper_entry_outcome",
               "paper_portfolio", "paper_stream", "paper_reconciliation_gap", "paper_identity_quarantined",
               "paper_update_quarantined", "paper_read")
PRIVATE_KEYS = {"account", "account_id", "account_number", "account_hash", "credentials", "authorization", "headers",
                "api_key", "apikey", "api_secret", "password", "secret", "access_token", "refresh_token", "session_token",
                "raw_output", "implementation_source", "extractor_implementation_source", "decision_implementation_source"}
NETWORK_URL = re.compile(r"\b(?:https?|wss?)://[^\s<>\"']+", re.I)
PUBLIC_PROVENANCE_URLS = {SIZE_SOURCE,SIZE_CHANGE_SOURCE}


def _private_key(key):
    lower = key.casefold()
    compact = re.sub(r'[^a-z0-9]','',lower)
    return (lower in PRIVATE_KEYS or lower.endswith(('_url','_uri','_secret','_api_key'))
        or compact in {'accountid','accountnumber','accounthash','accesstoken','refreshtoken','sessiontoken'}
        or re.search(r'api[-_]?key|api[-_]?secret|credential|authorization|password|secret',lower) is not None)


def export_safe(value):
    """Export only summaries; private raw bodies remain reconstructible in the journal."""
    if isinstance(value, dict):
        return {key: export_safe(item) for key,item in value.items()
                if not _private_key(key)}
    if isinstance(value, (list,tuple)):
        return [export_safe(item) for item in value]
    if isinstance(value,str):
        return re.sub(r"\bsk-[A-Za-z0-9_-]{12,}", "[secret omitted]", NETWORK_URL.sub(
            lambda match:match[0] if match[0] in PUBLIC_PROVENANCE_URLS else '[URL omitted from export]',value))
    return value


def _observed_paper(row,runs):
    data = row['data']
    return (runs.get(row['run_id'],{}).get('mode')=='OBSERVED' and data.get('mode','OBSERVED')=='OBSERVED'
            and not data.get('synthetic_fixture') and not row['kind'].startswith('synthetic_'))


def _order_snapshot(value):
    if not isinstance(value,dict) or not isinstance(value.get('client_order_id'),str):
        return None
    safe = {key:value[key] for key in ('client_order_id','symbol','side','qty','type','limit_price','time_in_force','extended_hours',
            'status','filled_qty','filled_avg_price','created_at','submitted_at','updated_at','filled_at','canceled_at','expired_at','failed_at') if key in value}
    if value.get('id') is not None:
        safe['broker_order_reference'] = 'paper-order-'+hashlib.sha256(str(value['id']).encode()).hexdigest()[:16]
    return safe


def _positive(value):
    try:
        number = Decimal(str(value))
        return number.is_finite() and number>0
    except InvalidOperation:
        return False


def _decision_summary(row,receipt=None):
    data = row['data']
    safe = {key:data[key] for key in ('event_id','ticker','mode','decision','reasons','checks','paper','orders_enabled',
        'strategy_version','decision_ready_utc','elapsed_from_receipt_ns','completed_within_deadline','client_order_id',
        'pre_dispatch_abort','supersedes_candidate_client_id','broker_acknowledged') if key in data}
    safe.update(run_id=row['run_id'],utc=iso_utc(row['stamp'].utc_ns))
    if isinstance(data.get('announcement_clock'),dict):
        safe['announcement_clock'] = _clock_summary(data['announcement_clock'])
    if receipt is not None:
        safe['latency_from_first_receipt_ns'] = data.get('elapsed_from_receipt_ns',row['stamp'].mono_ns-receipt.mono_ns)
    trace = data.get('decision_trace') or {}
    quote = (trace.get('inputs') or {}).get('quote')
    if isinstance(quote,dict):
        safe['execution_quote_at_decision'] = {key:quote[key] for key in ('ticker','feed','bid','ask','bid_size_shares',
            'ask_size_shares','source_utc','available_utc','available_mono_ns','coverage_verified','conditions_eligible',
            'size_unit_verified','size_unit','broker_session_supported','market_status','market_status_available_utc',
            'reason_codes','observation_method','source_age_ns','local_age_ns') if key in quote}
        safe['execution_quote_at_decision']['meaning'] = 'Quote checked for intended entry; never evidence of a fill'
    intent = trace.get('paper_intent') or data.get('paper_intent')
    if isinstance(intent,dict):
        safe['entry_limit_intent'] = {key:intent[key] for key in ('symbol','side','qty','type','limit_price','time_in_force',
            'extended_hours','client_order_id','ready_utc','expires_utc','scheduled_exit_utc','notional_cap') if key in intent}
    return safe


def _analysis_summary(data):
    # Raw model output, input catalogues and prepared source documents stay private.
    result = {key:data[key] for key in ('event_id','ticker','mode','receipt_mono_ns','deadline_mono_ns','started_utc','queue_delay_ns',
        'document_ready_after_receipt_ns','source_sha256','model_duration_ns','model','model_requested','prompt_version',
        'extractor_mode','comparison_version','strategy_version','prompt_sha256','schema_sha256','model_schema_sha256','context_sha256',
        'candidate_version','candidate_prompt_version','candidate_input_sha256','candidate_model_schema_sha256','candidate_catalogue_sha256',
        'facts','facts_pass_source_validation','fact_eligibility_gaps','model_reported_missing_information','checks','reasons',
        'decision','status','completed_utc','elapsed_from_receipt_ns','completed_within_deadline','issuer_identity') if key in data}
    if isinstance(data.get('announcement_clock'),dict):
        result['announcement_clock'] = _clock_summary(data['announcement_clock'])
    return result


def _clock_summary(value):
    if not isinstance(value,dict):
        return None
    result = {key:value[key] for key in ('registry_version','event_id','ticker','relevant','first_event_id','first_ticker',
        'first_key','clock_key','first_run_id','publication_ns','first_arrival_order','comparable_in_current_run',
        'block_reason','block_reasons','blocking_event_ids','identity_aliases','release_window_policy',
        'first_receipt_utc_ns','first_receipt_mono_ns','binding_signature') if key in value}
    if isinstance(result.get('first_receipt_utc_ns'),int):
        result.update(first_receipt_utc=iso_utc(result['first_receipt_utc_ns']),
            original_five_second_deadline_utc=iso_utc(result['first_receipt_utc_ns']+DEADLINE_NS))
    return result


def paper_summary(path,runs,decision_rows):
    """Correlate paper records while excluding all synthetic/unknown-mode transport."""
    orders, activities, portfolios = {}, [], []
    excluded = 0
    decisions = [row for row in decision_rows if _observed_paper(row,runs)]

    def owned(client_id):
        return orders.setdefault(client_id,{'client_order_id':client_id,'mode':'OBSERVED','paper':True,
            'event_ids':[],'dispatches':[],'aborts':[],'acknowledgments':[],'snapshots':[],'fill_updates':[],
            'cancel_requests':[],'exit_overdue':[],'request_failures':[]})

    def add_snapshot(value,row,source):
        snapshot = _order_snapshot(value)
        if snapshot:
            owned(snapshot['client_order_id'])['snapshots'].append({'utc':iso_utc(row['stamp'].utc_ns),
                'run_id':row['run_id'],'observed_mono_ns':row['stamp'].mono_ns,'source':source,'sequence':row['seq'],'order':snapshot})

    for row in decisions:
        data = row['data']
        for client in (data.get('client_order_id'),data.get('supersedes_candidate_client_id'),
                ((data.get('decision_trace') or {}).get('paper_intent') or {}).get('client_order_id')):
            if isinstance(client,str) and data.get('event_id') not in owned(client)['event_ids']:
                owned(client)['event_ids'].append(data.get('event_id'))
    for row in read_rows(path,kinds=PAPER_KINDS):
        if not _observed_paper(row,runs):
            excluded += 1
            continue
        data,kind = row['data'],row['kind']
        base = {'kind':kind,'utc':iso_utc(row['stamp'].utc_ns),'run_id':row['run_id'],'observed_mono_ns':row['stamp'].mono_ns}
        client = data.get('client_order_id')
        if kind=='paper_read':
            # Never export raw account/configuration/position/clock responses.
            if data.get('path')=='/v2/orders:by_client_order_id':
                add_snapshot(data.get('body'),row,'client_id_reconciliation')
            elif data.get('path')=='/v2/orders' and isinstance(data.get('body'),list):
                for value in data['body']:
                    add_snapshot(value,row,'open_order_reconciliation')
            continue
        if kind=='paper_portfolio':
            portfolio = data.get('portfolio') or {}
            item = {**base,'portfolio':{key:portfolio[key] for key in ('paper','reconciled','available_utc','currency',
                'cash','equity','peak_equity','daily_pnl','kill_switch','review_required','daily_pause','positions',
                'entry_reservations','reconciliation_reasons') if key in portfolio},
                'flags':{key:(data.get('flags') or {})[key] for key in ('reconciled','review_required','daily_pause','kill_switch')
                    if key in (data.get('flags') or {})}}
            portfolios.append(item)
            continue
        item = {**base,**{key:data[key] for key in ('event_id','client_order_id','parent_client_id','reason','error_class',
            'http_status','dispatch_observed','dispatch_boundary','elapsed_from_receipt_ns','state','scheduled_exit_utc',
            'decision_utc','lateness_ns') if key in data}}
        if kind=='paper_order_dispatch' and isinstance(client,str):
            item['payload'] = _order_snapshot(data.get('payload'))
            owned(client)['dispatches'].append(item)
        elif kind=='paper_order_dispatch_aborted' and isinstance(client,str):
            owned(client)['aborts'].append(item)
        elif kind=='paper_order_ack' and isinstance(client,str):
            owned(client)['acknowledgments'].append(item)
        elif kind=='paper_order_request_failed' and isinstance(client,str):
            owned(client)['request_failures'].append(item)
        elif kind=='paper_order_snapshot':
            add_snapshot(data.get('order'),row,data.get('source','order_snapshot'))
        elif kind=='paper_trade_update':
            raw = data.get('raw') or {}
            update = raw.get('data',raw)
            snapshot = _order_snapshot(update.get('order')) if isinstance(update,dict) else None
            if snapshot:
                client = snapshot['client_order_id']
                add_snapshot(update['order'],row,'trade_updates')
                item.update(client_order_id=client,event=update.get('event'))
                if update.get('event') in ('fill','partial_fill'):
                    fill = {**base,'client_order_id':client,'event':update['event'],
                        **{key:update[key] for key in ('qty','price','timestamp') if key in update},'order':snapshot}
                    identity = update.get('execution_id') or json.dumps({key:update.get(key) for key in ('event','qty','price','timestamp')}
                        | {'client_order_id':client,'filled_qty':snapshot.get('filled_qty'),'filled_avg_price':snapshot.get('filled_avg_price'),
                           'updated_at':snapshot.get('updated_at')},sort_keys=True)
                    fill['execution_reference'] = 'paper-execution-'+hashlib.sha256(str(identity).encode()).hexdigest()[:16]
                    if not any(v['execution_reference']==fill['execution_reference'] for v in owned(client)['fill_updates']):
                        owned(client)['fill_updates'].append(fill)
        elif kind=='paper_cancel_requested':
            if data.get('order_id') is not None:
                item['broker_order_reference'] = 'paper-order-'+hashlib.sha256(str(data['order_id']).encode()).hexdigest()[:16]
        elif kind=='paper_exit_decision' and isinstance(client,str):
            owned(client)['parent_client_id'] = data.get('parent_client_id')
            owned(client)['exit_decision'] = item
        elif kind=='paper_exit_overdue':
            parent = data.get('parent_client_id')
            if isinstance(parent,str):
                owned(parent)['exit_overdue'].append(item)
        activities.append(item)
    # The coordinator's abort callback supersedes its earlier candidate callback,
    # including old recordings missing the transport's explicit abort record.
    for row in decisions:
        data = row['data']
        if data.get('pre_dispatch_abort'):
            client = data.get('supersedes_candidate_client_id') or data.get('client_order_id')
            if isinstance(client,str):
                owned(client)['aborts'].append({'kind':'decision_pre_dispatch_abort','utc':iso_utc(row['stamp'].utc_ns),
                    'reason_codes':data.get('reasons',[])})
    for order in orders.values():
        if order.get('parent_client_id') in orders:
            order['event_ids'] = list(orders[order['parent_client_id']]['event_ids'])
        def latest_key(snapshot):
            try:
                updated = parse_rfc3339(snapshot['order'].get('updated_at',''))[0]
            except (TypeError,ValueError):
                updated = 0
            return updated,snapshot['sequence']
        latest = max(order['snapshots'],key=latest_key) if order['snapshots'] else None
        order['latest_snapshot'] = latest
        order['dispatch_observed'] = bool(order['dispatches']) and not order['aborts']
        order['submission_state'] = ('aborted_before_dispatch' if order['aborts'] else 'dispatch_observed' if order['dispatches']
            else 'acknowledgment_without_confirmed_dispatch' if order['acknowledgments'] else 'no_dispatch_evidence')
        order['fill_evidence_observed'] = any(_positive(v['order'].get('filled_qty')) for v in order['snapshots'])
        order['latest_reported_filled_qty'] = latest['order'].get('filled_qty') if latest else None
        order['cancellation_confirmed'] = bool(latest and latest['order'].get('status')=='canceled')
        order['expiration_confirmed'] = bool(latest and latest['order'].get('status')=='expired')
        references = {v['order']['broker_order_reference'] for v in order['snapshots'] if v['order'].get('broker_order_reference')}
        order['cancel_requests'] = [item for item in activities if item['kind']=='paper_cancel_requested'
            and item.get('broker_order_reference') in references]
        first_dispatch = order['dispatches'][0] if order['dispatch_observed'] else None
        fills = [value for value in order['snapshots'] if _positive(value['order'].get('filled_qty'))]
        first_fill = min(fills,key=lambda value:value['sequence']) if fills else None
        first_ack = order['acknowledgments'][0] if order['acknowledgments'] else None
        def observation_latency(observation):
            return (observation['observed_mono_ns']-first_dispatch['observed_mono_ns']
                if first_dispatch and observation and observation['run_id']==first_dispatch['run_id']
                and observation['observed_mono_ns']>=first_dispatch['observed_mono_ns'] else None)
        order['dispatch_to_ack_observation_ns'] = observation_latency(first_ack)
        order['dispatch_to_first_fill_observation_ns'] = observation_latency(first_fill)
        order['fill_observation_latency_meaning'] = 'Application observation after dispatch; not exchange fill time. Cross-run monotonic differences are not compared.'
        order['returns_after_costs'] = None
        order['performance_status'] = 'not_computed; paper fills and conservative execution model must be evaluated separately'
    return {'mode_filter':'OBSERVED only; synthetic and unknown-mode records excluded',
        'unique_dispatched_orders':sum(o['dispatch_observed'] for o in orders.values()),
        'aborted_order_attempts':sum(bool(o['aborts']) for o in orders.values()),
        'acknowledgment_count':sum(len(o['acknowledgments']) for o in orders.values()),
        'accepted_order_acknowledgments':sum(a.get('http_status') in (200,201) for o in orders.values() for a in o['acknowledgments']),
        'orders_with_fill_evidence':sum(o['fill_evidence_observed'] for o in orders.values()),
        'unique_fill_updates':sum(len(o['fill_updates']) for o in orders.values()),
        'cancel_request_count':sum(item['kind']=='paper_cancel_requested' for item in activities),
        'exit_overdue_count':sum(item['kind']=='paper_exit_overdue' for item in activities),
        'excluded_non_observed_paper_records':excluded,'orders':list(orders.values()),'activities':activities,
        'portfolio_observation_count':len(portfolios),'latest_portfolio_by_run':list({p['run_id']:p for p in portfolios}.values()),
        'limitations':['A before-headers dispatch record is transport evidence, not an acknowledgment or a fill.',
            'Explicit aborts supersede candidate decisions and remove the order from dispatched counts.',
            'Cancellation requests do not establish cancellation; confirmed order status is reported separately.',
            'Fills are broker paper observations, not conservative executable-fill assumptions or return estimates.']}


def quote_at(quotes, target: Stamp, feed, subscribed, max_age_ns=SECOND):
    available = [r for r in quotes if r["stamp"].mono_ns <= target.mono_ns]
    if not available:
        return {"status": "missing", "reason": "no_quote_received_by_target" if subscribed else "not_subscribed_before_alert",
                "feed": feed, "eligible_for_execution_research": False}
    quote = max(available, key=lambda r: (r["stamp"].mono_ns, r["seq"]))
    q, received = quote["data"]["raw"], quote["stamp"]
    local_age = target.mono_ns - received.mono_ns
    result = {"feed": quote["data"]["feed"], "source_timestamp_raw": q.get("t"),
              "observation_method": quote["data"].get("observation_method", "websocket_stream"),
              "received_utc": iso_utc(received.utc_ns), "local_age_ns": local_age,
              "bid": q.get("bp"), "ask": q.get("ap"),
              "bid_size_vendor_units": q.get("bs"), "ask_size_vendor_units": q.get("as"),
              "size_unit": "unknown", "size_unit_verified": False,
              "bid_size_shares": None, "ask_size_shares": None,
              "size_note": "Original vendor fields retained. Unsupported methods/dates/tapes remain unknown; no assumed x100 conversion",
              "conditions": q.get("c", []), "bid_exchange": q.get("bx"), "ask_exchange": q.get("ax"),
              "calendar_session_at_quote_receipt": quote["data"].get("calendar_session_at_receipt"),
              "status": "observed", "eligible_for_execution_research": False,
              "coverage": "single_exchange_IEX_not_NBBO" if feed == "iex" else "SIP_entitlement_and_session_coverage_must_be_verified"}
    result.update(verified_shares(q,result['feed'],observation_method=result['observation_method']))
    issues = []
    try:
        source, precision = parse_rfc3339(q.get("t", ""))
        source_age = target.utc_ns - source
        result.update(source_precision_ns=precision, source_age_ns=source_age)
        if source_age < 0:
            issues.append("source_timestamp_in_future_or_clock_offset")
        elif source_age > max_age_ns:
            issues.append("stale_source_timestamp")
    except (TypeError, ValueError):
        issues.append("missing_or_invalid_source_timestamp")
    if local_age > max_age_ns:
        issues.append("stale_local_receipt")
    try:
        bid, ask = Decimal(str(q["bp"])), Decimal(str(q["ap"]))
        bs, asks = Decimal(str(q["bs"])), Decimal(str(q["as"]))
        if not all(x.is_finite() for x in (bid, ask, bs, asks)) or bid <= 0 or ask <= 0 or ask < bid or min(bs, asks) <= 0:
            issues.append("invalid_crossed_or_empty_quote")
        else:
            result["spread_pct_of_mid"] = str((ask-bid) / ((ask+bid)/2) * 100)
    except (InvalidOperation, KeyError, TypeError):
        issues.append("invalid_quote_fields")
    if not subscribed and quote["data"].get("observation_method") != "rest_latest_snapshot":
        issues.append("subscription_not_confirmed_by_target")
    if q.get("c"):
        issues.append("quote_conditions_require_explicit_eligibility_rules")
    if feed == "iex":
        issues.append("single_exchange_coverage_insufficient_for_executable_market_claim")
    issues.append("halt_and_execution_venue_status_unverified")
    result["issues"] = issues
    if any(s.startswith("stale_") for s in issues):
        result["status"] = "stale"
    return result


def canonical_alert(alert):
    return json.dumps({k: v for k, v in alert.items() if k not in (
        "article_url", "deadline_mono_ns", "research_only_repeat", "first_receipt_run_id",
        "first_receipt_utc_ns", "first_deadline_utc_ns")}, sort_keys=True)


def build_report(path):
    # Raw market frames/trades and unrelated quotes remain in the journal. Report
    # generation must not materialize the entire continuously growing tick log.
    rows = list(read_rows(path, kinds=("run_start", "run_end", "alert", "document", "transport", "gap", "observation_window",
                                      "historical_request", "historical_page", "historical_result", "analysis", "decision", "probe",
                                      "quote_snapshot_result", "source_classification", "announcement_clock_binding")))
    counts = record_counts(path)
    runs = {}
    alerts = defaultdict(list)
    documents = defaultdict(list)
    quotes = defaultdict(list)
    controls = defaultdict(list)
    windows = set()
    window_lateness = {}
    for row in rows:
        data, kind, run = row["data"], row["kind"], row["run_id"]
        if kind == "run_start":
            runs[run] = {"run_id": run, **data, "start_utc": iso_utc(row["stamp"].utc_ns),
                         "start_los_angeles": local_time(row["stamp"].utc_ns)}
        elif kind == "run_end":
            runs.setdefault(run, {"run_id": run})["end_utc"] = iso_utc(row["stamp"].utc_ns)
        elif kind == "alert":
            alerts[(data["event_id"], runs.get(run, {}).get("mode", "UNKNOWN"))].append(row)
        elif kind == "document":
            documents[(data["event_id"], runs.get(run, {}).get("mode", "UNKNOWN"))].append(row)
        elif kind == "quote":
            quotes[(run, data["raw"].get("S"))].append(row)
        elif kind in ("transport", "gap"):
            controls[run].append(row)
        elif kind == "observation_window":
            windows.add((run, data["event_id"], data["receipt_mono_ns"]))
            window_lateness[(run, data["event_id"], data["receipt_mono_ns"])] = data.get("scheduler_lateness_ns")
    paper = paper_summary(path,runs,[r for r in rows if r['kind']=='decision'])
    reference_latest, reference_gaps, reference_counts = {}, Counter(), Counter()
    for row in read_rows(path, kinds=("context_reference",)):
        data = row["data"]
        if runs.get(row["run_id"], {}).get("mode") != "OBSERVED" or data.get("mode") != "OBSERVED":
            continue
        reference_counts[data.get("status", "unknown")] += 1
        reference_gaps.update(data.get("reason_codes", []))
        summary = {key: data[key] for key in ("ticker", "status", "reason_codes", "error_class", "reference_version",
            "source_sha256", "price", "observed_at", "document_id", "fact_id") if key in data}
        summary.update(run_id=row["run_id"], available_utc=iso_utc(row["stamp"].utc_ns), entry_authority=False)
        reference_latest[(row["run_id"], data.get("ticker"))] = summary
    for metadata in runs.values():
        metadata['live_orders_enabled'] = False
        metadata['orders_enabled'] = False
        metadata['paper_enabled_requested'] = any(metadata.get(key) is True for key in (
            'paper_enabled_requested','paper_enabled','paper_execution_enabled','paper_orders_enabled','allow_paper_orders'))
    events = []
    for (event_id, mode), receipts in alerts.items():
        # Journal sequence, not a later publication field or correction, fixes the first receipt.
        first = min(receipts, key=lambda r: r["seq"])
        a, received, run = first["data"], first["stamp"], first["run_id"]
        metadata = runs.get(run, {})
        target = Stamp(received.utc_ns + DEADLINE_NS, received.mono_ns + DEADLINE_NS)
        snapshot_times = [r["stamp"].mono_ns for r in rows if r["run_id"] == run and r["kind"] == "quote_snapshot_result"
                          and r["data"].get("event_id") == event_id]
        quotes[(run, a["ticker"])] = quote_window(path, run, a["ticker"], received.mono_ns, max([target.mono_ns, *snapshot_times]))
        def subscription_at(target_stamp):
            states = [r for r in controls[run] if r["kind"] == "transport"
                      and r["data"].get("stream") == "alpaca" and r["stamp"].mono_ns <= target_stamp.mono_ns]
            return bool(states and states[-1]["data"].get("state") == "ready"
                        and a["ticker"] in states[-1]["data"].get("quotes", []))
        event_docs = documents[(event_id, mode)]
        hashes = list(dict.fromkeys(d["data"]["content_sha256"] for d in event_docs if "content_sha256" in d["data"]))
        verified = [d for d in event_docs if d["data"].get("full_text_verified")]
        full = min(verified, key=lambda r: r["seq"]) if verified else None
        doc_summaries = []
        for doc in event_docs:
            d = doc["data"]
            doc_summaries.append({k: d[k] for k in ("status", "http_status", "content_sha256", "byte_count", "title", "wire_source",
                                                   "source_tickers", "adapter_version", "provider_body_complete", "completeness_scope",
                                                   "body_table_count", "preformatted_block_count", "adapter_issues",
                                                   "contains_html_tables", "full_text_verified", "error_class",
                                                   "finished_within_five_seconds", "download_complete_utc_ns") if k in d}
                                 | {"observed_utc": iso_utc(doc["stamp"].utc_ns), "run_id": doc["run_id"]})
        canonical = Counter(canonical_alert(r["data"]) for r in receipts)
        relevant_controls = [r for r in controls[run] if r["stamp"].mono_ns <= target.mono_ns
                             and (r["stamp"].mono_ns >= received.mono_ns - 60 * SECOND
                                  or r["data"].get("state") == "disconnected")]
        pub = a.get("published_ns")
        event_snapshots = [r for r in quotes[(run, a["ticker"])] if r["data"].get("event_id") == event_id]
        snapshot_summaries = []
        for snapshot in event_snapshots:
            offset = snapshot["data"].get("target_offset_ns", 0)
            snapshot_summaries.append({"target_offset_ns": offset,
                                       "available_after_receipt_ns": snapshot["stamp"].mono_ns-received.mono_ns,
                                       "available_after_target_ns": snapshot["stamp"].mono_ns-received.mono_ns-offset,
                                       **quote_at([snapshot], snapshot["stamp"], metadata.get("feed"), True)})
        event_analyses = [_analysis_summary(r["data"]) for r in rows if r["kind"] == "analysis" and r["run_id"] == run and r["data"]["event_id"] == event_id]
        decision_rows = [r for r in rows if r["kind"] == "decision" and r["run_id"] == run and r["data"]["event_id"] == event_id]
        effective_decision = decision_rows[-1] if decision_rows else None
        decisions = [_decision_summary(r,received) for r in decision_rows]
        execution_quotes = [d['execution_quote_at_decision'] for d in decisions if 'execution_quote_at_decision' in d]
        paper_orders = [order for order in paper['orders'] if mode=='OBSERVED' and event_id in order['event_ids']]
        classifications = [{'utc':iso_utc(row['stamp'].utc_ns),'run_id':row['run_id'],
            **{key:row['data'][key] for key in ('event_id','relevant','reason','current_period_end','issuer_identity','catalogue_source_sha256') if key in row['data']},
            'announcement_clock':_clock_summary(row['data'].get('announcement_clock'))}
            for row in rows if row['kind']=='source_classification' and row['run_id']==run and row['data'].get('event_id')==event_id]
        clocks = [row['data'].get('announcement_clock') for row in decision_rows if isinstance(row['data'].get('announcement_clock'),dict)]
        if not clocks:
            clocks = [row['data'] for row in rows if row['kind']=='announcement_clock_binding' and row['run_id']==run
                and row['data'].get('event_id')==event_id]
        if not clocks:
            clocks = [c['announcement_clock'] for c in classifications if c['announcement_clock']]
        effective_client = (effective_decision['data'].get('client_order_id') or
            (((effective_decision['data'].get('decision_trace') or {}).get('paper_intent') or {}).get('client_order_id'))) if effective_decision else None
        entry_abort_orders = [order for order in paper_orders if effective_client and order['client_order_id']==effective_client]
        abort_superseded = bool(effective_decision and effective_decision['data'].get('decision')=='paper_entry_ready'
            and any(order['aborts'] for order in entry_abort_orders))
        effective_reasons = effective_decision['data'].get('reasons',[]) if effective_decision else []
        if abort_superseded:
            effective_reasons = list(dict.fromkeys([*effective_reasons,*[reason for order in entry_abort_orders for abort in order['aborts']
                for reason in abort.get('reason_codes',[abort.get('reason','paper_dispatch_aborted')])]]))
        events.append({
            "event_id": event_id, "mode": mode, "ticker": a["ticker"], "first_run_id": run,
            "source_wire": next((d["data"]["wire_source"] for d in event_docs if d["data"].get("wire_source")), a.get("source_wire")),
            "title": next((d["data"]["title"] for d in event_docs if d["data"].get("title")), None),
            "publication_raw": a.get("published_raw"),
            "publication_precision_ns": a.get("published_precision_ns"), "publication_meaning": a.get("publication_meaning"),
            "receipt_utc": iso_utc(received.utc_ns), "receipt_los_angeles": local_time(received.utc_ns),
            "receipt_boundary": metadata.get("receipt_boundary"),
            "ingress_version": a.get("ingress_version", metadata.get("ingress_version")),
            "unmeasured_before_receipt": metadata.get("unmeasured_before_receipt"),
            "full_alert_available_utc": iso_utc(a["full_message_available_utc_ns"]) if isinstance(a.get("full_message_available_utc_ns"), int) else None,
            "wire_ingress_to_application_read_ns": a.get("library_queue_delay_ns"),
            "wire_ingress_delay_meaning": a.get("library_queue_delay_meaning"),
            "publication_to_receipt_ns": received.utc_ns - pub if pub is not None else None,
            "publication_latency_caveat": "Vendor timestamp meaning and local clock synchronization unverified",
            "five_second_target_utc": iso_utc(target.utc_ns),
            "five_second_window_observed": (run, event_id, received.mono_ns) in windows,
            "five_second_scheduler_lateness_ns": window_lateness.get((run, event_id, received.mono_ns)),
            "full_text_available_utc": iso_utc(full["stamp"].utc_ns) if full else None,
            "receipt_to_full_text_ns": full["stamp"].mono_ns-received.mono_ns if full and full["run_id"] == run else None,
            "full_text_within_first_deadline": full["stamp"].mono_ns <= target.mono_ns if full and full["run_id"] == run else None,
            "full_text_state": "verified" if full else "unavailable_or_unverified",
            "documents": doc_summaries,
            "quote_snapshots": snapshot_summaries,
            "quote_at_receipt": quote_at(quotes[(run, a["ticker"])], received, metadata.get("feed"), subscription_at(received)),
            "quote_at_five_seconds": (quote_at(quotes[(run, a["ticker"])], target, metadata.get("feed"), subscription_at(target))
                                      if (run, event_id, received.mono_ns) in windows else
                                      {"status": "window_not_observed", "eligible_for_execution_research": False}),
            "repeat_alert_count": len(receipts)-1, "exact_duplicate_count": sum(n-1 for n in canonical.values()),
            "alert_kinds": sorted({r["data"]["alert_kind"] for r in receipts}),
            "observed_document_versions": len(hashes), "document_content_changes": max(0, len(hashes)-1),
            "explicit_correction_claims": [r["data"]["correction_claim"] for r in receipts if r["data"].get("correction_claim")],
            "revision_caveat": "Changed bytes need not be a financial correction; new-ID corrections require vendor linkage",
            "transport_and_gaps": [{"utc": iso_utc(r["stamp"].utc_ns), **r["data"]} for r in relevant_controls],
            "analysis": event_analyses,
            "source_classification": classifications,
            "canonical_announcement_clock": _clock_summary(clocks[-1]) if clocks else None,
            "quote_observation_clock": "Raw vendor event ID first receipt; canonical strategy deadline can be earlier",
            "decision": "skip" if abort_superseded else effective_decision["data"]["decision"] if effective_decision else "not_implemented_recorder_only",
            "decision_reasons": effective_reasons, "decision_superseded_by_transport_abort": abort_superseded,
            "decision_utc": iso_utc(effective_decision["stamp"].utc_ns) if effective_decision else None,
            "decision_latency_ns": effective_decision['data'].get('elapsed_from_receipt_ns',effective_decision["stamp"].mono_ns-received.mono_ns) if effective_decision else None,
            "decision_history": decisions,
            "execution_quote_at_decision": execution_quotes[-1] if execution_quotes else None,
            "execution_quote_note": "Checked entry quote is distinct from receipt/+5 research observations; no fill inferred",
            "live_orders_enabled": False, "paper_enabled_requested": metadata['paper_enabled_requested'],
            "orders_submitted": sum(order['dispatch_observed'] for order in paper_orders),
            "observed_paper_orders": paper_orders, "returns_after_costs": None,
            "performance_status": "not_computed_from_quote_snapshots_or_unfilled_intents",
        })
    report = {"report_schema": 2, "purpose": "Announcement/quote and paper lifecycle observations; no trading performance inferred",
            "orders_enabled": False, "runs": list(runs.values()), "events": events,
            "live_orders_enabled": False,
            "orders_enabled_scope": "Legacy live-order flag; paper configuration and observed dispatch are separate fields",
            "paper_enabled_requested": any(r['paper_enabled_requested'] for r in runs.values()),
            "paper_enabled": any(r['paper_enabled_requested'] and r.get('mode')=='OBSERVED' for r in runs.values()),
            "paper_enabled_meaning": "Run configuration request; not evidence of dispatch, acknowledgment or a fill",
            "orders_submitted": paper['unique_dispatched_orders'], "paper_orders_dispatched": paper['unique_dispatched_orders'],
            "paper_summary": paper,
            "reference_preparation": {"mode_filter": "OBSERVED only", "entry_authority": False,
                "status_counts": dict(reference_counts), "gap_reason_counts": dict(reference_gaps),
                "latest_by_run_symbol": list(reference_latest.values()),
                "meaning": "Sampled prepublication reference; not a current execution quote, fill or entry authorization"},
            "record_counts": counts,
            "analysis_records": [_analysis_summary(r["data"]) for r in rows if r["kind"] == "analysis"],
            "historical_quotes": historical_reports(rows),
            "probe_results": [r["data"] for r in rows if r["kind"] == "probe"],
            "document_observations_without_alert": [
                {"event_id": r["data"]["event_id"], "observed_utc": iso_utc(r["stamp"].utc_ns),
                 **{k: r["data"][k] for k in ("status", "http_status", "full_text_verified", "error_class") if k in r["data"]}}
                for r in rows if r["kind"] == "document" and not any(key[0] == r["data"]["event_id"] for key in alerts)],
            "transport_and_gaps": [{"run_id": r["run_id"], "utc": iso_utc(r["stamp"].utc_ns), **r["data"]}
                                   for r in rows if r["kind"] in ("transport", "gap")],
            "limitations": ["No alerts during a sample does not establish coverage, configured rules, or absence of releases",
                            "No full-text completeness is inferred from a headline or generic HTML",
                            "Quotes are latest locally received at or before the target; never future observations",
                            "Receipt/+5 quotes and order intents never establish fills or returns; paper fill evidence is separate",
                            "Paper-enabled configuration is distinct from observed dispatch, acknowledgment and fill",
                            "Synthetic and unknown-mode lifecycle records never enter observed paper counts",
                            "Raw account data, signed/private URLs, model inputs and credential fields remain in the private journal",
                            "Source publication times and local wall clock are not independently synchronized",
                            "In-flight journal records can be lost on process/host failure; incomplete runs remain visible"]}
    return export_safe(report)


def atomic_text(path, contents):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, prefix="."+path.name, delete=False) as output:
            temporary = Path(output.name)
            output.write(contents)
            output.flush()
            os.fsync(output.fileno())
        temporary.replace(path)
    finally:
        if temporary and temporary.exists():
            temporary.unlink()


def write_report(report, json_path, html_path=None):
    report = export_safe(report)
    json_path = Path(json_path)
    json_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_text(json_path, json.dumps(report, indent=2, allow_nan=False) + "\n")
    if html_path:
        path = Path(html_path)
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        # Escape every untrusted source value. Never embed source HTML or signed URLs.
        body = html.escape(json.dumps(report, indent=2, allow_nan=False))
        def esc(value):
            return html.escape(str(value) if value is not None else "Unavailable")
        synthetic = any(r.get("mode") == "SYNTHETIC" for r in report["runs"])
        development = any(r.get("mode") == "HISTORICAL_DEVELOPMENT" for r in report["runs"])
        historical = bool(report.get("historical_quotes"))
        mode = ("SYNTHETIC · fabricated observations" if synthetic else "HISTORICAL DEVELOPMENT · primary sources retrieved later" if development
                else "HISTORICAL · quotes retrieved after their source timestamps"
                if historical else "OBSERVED · bounded real connection sample")
        counts = report["record_counts"]
        historical_count = sum(h["result"].get("quote_count", 0) for h in report.get("historical_quotes", []))
        summary = '<div class="stats">' + ''.join(
            f'<div><strong>{value}</strong><span>{esc(label)}</span></div>' for label, value in [
                ("Events", len(report["events"])), ("Quote observations", counts.get("quote", 0)),
                ("Complete release bodies", sum(e["full_text_state"] == "verified" for e in report["events"])),
                ("Historical quotes", historical_count), ("Heartbeat replies", counts.get("heartbeat", 0)),
                ("Observed paper dispatches", report.get('paper_orders_dispatched',0))]) + '</div>'
        events = []
        for event in report["events"]:
            details = [("Publication (vendor)", event["publication_raw"]),
                       ("Publication meaning", event["publication_meaning"]),
                       ("Publication precision, ns", event["publication_precision_ns"]),
                       ("Receipt · Los Angeles", event["receipt_los_angeles"]),
                       ("Receipt · UTC", event["receipt_utc"]),
                       ("Receipt boundary", event.get("receipt_boundary")),
                       ("Full alert available · UTC", event.get("full_alert_available_utc")),
                       ("Ingress → application read, ms", str(Decimal(event["wire_ingress_to_application_read_ns"])/1_000_000)
                        if event.get("wire_ingress_to_application_read_ns") is not None else "not measured by this run"),
                       ("Ingress delay meaning", event.get("wire_ingress_delay_meaning")),
                       ("Publication → receipt, ms", str(Decimal(event["publication_to_receipt_ns"])/1_000_000)
                        if event["publication_to_receipt_ns"] is not None else None),
                       ("Five-second target · UTC", event["five_second_target_utc"]),
                       ("Five-second window observed", event["five_second_window_observed"]),
                       ("Full-text availability", event["full_text_state"]),
                       ("Full text available · UTC", event["full_text_available_utc"]),
                       ("Receipt → full text, ms", str(Decimal(event["receipt_to_full_text_ns"])/1_000_000) if event.get("receipt_to_full_text_ns") is not None else None),
                       ("Decision", event["decision"]), ("Decision reasons", "; ".join(event.get("decision_reasons", []))),
                       ("Paper submission requested", event.get('paper_enabled_requested',False)),
                       ("Observed paper dispatches", event.get('orders_submitted',0)),
                       ("Receipt → decision, ms", str(Decimal(event["decision_latency_ns"])/1_000_000) if event.get("decision_latency_ns") is not None else None),
                       ("Repeat alerts / exact duplicates", f'{event["repeat_alert_count"]} / {event["exact_duplicate_count"]}'),
                       ("Observed document versions", event["observed_document_versions"])]
            detail_html = '<dl>' + ''.join(f'<dt>{esc(k)}</dt><dd>{esc(v)}</dd>' for k,v in details) + '</dl>'
            snapshots = []
            for label, q in [("At receipt", event["quote_at_receipt"]), ("At +5 seconds", event["quote_at_five_seconds"])]:
                snapshots.append('<tr>' + ''.join(f'<td>{esc(value)}</td>' for value in [label, q.get("status"), q.get("bid"),
                                 q.get("ask"), q.get("spread_pct_of_mid"), q.get("source_age_ns"), q.get("feed")]) + '</tr>')
            for q in event.get("quote_snapshots", []):
                label = f'REST sample available {Decimal(q["available_after_receipt_ns"])/1_000_000:.1f} ms after receipt'
                snapshots.append('<tr>' + ''.join(f'<td>{esc(v)}</td>' for v in [label, q.get("status"), q.get("bid"), q.get("ask"),
                                 q.get("spread_pct_of_mid"), q.get("source_age_ns"), q.get("feed")]) + '</tr>')
            q = event.get('execution_quote_at_decision')
            if q:
                snapshots.append('<tr>'+''.join(f'<td>{esc(v)}</td>' for v in ['Checked for entry · intent, no fill inferred',
                    q.get('market_status'),q.get('bid'),q.get('ask'),q.get('spread_pct_of_mid'),q.get('source_age_ns'),q.get('feed')])+'</tr>')
            events.append(f'<section><h2>{esc(event["ticker"])} · {esc(event["mode"])}</h2><p>{esc(event.get("title"))}</p>{detail_html}'
                          '<div class="table"><table><thead><tr><th>Snapshot</th><th>Status</th><th>Bid</th><th>Ask</th>'
                          '<th>Spread %</th><th>Source age, ns</th><th>Feed</th></tr></thead><tbody>'
                          + ''.join(snapshots) + '</tbody></table></div></section>')
        for history in report.get("historical_quotes", []):
            request, result = history["request"], history["result"]
            for symbol, snapshots in history["snapshots"].items():
                detail_html = '<dl>' + ''.join(f'<dt>{esc(k)}</dt><dd>{esc(v)}</dd>' for k, v in [
                    ("Anchor · Los Angeles", request["anchor_los_angeles"]), ("Anchor · UTC", request["anchor_utc"]),
                    ("Anchor meaning", request["anchor_meaning"]), ("Actual retrieval began · UTC", request["retrieval_started_utc"]),
                    ("Download status", result["status"]), ("Pages complete", result["complete"]),
                    ("Quotes downloaded", snapshots["quote_count"]), ("Known to system at past anchor", False)]) + '</dl>'
                snapshot_rows = []
                for label, key in [("Source-time anchor", "at_anchor"), ("Source-time anchor +5 seconds", "at_anchor_plus_five_seconds")]:
                    q = snapshots[key]
                    snapshot_rows.append('<tr>' + ''.join(f'<td>{esc(v)}</td>' for v in [label, q.get("status"), q.get("bid"),
                                         q.get("ask"), q.get("spread_pct_of_mid"), q.get("source_timestamp_raw")]) + '</tr>')
                events.append(f'<section><h2>{esc(symbol)} · historical SIP</h2>'
                              '<p>Retrieved later for research. This is not a live quote or a measured wire-receipt benchmark.</p>'
                              + detail_html + '<div class="table"><table><thead><tr><th>Target</th><th>Status</th><th>Bid</th>'
                              '<th>Ask</th><th>Spread %</th><th>Quote source time</th></tr></thead><tbody>' + ''.join(snapshot_rows)
                              + '</tbody></table></div></section>')
        for analysis in report.get("analysis_records", []):
            details = [("Input mode", analysis["mode"]), ("Status", analysis["status"]),
                       ("Decision", analysis["decision"]), ("Reasons", "; ".join(analysis["reasons"])),
                       ("Receipt → completion, ms", str(Decimal(analysis["elapsed_from_receipt_ns"])/1_000_000)),
                       ("Completed within five seconds", analysis["completed_within_deadline"]),
                       ("Model", analysis.get("model", analysis.get("model_requested"))),
                       ("Prompt version", analysis["prompt_version"]), ("Facts extracted", len(analysis["facts"]))]
            details.extend([("Facts pass source checks", analysis.get("facts_pass_source_validation", "not recorded by this version")),
                            ("Fact eligibility gaps", "; ".join(analysis.get("fact_eligibility_gaps", [])))])
            detail_html = '<dl>' + ''.join(f'<dt>{esc(k)}</dt><dd>{esc(v)}</dd>' for k,v in details) + '</dl>'
            events.append(f'<section><h2>{esc(analysis["ticker"])} · timed extraction · {esc(analysis["mode"])}</h2>'
                          + detail_html + '<p>Extraction and earnings comparisons do not authorize an order.</p></section>')
        paper = report.get('paper_summary',{})
        reference = report.get('reference_preparation', {})
        if reference.get('latest_by_run_symbol'):
            events.append('<section><h2>Prepublication reference preparation</h2>'
                '<p>Sampled reference prices grant no entry authority. Closed sessions, IEX and unavailable or unverified data remain explicit gaps.</p>'
                '<pre>'+esc(json.dumps(reference, indent=2))+'</pre></section>')
        if paper.get('orders') or report.get('paper_enabled_requested'):
            order_rows = []
            for order in paper.get('orders',[]):
                latest = (order.get('latest_snapshot') or {}).get('order',{})
                payload = (order.get('dispatches') or [{}])[-1].get('payload') or latest
                order_rows.append('<tr>'+''.join(f'<td>{esc(value)}</td>' for value in [payload.get('symbol'),payload.get('side'),
                    order['submission_state'],latest.get('status','no order snapshot'),payload.get('qty'),
                    order.get('latest_reported_filled_qty'),latest.get('filled_avg_price'),order.get('cancellation_confirmed')])+'</tr>')
            events.append('<section><h2>Observed paper lifecycle</h2><p>Paper submission requested: '
                +esc(report.get('paper_enabled_requested',False))+'. Unique dispatches: '+esc(paper.get('unique_dispatched_orders',0))
                +'. Aborted attempts: '+esc(paper.get('aborted_order_attempts',0))+'. Live submission is disabled.</p>'
                '<p>Acknowledgments, partial fills and cancellation confirmation are separate observations. No returns are inferred.</p>'
                '<div class="table"><table><thead><tr><th>Company</th><th>Side</th><th>Submission evidence</th><th>Latest status</th>'
                '<th>Intended shares</th><th>Reported cumulative fills</th><th>Fill average</th><th>Cancellation confirmed</th></tr></thead>'
                '<tbody>'+''.join(order_rows)+'</tbody></table></div></section>')
        if not events:
            events.append('<section><h2>No announcement events observed</h2><p>This sample does not establish feed coverage, '
                          'configured rules, event latency, or extended-hours quote availability.</p></section>')
        transport = ''.join('<tr>' + ''.join(f'<td>{esc(value)}</td>' for value in [r.get("utc"), r.get("stream"),
                            r.get("state") or r.get("reason"), r.get("feed")]) + '</tr>' for r in report["transport_and_gaps"])
        evidence = ('<section><h2>Connections and gaps</h2><div class="table"><table><thead><tr><th>UTC</th><th>Stream</th>'
                    '<th>State / gap</th><th>Feed</th></tr></thead><tbody>' + transport + '</tbody></table></div></section>')
        atomic_text(path, '<!doctype html><meta charset="utf-8"><title>Announcement recorder report</title>'
                        '<style>body{font:16px system-ui;max-width:1100px;margin:40px auto;padding:0 20px;'
                        'background:#101923;color:#e7edf6}pre{white-space:pre-wrap;overflow-wrap:anywhere;'
                        'background:#192737;padding:24px;border-radius:12px}h1{font-size:28px}h2{font-size:21px}'
                        'p,dt,span{color:#b9c8d9}.mode{color:#9fe1cb}section{margin:26px 0;padding:24px;'
                        'background:#192737;border-radius:12px}.stats{display:flex;flex-wrap:wrap;gap:18px}'
                        '.stats div{background:#192737;padding:20px;flex:1;min-width:140px;border-radius:12px}'
                        'strong,span{display:block}strong{font-size:32px}dl{display:grid;grid-template-columns:1fr 2fr;gap:12px}'
                        'dd{margin:0;overflow-wrap:anywhere}.table{overflow:auto}table{border-collapse:collapse;width:100%;'
                        'font-size:14px}th,td{text-align:left;padding:12px 8px;border-bottom:1px solid #35465a;'
                        'white-space:nowrap}summary{cursor:pointer;padding:15px 0}@media(max-width:650px){dl{display:block}'
                        'dd{margin-bottom:14px}}</style>'
                        f'<p class="mode">{esc(mode)}</p><h1>Announcement recorder</h1>'
                        '<p>UTC timestamps with America/Los_Angeles display times. Live order submission is disabled. '
                        'Configured paper submission and observed lifecycle evidence are shown separately; no trading performance inferred.</p>'
                        + summary + ''.join(events) + evidence
                        + f'<details><summary>Complete report and limitations</summary><pre>{body}</pre></details>')
