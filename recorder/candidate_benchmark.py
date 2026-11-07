"""Bounded development measurements of retrieval + candidate selection, not live trades."""
import argparse
import asyncio
import base64
import hashlib
import html
import json
import os
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from .analysis import DEFAULT_MODEL, ModelRequestBudget, validate_facts
from .candidates import CandidateExtractor, PROMPT_VERSION, VERSION
from .case_study import parse_exhibit
from .journal import Journal
from .report import atomic_text
from .timebase import DEADLINE_NS, Stamp, iso_utc


async def benchmark(manifests, db_path, json_path, html_path=None, *, attempts=1, model=DEFAULT_MODEL):
    if Path(db_path).exists() or not 1 <= attempts <= 3 or not 1 <= len(manifests) <= 4 or attempts*len(manifests) > 12:
        raise ValueError("New database, one to four cases, one to three attempts, at most twelve API attempts")
    definitions = [json.loads(Path(p).read_text()) for p in manifests]
    for case in definitions:
        url = urlsplit(case['primary_source_url'])
        if (url.scheme != 'https' or url.hostname not in ('www.sec.gov','investor.nvidia.com','www.microsoft.com','ir.amd.com')
                or url.username or url.password or url.fragment or url.query or case.get('source_reviewed') is not True):
            raise ValueError("Reviewed primary-source URLs only; no authenticated article URLs")
    journal = Journal(db_path, {'mode':'HISTORICAL_DEVELOPMENT','purpose':'candidate_extraction_component_benchmark',
        'orders_enabled':False,'candidate_version':VERSION,'prompt_version':PROMPT_VERSION,'model_requested':model})
    results = []
    try:
        budget = ModelRequestBudget(journal,limit=12)
        async with httpx.AsyncClient(timeout=5,follow_redirects=False) as client:
            extractor = CandidateExtractor(client,model,budget,journal)
            for case in definitions:
                journal.emit('candidate_development_case',case)
                for attempt in range(attempts):
                    started = Stamp.now()
                    row = {'case_id':case['case_id'],'ticker':case['ticker'],'attempt':attempt+1,'mode':'HISTORICAL_DEVELOPMENT',
                        'benchmark_started_utc':iso_utc(started.utc_ns),'decision':'skip','orders_enabled':False,
                        'facts':[],'source_validation_errors':[],'reasons':[]}
                    try:
                        async with asyncio.timeout(DEADLINE_NS/1e9):
                            response = await client.get(case['primary_source_url'],headers={'User-Agent':'AnnouncementResearch/0.1 paper-only development research'})
                            downloaded = Stamp.now()
                            raw = response.content
                            raw_hash = hashlib.sha256(raw).hexdigest()
                            row.update(http_status=response.status_code,html_sha256=raw_hash,download_elapsed_ns=downloaded.mono_ns-started.mono_ns)
                            if response.status_code != 200 or len(raw)>8*1024*1024 or raw_hash!=case['reviewed_html_sha256']:
                                row['reasons'].append('reviewed_primary_document_unavailable_or_version_changed')
                            else:
                                text,tables = await asyncio.to_thread(parse_exhibit,raw)
                                if not all(p in text for p in case['identity_passages']):
                                    row['reasons'].append('primary_source_identity_not_verified')
                                else:
                                    ready = Stamp.now()
                                    row.update(full_text_available_utc=iso_utc(ready.utc_ns),full_text_after_start_ns=ready.mono_ns-started.mono_ns,
                                        source_sha256=hashlib.sha256(text.encode()).hexdigest(),html_tables_preserved=tables)
                                    journal.emit('candidate_primary_document',{'case_id':case['case_id'],'text':text,'raw_b64':base64.b64encode(raw).decode(),
                                        'html_sha256':raw_hash,'actual_download_completed_utc':iso_utc(downloaded.utc_ns),'source_tables':tables},ready)
                                    result = await extractor.extract_document({'text':text,'raw_html':raw},case['ticker'])
                                    row.update({k:result[k] for k in ('raw_output','model','response_id','request_id','usage','candidate_catalogue','candidate_input_sha256') if k in result})
                                    row['offered_candidate_count'] = len(result['candidate_catalogue']['preferred_ids'])
                                    if result.get('decoding_error'):
                                        row['reasons'].append(result['decoding_error'])
                                    else:
                                        selection = json.loads(result['raw_output'])
                                        row.update(selected_candidate_count=len(selection['selected_ids']),
                                            rejected_candidate_count=len(selection['rejected_ids']),
                                            all_offered_candidates_selected=not selection['rejected_ids'])
                                        facts,errors = await asyncio.to_thread(extractor.validate_result,result,{'text':text,'raw_html':raw},case['ticker'])
                                        row.update(facts=facts,source_validation_errors=errors,model_missing_information=result['payload']['missing_information'])
                                        if errors:
                                            row['reasons'].append('source_validation_failed')
                    except (TimeoutError,httpx.TimeoutException):
                        row['reasons'].append('five_second_component_deadline_expired')
                    except Exception as exc:
                        row['reasons'].append(getattr(exc,'code','component_error'))
                        row['error_class'] = type(exc).__name__
                    finished = Stamp.now()
                    elapsed = finished.mono_ns-started.mono_ns
                    within = elapsed < DEADLINE_NS
                    if not within and 'five_second_component_deadline_expired' not in row['reasons']:
                        row['reasons'].append('five_second_component_deadline_expired')
                    row.update(completed_utc=iso_utc(finished.utc_ns),elapsed_ns=elapsed,completed_within_deadline=within,
                        source_checks_passed=bool(row['facts']) and not row['source_validation_errors'] and within and not row['reasons'])
                    row['eligibility_gaps'] = [f"fact_{i}:"+gap for i,f in enumerate(row['facts']) for gap,missing in (
                        ('currency_not_verified','UNSPECIFIED' in f['unit']),('accounting_basis_not_verified',f['basis']=='UNSPECIFIED')) if missing]
                    row['reasons'].append('development_component_only; point_in_time_expectations_quotes_portfolio_execution_not_evaluated')
                    journal.emit('candidate_component_result',row,finished)
                    results.append(row)
        journal.emit('run_end',{'reason':'candidate_component_benchmark_complete','attempts':len(results)})
    finally:
        journal.close()
    report = {'mode':'HISTORICAL_DEVELOPMENT','candidate_version':VERSION,'prompt_version':PROMPT_VERSION,'attempts':results,
        'timely_attempts':sum(r['completed_within_deadline'] for r in results),'source_checked_attempts':sum(r['source_checks_passed'] for r in results),
        'orders_submitted':0,'limitations':[
            'Selected historical development cases; no untouched evaluation or population accuracy estimate',
            'Current-day primary-source download + candidate construction + API selection + source validation only',
            'No historical or prospective wire receipt; no live quote, risk, reservation or execution in this component benchmark',
            'Repeated cases can use warm connections and cached model inputs; tiny samples do not establish latency percentiles',
            'Source checks validate selected facts only; even accepting every offered candidate does not establish complete source extraction',
            'Table cells are bound through actual DOM column spans; accounting basis and currency remain unknown unless explicit',
            'Historical model knowledge contamination is possible; every selected amount and period is fixed by source-backed software',
            'Missing/ambiguous facts and changes to non-GAAP definitions require matching pre-event context; no strategy qualification established']}
    atomic_text(json_path,json.dumps(report,indent=2)+'\n')
    if html_path:
        esc=lambda value:html.escape(str(value))
        rows=''.join('<tr>'+''.join('<td>'+esc(v)+'</td>' for v in (r['ticker'],r['attempt'],round(r['elapsed_ns']/1e6,3),
            r['completed_within_deadline'],r['source_checks_passed'],len(r['facts']),
            str(r.get('selected_candidate_count','?'))+'/'+str(r.get('offered_candidate_count','?')),
            '; '.join(r['eligibility_gaps'])))+'</tr>' for r in results)
        facts=''.join('<h3>'+esc(r['ticker'])+' / attempt '+str(r['attempt'])+'</h3><pre>'+esc(json.dumps(r['facts'],indent=2))+'</pre>' for r in results)
        atomic_text(html_path,'<!doctype html><meta charset="utf-8"><title>Candidate extraction development evidence</title>'
            '<style>body{font:16px system-ui;max-width:1100px;margin:40px auto;padding:20px;background:#101923;color:#e7edf6}'
            'td,th{padding:10px;text-align:left;border-bottom:1px solid #35465a}table{border-collapse:collapse;width:100%}'
            'pre{white-space:pre-wrap;overflow-wrap:anywhere}section{overflow:auto;margin:24px 0}</style>'
            '<p>HISTORICAL DEVELOPMENT · component measurement only · no orders</p><h1>Verified candidate selection</h1>'
            '<p>The model selects fixed source-backed IDs. It cannot change amounts, periods, units or basis.</p>'
            '<section><table><tr><th>Company</th><th>Attempt</th><th>Total ms</th><th>Within5s</th><th>Selected-fact source checks</th><th>Facts</th><th>Accepted/offered</th><th>Eligibility gaps</th></tr>'+rows+'</table></section>'
            '<section><h2>Selected evidence</h2>'+facts+'</section><h2>Limits</h2><ul>'+''.join('<li>'+esc(x)+'</li>' for x in report['limitations'])+
            '</ul><details><summary>Full component record</summary><pre>'+esc(json.dumps(report,indent=2))+'</pre></details>')
    return report


def main():
    os.umask(0o077)
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--manifest',action='append',required=True);p.add_argument('--db',required=True)
    p.add_argument('--report',required=True);p.add_argument('--html');p.add_argument('--attempts',type=int,default=1)
    p.add_argument('--model',default=DEFAULT_MODEL)
    args=p.parse_args()
    result=asyncio.run(benchmark(args.manifest,args.db,args.report,args.html,attempts=args.attempts,model=args.model))
    print(json.dumps({'attempts':len(result['attempts']),'timely':result['timely_attempts'],'source_checked':result['source_checked_attempts'],'orders_submitted':0}))


if __name__=='__main__':
    main()
