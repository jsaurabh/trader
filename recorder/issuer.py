"""Conservative, deterministic issuer binding for the first earnings declaration."""
import hashlib
import re

VERSION = "first-earnings-declaration-issuer-v1"
EXCHANGES = {"NYSE":"NYSE", "NASDAQ":"NASDAQ", "NASDAQGS":"NASDAQ", "NASDAQGM":"NASDAQ",
             "NASDAQCM":"NASDAQ", "NYSEAMERICAN":"AMEX", "AMEX":"AMEX", "NYSEARCA":"ARCA", "BATS":"BATS"}
TICKER = re.compile(r"\b(NASDAQ(?:GS|GM|CM)?|NYSE(?:\s+AMERICAN|\s+ARCA)?|AMEX|BATS)\s*:\s*([A-Z][A-Z0-9./-]{0,14})\b", re.I)
VERB = re.compile(r"\b(?:announced|reported|released|posted|disclosed|issued|published|announces|reports|releases|posts|discloses|issues|publishes)\b", re.I)
RESULTS = re.compile(r"^\s+(?:(?:today|its)\s+){0,2}(?:(?:financial|quarterly|annual|earnings|operating)\s+results\b|results\s+for\b|(?:record\s+)?(?:revenue|net sales)\s+for\b)", re.I)
PERIOD = re.compile(r"\b(?:quarter|fiscal year|full year|year ended|year ending|three months|twelve months)\b", re.I)
LEGAL_AD = re.compile(r"\b(?:shareholder alert|investor alert|class action|securities fraud|law firm|lawsuit|investigation alert|investigates? potential|legal notice)\b", re.I)


def _tokens(value):
    return re.findall(r"[a-z0-9]+|&", value.casefold())


def _name_pattern(value):
    tokens=re.findall(r"[A-Za-z0-9]+|&",value)
    return re.compile(r"(?<!\w)"+r"[\s,.'’®™-]+".join(re.escape(t) for t in tokens)+r"(?!\w)",re.I) if tokens else None


def _subject(source, verb_start):
    # Body paragraphs and conventional dateline separators are source boundaries;
    # names and numeric facts remain unmodified and offsets remain original.
    start=source.rfind("\n",0,verb_start)+1
    prefix=source[start:verb_start]
    separators=list(re.finditer(r"\s(?:--|—|–)\s",prefix))
    if separators:
        start += separators[-1].end()
    else:
        boundaries=list(re.finditer(r"[!?]\s+|\.\s+(?=[A-Z][a-z])",prefix))
        if boundaries:
            start += boundaries[-1].end()
    while start<verb_start and source[start].isspace():start+=1
    end=verb_start
    temporal=re.search(r"\b(?:today|this morning|this afternoon)\s*$",source[start:end],re.I)
    if temporal:end=start+temporal.start()
    while end>start and source[end-1].isspace():end-=1
    return start,end


def verify_issuer(document, company):
    """Bind a prepared issuer to the FIRST actual financial-results declaration.

    `document` must contain verified full text, preferably `article_body_text`; a
    `text` field is also accepted for a verified source adapter. Source ticker lists,
    headlines and arbitrary company mentions never establish issuer identity.
    `company` is trusted prepared configuration, with source-backed verification
    flags/names generated before publication, not output authored by the model.
    """
    result={"verified":False,"evidence":[],"source_sha256":None,"method":VERSION,"reasons":[]}
    if not isinstance(document,dict) or not isinstance(company,dict):
        result["reasons"].append("missing_document_or_prepared_company");return result
    field="article_body_text" if isinstance(document.get("article_body_text"),str) else "text"
    source=document.get(field)
    result["text_field"]=field
    if document.get("full_text_verified") is not True or not isinstance(source,str) or not source.strip():
        result["reasons"].append("verified_full_release_text_required");return result
    result["source_sha256"]=hashlib.sha256(source.encode()).hexdigest()
    if document.get("content_sha256"):result["raw_source_sha256"]=document["content_sha256"]
    if company.get("classification_verified") is not True or not company.get("canonical_company_id"):
        result["reasons"].append("prepared_canonical_common_stock_identity_required");return result
    if LEGAL_AD.search(source[:1600]):
        result["reasons"].append("legal_ad_or_claim_opening_not_issuer_earnings_release");return result
    candidate=None
    for verb in VERB.finditer(source):
        tail=source[verb.end():verb.end()+320]
        if not RESULTS.match(tail) or not PERIOD.search(tail[:240]):continue
        prefix=source[max(0,verb.start()-80):verb.start()]
        if re.search(r"\b(?:will|would|plans? to|scheduled to|expects? to|expected to|going to)\s*$",prefix,re.I):continue
        candidate=verb;break
    if candidate is None:
        result["reasons"].append("no_supported_actual_financial_results_declaration");return result
    start,end=_subject(source,candidate.start())
    subject=source[start:end]
    declaration_end=source.find("\n",candidate.end())
    declaration_end=min(len(source),candidate.end()+320) if declaration_end<0 else declaration_end
    declaration_end=min(declaration_end,candidate.end()+320)
    result["evidence"].append({"role":"first_financial_results_declaration","start":start,"end":declaration_end,"passage":source[start:declaration_end]})
    if not subject or len(subject)>600 or subject.startswith(('"',"'","“","‘")) or re.search(r"\b(?:said|says|according to|following|after|before|when|that|will)\b|[;\n]",subject,re.I):
        result["reasons"].append("first_declaration_subject_not_direct_issuer");return result
    aliases=[]
    if company.get("issuer_identity_verified") is True:
        aliases=[name for name in [company.get("legal_issuer_name")]+company.get("verified_issuer_aliases",[]) if isinstance(name,str) and len(name.strip())>=3]
    # Legal names must start the direct subject; a competitor in the description
    # cannot certify the announcing company. A trailing source description is allowed.
    legal_match=None
    for alias in aliases:
        pattern=_name_pattern(alias)
        match=pattern.match(subject) if pattern else None
        if not match:continue
        remainder=subject[match.end():].lstrip(". ")
        if remainder.startswith("("):
            closing=remainder.find(")")
            if closing<0:continue
            remainder=remainder[closing+1:].lstrip()
        if remainder and not remainder.startswith(","):continue
        if re.search(r"^,\s*(?:and\b|&|with\b|in partnership with\b)",remainder,re.I):continue
        if remainder and re.search(r"\b(?:jointly|together with|alongside|co-announced|and [A-Z][A-Za-z]+ (?:Inc|Corp|Ltd))\b",remainder):continue
        legal_match=match;break
    exchanges=list(TICKER.finditer(subject))
    if len(exchanges)>1:
        result["reasons"].append("multiple_exchange_ticker_declarations");return result
    exchange_match=None
    if exchanges:
        match=exchanges[0]
        expected=(company.get("exchange"),company.get("ticker"))
        actual=(EXCHANGES.get(re.sub(r"\s+","",match[1]).upper()),match[2].upper())
        if actual!=expected:
            result["reasons"].append("first_declaration_exchange_ticker_conflicts");return result
        opening=subject.rfind("(",0,match.start())
        closing=subject.find(")",match.end())
        head=subject[:opening].strip(" ,.") if opening>=0 else ""
        head_tokens=_tokens(head)
        canonical_tokens=[_tokens(name) for name in aliases]
        joint=(re.search(r"\band\b",head,re.I) is not None or "&" in head and not any(head_tokens==tokens[:len(head_tokens)] for tokens in canonical_tokens))
        remainder=subject[closing+1:].lstrip() if closing>=0 else "INVALID"
        joint = joint or re.search(r"^,\s*(?:and\b|&|with\b|in partnership with\b)|\b(?:jointly|together with|alongside|co-announced)\b",remainder,re.I) is not None
        if (company.get("exchange_ticker_verified") is True and opening>0 and closing>=match.end()
                and 1<len(head)<180 and not joint and (not remainder or remainder.startswith(","))):
            exchange_match=match
    if legal_match is None and exchange_match is None:
        result["reasons"].append("first_declaration_not_bound_to_verified_issuer_name_or_exchange_ticker");return result
    if legal_match:
        result["evidence"].append({"role":"verified_issuer_name","start":start+legal_match.start(),"end":start+legal_match.end(),"passage":legal_match.group()})
    if exchange_match:
        result["evidence"].append({"role":"verified_exchange_ticker","start":start+exchange_match.start(),"end":start+exchange_match.end(),"passage":exchange_match.group()})
    result.update(verified=True,canonical_company_id=company["canonical_company_id"],ticker=company.get("ticker"))
    return result
