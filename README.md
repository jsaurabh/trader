# Announcement research and paper trading

A Python application for researching whether earnings and guidance announcements leave an executable opportunity after receipt. It records RTPR announcements and market data, extracts source-backed facts, applies deterministic comparisons and risk limits, and optionally manages **paper-only** orders.

No profitable edge or complete prospective five-second performance has been established. Historical diagnostics, synthetic fixtures and observed market records are kept separate.

## Timing and safety

The original five-second deadline begins when the first relevant wire alert is received, before parsing. It includes queueing, full-text retrieval, extraction, source validation, numerical comparisons, current quotes, portfolio checks and durable audit storage. Submit as soon as ready; missing information or expiry means skip. Duplicates, corrections and process restarts cannot renew the clock.

Wire receipt uses transport `data_received()` callback entry before protocol parsing and message queueing. The first complete data frame retains its timestamp across fragments and delayed reads; cancellation and queue faults cannot restart the budget. Callback-to-read delay includes parsing, remaining fragments and library/application queues. Kernel/TLS/pre-callback time and earlier incomplete-frame bytes remain unmeasured. Quotes/status become available only after complete-message read, so an earlier fragment cannot create a future-price observation.

Quote-condition metadata refreshes outside the entry path every six hours, with prompt retries after missing/failed preparation. Expired or unsupported definitions remain ineligible.

Paper transport is pinned to `https://paper-api.alpaca.markets`; no live order transport is implemented. Entries require verified issuer identity, comparable point-in-time expectations, fresh eligible quotes, risk checks, a durable reservation and committed decision evidence. The transport rechecks deadline, announcement binding, market state and kill switch immediately before request headers. Ambiguous submissions are reconciled rather than blindly retried.

Research defaults are long common-stock shares, no leverage/options/shorting, $100,000 simulated starting cash, at most $1,000 per position and three simultaneous companies, one entry per announcement, a $500 daily loss pause and a 5% drawdown review. These are development settings, not proven profitable rules. Existing positions retain their scheduled exits through entry pauses.

## Setup

Python 3.12+:

```bash
python -m pip install -e .
python -m unittest discover -s tests -v
```

Provide credentials using secure environment bindings. Do not commit `.env` files or put credentials in command arguments:

- `RTPR_API_KEY`
- `ALPACA_API_KEY`
- `ALPACA_API_SECRET`
- `OPENAI_API_KEY` when analysis is enabled

Obtain appropriate data-provider access and permissions for your own deployment. These commands do not buy subscriptions, create accounts, fund brokerage accounts or activate data plans. Data and model usage can have provider costs.

## Record and analyze

Recorder-only capture:

```bash
python -m recorder serve --symbols AAPL,MSFT --feed iex --db data/observed.sqlite --report reports/observed.json --html reports/observed.html
```

Add `--analyze --extractor candidates` for timed source extraction. Prepared context can be supplied using `--context-db data/context.sqlite`. Prepare source-backed context with `python -m recorder.context_cli --help`; essential unknowns remain skips.

IEX is a partial single-venue diagnostic feed. `--feed sip` must be selected explicitly with working entitlements; there is no delayed or IEX fallback. A working feed subscription alone does not establish suitable extended-hours liquidity or current regulatory trading state. Pre-subscribe intended symbols before announcements and respect the account's actual limits.

A bounded reference sampler is available with `--reference-symbols AAPL,MSFT` and `--context-db`. It samples at most three prepared symbols, no faster than every ten seconds, outside the entry path. Only supported fresh SIP quotes create source-backed midpoint references. A reference price is neither an executable entry price nor entry authority.

## Optional paper execution

Add `--paper --paper-state data/paper-ledger.sqlite` to enable gated paper execution. This implies analysis. It verifies and binds an empty $100,000 paper account, requires restrictive long-only paper configuration, subscribes to paper trade updates and reconciles durable owned-order state. Never mix synthetic and observed state.

```bash
python -m recorder paper-status --state data/paper-ledger.sqlite
python -m recorder paper-kill --on
python -m recorder paper-kill --off
```

The kill file pauses new entries and causes the running coordinator to cancel owned entry remainders. Existing exits continue according to their rules. Clearing the file does not clear other risk/reconciliation pauses. Cancel-request acceptance does not establish cancellation; partial fills and confirmed terminal states are tracked separately.

Limit orders, explicit expiry, restart recovery, duplicate protection and scheduled exits are implemented. The default scheduled exit is the applicable next regular-session open plus thirty minutes. Exchange calendars handle holidays, DST and shortened sessions; unsupported broker windows remain ineligible.

## Synthetic and historical research

A complete network-free fabricated paper lifecycle uses a new journal and ledger:

```bash
python -m recorder.paper_demo --db data/synthetic.sqlite --state data/synthetic-ledger.sqlite --report reports/synthetic.json --html reports/synthetic.html
```

Its companies, prices, expectations, broker and fills are fabricated. Add `--api` for one actual OpenAI call on fabricated input; no broker HTTP requests are made. Scenarios include delayed market data, queued admission/dispatch, missing source/context and rejected facts. A synthetic timing result is not a live latency or trading-performance result.

Additional tools:

```bash
python -m recorder history --help
python -m recorder case-study --help
python -m recorder.candidate_benchmark --help
python -m recorder.research_report --help
```

Historical quotes preserve actual later retrieval times and never become live observations. Replay requires declared quote/size/condition/status capabilities, models partial/unfilled orders and costs, and separates broker paper fills from conservative modeled fills. Chronological development, validation and untouched evaluation remain separate. Conditional ask-to-bid marks are not strategy returns.

## Modules

| Area | Implementation |
| --- | --- |
| Ingestion, documents and journal | `collectors.py`, `documents.py`, `journal.py` |
| Original announcement identity and clock | `announcement_clock.py`, `issuer.py` |
| Point-in-time company context and reference prices | `context.py`, `context_cli.py`, `reference.py` |
| Candidate extraction and deterministic validation | `candidates.py`, `analysis.py` |
| Quotes, sessions and risk checks | `market_inputs.py`, `timebase.py`, `decision.py` |
| Paper lifecycle and durable portfolio | `paper_broker.py`, `execution.py`, `portfolio.py` |
| Conservative replay and reporting | `execution_model.py`, `replay.py`, `report.py`, `research_report.py` |

Announcement text is untrusted input. The model selects source-backed facts and has no order tool. Numerical software validates periods, units, accounting/currency scope, contradictions and expectation availability. Unknown or unmatched accounting definitions prevent comparison.

## Private artifacts

`data/`, `reports/`, SQLite files and local environment files are ignored. Raw captures may contain licensed source content, signed URLs and private account data. Do not publish them. Reports distinguish configuration, dispatch, acknowledgment, fills, cancellations and observed prices; they do not infer returns from an unfilled intent.

The recorder depends on its execution environment remaining available; it is not guaranteed hosted infrastructure. Clock representation does not establish clock accuracy, and reconnects do not prove complete replay coverage. Extensive historical and prospective paper validation is required before considering real money.
