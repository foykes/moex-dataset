# Research export interface audit

Audit date: 2026-09-07. Owner: @foykes. [U01 Issue #2](https://github.com/foykes/moex-dataset/issues/2).

## Decision

**NO-GO for canonical mechanics daily import, strict daily research import, strict dividend import, point-in-time universe, and adjusted-price research.** All 15 owner-confirmed candidate files are readable and can be retained as a hash-pinned raw evidence snapshot. That archival capability does not make their contents eligible for canonical import or a research bundle.

Both daily CSV/XLSX pairs lack `RSI14`, stable identity, board and adjusted OHLC. Each daily dataset has exactly 500 rows for 248 of its 263 tickers. The 30-year pair additionally contains a conflicting daily key, 39 invalid OHLC rows and two multi-year intervals labeled as daily candles. Dividend exports contain incomplete semantics and conflicting evidence. Reference files describe different snapshots and do not establish historical identity or universe completeness.

This audit changes documentation only. It does not produce an adapter, enrich data, recalculate indicators, run models, or raise research readiness.

## Evidence boundary and method

- Audited the generated directory explicitly confirmed by the owner, including its old dividend/reference files. File modification time was not treated as generation time, market knowledge time or proof of freshness. No committed sample replaced a generated file.
- Upstream base: `5e4eb07fa24e9ecf1c77c2fefb0adfde453ff1c4` on `main`. The existing local checkout is at `b4389cd2443f140745ccd315b6caae854bfbab17` with uncommitted collector changes. [PR #1](https://github.com/foykes/moex-dataset/pull/1) remains open and adds only `AGENTS.md`; it is not the audit branch base.
- Consumer contract baseline: `foykes/moex-rsi-research` main `dfb56e71692d9c222abc87e51e5534873ead23cc`, including accepted C00. U01 ownership and exclusive report reservation were confirmed by the coordinator in [research draft PR #54](https://github.com/foykes/moex-rsi-research/pull/54), not merged into main at audit time. Other lanes remain queued.
- Files were read as bytes, SHA-256 hashed, parsed from those same bytes, and rehashed after analysis. All candidate hashes remained unchanged. No upstream Python module was imported or executed. Exact absolute paths, detailed per-ticker results and analysis tools are retained only in ignored local evidence.
- Parser: bundled Python 3.12, pandas 2.2.3, NumPy 2.3.5 and openpyxl 3.1.5. Each XLSX has one sheet. Types below are observed parser dtypes, not a declared producer schema. `object` is primarily source text; all-null columns can infer different dtypes across formats.
- Date parsing uses explicit ISO date/datetime and Russian `DD.MM.YYYY[ HH:MM:SS]` formats. Missing values and nonempty unparseable values are counted separately. Forecast annotations are retained in the source and parsed separately for diagnostics. Numeric source values are not repaired. Exact duplicate checks exclude a serialized dataframe index as well as checking all columns.
- Equity/DR classification uses the audited current full catalogue only as an observed cross-check. It is not an as-of join. No historical reuse, missing instrument or inactive coverage claim is inferred from a current mapping.
- Corpus: the two daily pairs, MOEX `all` pair, Dohod.ru `all_payments` pair, both stock/full reference pairs, `tickers_dates` pair and distinct legacy `ticker_dates.xlsx`. Intraday exports, Dohod overview/annual summary products, published mirrors and former Dry Run 001 files are outside this input audit.

## Daily findings

Counts apply independently to each file in a CSV/XLSX pair; formats are not added together.

| Check | 30-year daily | 10-year daily |
|---|---:|---:|
| Rows / tickers | 127,149 / 263 | 127,100 / 263 |
| Actual begin-date range | 2003-07-10 to 2026-06-30 | 2016-07-04 to 2026-06-30 |
| Tickers with exactly 500 rows | 248 | 248 |
| Tickers ending before 2020 | 196 | 178 |
| Tickers with any 2026 row | 24 | 24 |
| Missing values in the nine present columns | 0 | 0 |
| Missing `RSI14`, identity/board, adjusted OHLC columns | All absent | All absent |
| Duplicate `(ticker, begin-date)` groups / excess rows | 1 / 1 | 0 / 0 |
| Exact duplicate rows, including/excluding export index | 0 / 0 | 0 / 0 |
| Zero open / zero close | 39 / 39 | 0 / 0 |
| OHLC bound violations | 39 | 0 |
| Zero volume with positive value | 96 | 0 |
| Negative or nonfinite OHLC/value/volume | 0 | 0 |
| Zero value / malformed begin or end / end before begin | 0 / 0 / 0 | 0 / 0 / 0 |
| Begin and end on different calendar dates | 2 | 0 |
| Time ordering inside each ticker | Ascending | Ascending |
| Global ticker/date or global date ordering | Neither | Neither |
| Current-catalogue equity / DR tickers | 262 / 1 | 262 / 1 |
| Current-catalogue equity / DR rows | 126,649 / 500 | 126,600 / 500 |

The duplicate is `ABIO` on 2011-01-01. Both rows end on 2023-08-22 and have differing OHLC/value/volume. These are conflicting multi-year records, not identical copies that can safely be dropped. The 39 zero-open/close rows also violate `low <= open, close`; zero volume with positive value is a separate diagnostic, not a license to fill volume.

### Coverage and pagination

The global maximum date hides stale per-ticker histories:

| Ticker | 30-year begin-date coverage | 10-year begin-date coverage |
|---|---|---|
| SBER | 2007-07-20 to 2009-07-28 | 2016-07-04 to 2018-06-22 |
| GAZP | 2006-01-23 to 2008-01-25 | 2016-07-04 to 2018-06-22 |
| GMKN | 2006-12-26 to 2009-01-11 | 2016-07-04 to 2018-06-22 |

`moex_query` performs one successful candle request with no page offset. Both inspected `full_reload` implementations call it once per ticker for the full requested interval. A bounded [official SBER second-page check](https://iss.moex.com/iss/engines/stock/markets/shares/securities/SBER/candles.json?from=2007-07-20&till=2026-06-30&interval=24&start=500&iss.only=candles) returned 500 further rows, 2009-07-29 through 2011-08-02. Response size: 52,404 bytes; SHA-256: `1e00a9d99967750b8d5ba813321dca6759e06db793ad7c8dcc0267511d6a175f`. The check proves that the audited file omits available SBER history; it does not certify every ticker or the entire API history.

Thus neither filename proves complete 10-/30-year coverage. The 30-year aggregate spans approximately 23 years, while even the near-ten-year aggregate in the other file is assembled from mostly short, different ticker windows. The producer has no retained run manifest binding these bytes to a particular code revision. Missing pagination is demonstrated in both code versions and is consistent with, and material to, the observed truncated snapshot.

`data_update` strips technical columns before writing, considers only tickers whose latest stored record is within 30 days for normal updates, and adds newly seen tickers using a one-year window. That policy can leave truncated active histories stale. It sorts updated rows, whereas `full_reload` does not impose global ordering. The audited order alone cannot prove which execution path produced a file.

### RSI, prices and file publication

All four daily files contain only `open, close, high, low, value, volume, begin, end, ticker`. RSI warm-up behavior, internal RSI gaps and calculated RSI values are **unobservable** because the column is absent, not because there are zero warm-up nulls.

In both code versions, `tech.calculator` uses `talib.RSI(df_ticker["close"], timeperiod=14)`, groups only by ticker, preserves the input row order within each ticker and joins results back by original index. It does not sort or enforce stable identity before the calculation. Its TA-Lib build/version and the actual successful indicator run are not attested. The candle path copies provider OHLC without a collector-side adjustment; the provider's exact historical price basis is unverified. No adjusted-price or stable-identity research-RSI claim is justified.

`main.py` first runs notebook-to-script conversion, then collection, MOEX dividends, technical indicators, Dohod scraping and publishing. The five script/notebook pairs have equivalent function ASTs within each inspected version. Only `data_gathering` differs materially between local and main among these paired modules; this does not prove execution provenance.

Writes use fixed names and overwrite CSV/XLSX. `tech.calculator` overwrites its input; `upload` replaces FTP names and clears/replaces the Google sheet. The 10-year daily CSV is explicitly the Google-sheet input, so it is operationally relevant. No versioned snapshot manifest or successful-stage attestation was supplied. CSV and XLSX need separate hashes; publication is not demonstrated to be atomic across files. No production entrypoint or upload was run during the audit.

## Dividend findings

### MOEX source

The `all` pair contains 661 rows for 69 tickers, nominal amounts in RUB (638 rows) and USD (23), with no missing values in its five columns. There are 15 zero amounts, no negative/nonfinite amounts and no duplicates by either `(TRADE_CODE, dt)` or `(ISIN, dt, value, currency)`. The parsed date range is 2013-08-02 to **2111-01-01**; the latter is a `MOEX` row with amount 17.35 RUB and must not be silently corrected or accepted as realized history.

`dividends.div_loader` reads positions 2, 3 and 4 of `data['dividends']['data']` into `dt`, amount and currency, discarding the original column names and provider security/event identity. It queries securities by ISIN, iterates matching SECIDs, and writes the caller-supplied ticker/ISIN rather than retaining each contributing SECID. The code has no reconciliation, revision or cancellation preservation.

**The exact `dt` semantics could not be re-established from the live primary interface in this audit.** On 2026-09-07, the [exact official endpoint used by the collector](https://iss.moex.com/iss/securities/SBER/dividends.json) returned HTTP 200 with only `description` and `boards`, no `dividends` block or column metadata (response SHA-256 `76586bf51bc2a6349c4e2b83d4817f96caf21ce6db793ad7c8dcc0267511d6a175f`). No original provider response was retained with the export. Registry-close date is a hypothesis supported by cross-source matching below, not verified source semantics. In particular, `dt` must not be mapped to ex-date, payment date, eligibility date or announcement date. U04 needs original provider column metadata or other primary evidence before promoting a registry-date mapping. This primary-semantic evidence is unavailable; the local export itself is available.

The five-field format lacks announcement/payment/ex/eligibility dates, explicit realized/forecast status, revision/cancellation data, stable historical identity/board, source event IDs and per-row source references. No forecast marker was observed; this does not certify that every row is realized. Current-catalogue cross-check: 63 equity tickers, one DR and five unmapped tickers. No global ticker/date ordering; nine ticker groups are not date-ascending.

### Dohod.ru source

The `all_payments` pair contains 1,970 rows for 115 tickers. Its seven columns include a serialized index, announcement date, registry-close date, dividend accounting year, nominal amount, page URL and ticker.

- 208 rows have `(прогноз)` in registry-close text. An unmodified date parser rejects those annotated strings; removing that known annotation **for diagnostics only** yields valid dates for all 208. There are no remaining malformed dates after this diagnostic separation.
- 210 announcement dates and accounting years are missing. All 208 forecast rows lack announcement dates, and two nonforecast rows also lack them. Retrieval time cannot supply a missing market knowledge date.
- Unannotated registry dates span 2000-04-24 to 2024-10-17. Including separately parsed forecast dates extends the maximum to 2025-07-31. Announcement dates span 2000-03-31 to 2024-07-15. The filename `all_payments` does not supply a payment-date field.
- 200 nonforecast rows have announcement dates after their registry dates. They require historical-semantic investigation before feature use; this ordering check alone does not establish that the economic event is erroneous.
- There are 47 repeated `(ticker, registry-text)` groups, 94 rows involved and 47 excess rows. Forty-six groups have different nominal amounts. One group repeats `(ticker, registry-text, amount)` but differs in other fields; there are zero exact duplicate payload rows. Same-date events cannot automatically be collapsed.
- Amounts are finite and nonnegative, including 73 zero amounts. Currency is absent for every row; it must not be assumed to be RUB.
- All 1,970 rows have a `www.dohod.ru` source page URL. Explicit source event IDs, stable identity/ISIN, cancellation/revision status, ex/eligibility/payment dates and complete coverage attestations are absent.
- Current-catalogue cross-check: 108 equity tickers, one DR and six unmapped tickers. Neither global ticker nor date order is ascending; 107 ticker groups are not date-ascending. Sorting for comparison is not a source correction.

### Cross-source diagnostic

Matching only ticker and parsed date, treating MOEX `dt` as a **provisional** registry-date candidate and excluding Dohod forecasts, yields 603 shared keys and 624 row pairs. Of these pairs, 463 have equal nominal amounts within absolute tolerance `1e-8`, and 161 differ, across 157 date keys. This is a diagnostic candidate join, not reconciled events: Dohod currency is missing, stable historical identity is unproved, and multiple payouts can share a registry date. The matches support investigating registry-close semantics but do not authenticate the MOEX date field. Both source contributions must survive S01 intact.

## Reference and identity findings

- Current `moex_stocks.csv`: 263 unique instruments/tickers, 262 equities and one DR, snapshot `DATESTAMP=2026-06-29`. Identity/ISIN/board-text fields are nonnull, but currency is missing once.
- `moex_stocks.xlsx`: 262 instruments, 261 nonnull unique tickers, snapshot `DATESTAMP=2024-08-02`, 252 equities and ten DRs. One ticker and one board-text value are missing; currency is missing ten times. It lacks the CSV's `DISCLOSURE_IS_LIMITED` column. The inspected writer has the stock XLSX write commented out. This is a stale distinct snapshot, not an interchangeable format copy.
- Stock snapshots share 247 tickers: 16 appear only in the current CSV, 14 only in the old XLSX. All shared tickers retain `INSTRUMENT_ID`, but `OZON` and `ETLN` change ISIN. These facts do not prove unrelated ticker reuse or provide effective intervals; they demonstrate why a ticker-only or historical ISIN backfill is unsafe.
- Current `moex_full` CSV/XLSX: 4,270 unique instrument IDs; 3,853 nonnull unique tickers. There are 417 missing tickers and ISINs and 641 missing board-text values. Type counts: 3,746 bonds, 262 equities, 237 investment units, 24 Eurobonds and one DR. It is explicitly not stock-only.
- `ISS_BOARDS` is concatenated human-readable trading-mode text, including multiple modes, not a canonical board code or date-effective board relation. `LISTING_LEVEL_HIST` is present for all reference rows but is unstructured listing-tier history, not a complete historical identity/universe table. `DATESTAMP`, registration, decision and inclusion dates have different meanings; none is an automatic `effective_from`/`effective_to` contract.
- `tickers_dates` CSV/XLSX: 263 unique tickers; `issue_date` spans 2003-07-10 to 2026-06-24; all 263 `stopped_date` values are missing. The collector also leaves missing values on failed lookups and only selects `stock_shares` in its current-status lookup. Null stopped dates therefore cannot prove that all instruments are active or that DR/delisting coverage is complete.
- The distinct `ticker_dates.xlsx` has 3,101 unique `ticker` values, `date_from`/`date_till` rather than the plural-file schema, and 116 missing values in each date column. Date-from coverage is 1997-03-24 to 2025-03-24 and date-till coverage 2012-03-02 to 2025-03-24. Its producer is not identified by the inspected current writer, which writes the plural filename. Against the current catalogue it contains 254 equities, one DR, 1,979 other instruments and 867 unmapped tickers. It cannot be substituted for historical equity listing intervals.
- No reference candidate has duplicate nonnull ticker keys or duplicate instrument IDs where available. All are not globally ticker-sorted. Absence of collisions within current snapshots does not establish absence of historical ticker reuse. Historical inactive/delisted coverage remains unverified.

## Five code-review hypotheses

| Hypothesis | Main code | Local code / paired notebook | Actual-file conclusion |
|---|---|---|---|
| Equity/DR selection is overwritten by the full catalogue | Confirmed in `moex_tickerlists`: full-catalogue assignment follows the filtered assignment | Rejected for this version: both overwriting assignments are commented out; full catalogue is passed separately for type lookup | Daily files match 262 current equity tickers plus one DR; no observed other types or unmapped tickers. This does not prove the producing revision or a historical stock-only universe. |
| `range(1, years)` causes an off-by-one history label | The helper `moex` requests `years-1` 365-day slices | Same helper behavior | Confirmed for the helper, **not established as the cause of these exports**: current full-reload/update entrypoints use `moex_query` directly, with `years*365` on full reload. The demonstrated export problem is truncated coverage/no pagination. |
| RSI14 is TA-Lib RSI(14) on raw close grouped only by ticker | Confirmed call, period, input column and grouping; no internal sort | Same behavior | No supplied RSI exists in any daily candidate. The collector adds no price adjustment, but exact provider basis, TA-Lib version and successful calculation provenance are unverified. The entire raw-basis/provided-RSI claim is not proven. |
| MOEX dividend `dt` is registry-close date | Positional field 2 is copied to `dt`, original metadata discarded | Same behavior | **Unverified from primary evidence**: current endpoint has no dividend block; original response schema absent. Cross-source matches support a provisional hypothesis only. Never map to ex/payment/knowledge dates. |
| Dohod exports include forecasts and missing announcement dates | Scraped tables are saved without an explicit forecast-removal or announcement-completeness gate | Same behavior | Confirmed: 208 forecast rows, 210 missing announcement dates, including two nonforecast rows. |

Main evidence locations are `data_gathering.py` (`moex_tickerlists`, `moex_query`, `moex`, `full_reload`, `data_update`, `build_tickers_dates`), `tech.py` (`calculator`), `dividends.py` (`div_loader`, `main`), `dohodru_data.py` (`get_page_info`, `main`), and `upload.py` (`gdoc_upload`, `ftp_upload`). Use the pinned base revision above; local counterparts are identified by the byte hashes below. Do not read a changed future main as the audited implementation.

## Capability decisions and smallest follow-ups

| Capability | Decision for the audited files | Evidence required to reach GO |
|---|---|---|
| Mechanics import into canonical `daily_features` using supplied RSI | **NO-GO** | An enriched producer file with supplied RSI/provenance and explicitly bounded adequate coverage, deterministic identity compatibility at the mechanics boundary, valid daily intervals/prices and resolved source-key conflicts. Preserving raw bytes separately is GO for archival evidence only. No silent RSI calculation or bad-row dropping. |
| Strict daily research import | **NO-GO** | Complete required history plus warm-up/label tails; stable identity and board history; verified adjusted OHLC and RSI basis/grouping provenance; validated uniqueness, data quality and source coverage. R02 freezes the RSI contract; S01/S02/S06 reconcile intake and identity. |
| Strict dividend import | **NO-GO** | Verified date semantics, stable event/instrument identity, currency, realized/forecast and revision policy, reconciled conflicts and coverage. Feature knowledge dates, label/share-basis requirements and portfolio payment/eligibility dates need separate capability evidence. R01/U04/S06 supply the contracts and evidence. |
| Point-in-time universe | **NO-GO** | Source-backed historical classifications, listings/delistings, board/ticker/ISIN transitions and effective intervals; complete inactive coverage or explicit coverage limits. Current catalogues and blank stopped dates are insufficient. S02 owns reconciliation. |
| Adjusted-price research | **NO-GO** | Complete verified `adjusted_open/high/low/close` and source factors/actions on the `split_and_corporate_action_adjusted_ex_cash_dividends` basis, with cash dividends excluded, plus stable-identity adjusted-price RSI provenance. No raw-equals-adjusted shortcut. U03 depends on R02. |

Decisions apply to the inspected full-history candidates. A Study 001 universe/window was not supplied or frozen here; the audit does not choose a convenient subset, omit terminal events, or approve a whole eight-table bundle. Date-effective lots, calendars, benchmarks, corporate actions and liquidity provenance remain separate bundle gates.

### Producer responsibilities

1. **UX01 pagination correction is warranted as a follow-up recommendation.** The code defect and omitted SBER history are demonstrated, affect the confirmed source snapshot, and cannot be repaired by a local-file adapter. Keep the prospective Issue to one pagination/coverage defect and mocked pagination tests; require a newly audited output after the owner selects its target scope. Exact Study 001 impact remains dependent on the later frozen universe/window. This audit opens no collector-fix Issue and changes no collector.
2. Establish a successful enriched-export handoff with preserved legacy `RSI14` and calculation provenance. The missing column does not establish whether indicator execution failed, was skipped or was later overwritten; investigate that production stage before selecting a repair. U01 does not run it.
3. **U02 is recommended**, for versioned file identity, actual per-ticker coverage, calculation settings and completed-stage evidence. It is not a prerequisite for consumer byte hashing or S01.
4. **U03 is required by the observed adjusted-price/RSI gaps**, after R02 freezes its contract and a verified adjustment source is available. **U04 is required by dividend gaps**, after R01. Primary MOEX date metadata is an explicit additional U04 blocker. These are dependency recommendations; neither lane is activated here.
5. Resolve stale format variants and source data anomalies through producer evidence. Do not round-trip CSV through XLSX, silently discard the conflicting ABIO record, convert missing currency to RUB, or replace future dates with guessed historical dates.

### Minimal S01 consumer behavior

- Preserve and independently hash the exact chosen file bytes; verify optional upstream attestations. Bind parser/mapping versions, source labels, observed coverage and warnings into intake identity. Retrieval timestamps remain traceability fields, not market knowledge or stable identity inputs.
- Read local snapshots only; use neither upstream Python imports nor a fresh collector. Read identifiers as text at the mapping boundary, record serialized index columns as source artifacts and never treat them as instrument identity.
- Use explicit date formats and normalize `RSI14 -> rsi14` only when supplied. Missing RSI is a declared capability blocker, not an instruction to calculate it. Preserve original values and forecasting annotations in evidence.
- Validate uniqueness on parsed ticker/session keys, valid daily intervals and OHLC, plus explicit coverage. Reject ambiguous/non-equity mappings and unresolved conflicts at canonical intake; a current equity/DR cross-check cannot authorize historical backfill or turn a DR into an ordinary share.
- Keep MOEX and Dohod source contributions distinct. Preserve missing values, candidate repeated events, source URLs, forecasts and conflicts; do not merge them into canonical realized events in S01.
- Treat full/stocks reference variants and singular/plural ticker-date files as different source contracts. Do not invent board IDs from `ISS_BOARDS` text or assign current instrument metadata backward in time.
- Report blockers as data issue, upstream export issue, adapter requirement or expected limitation. None of this audit's source problems authorizes a model/pipeline workaround.

## CSV/XLSX comparisons

Comparisons ignore only the serialized dataframe index. Byte identity is always separate. Daily prices/value differ in their exact parsed binary floating representation, but every compared numeric value is within `atol=1e-12, rtol=1e-12`; counts, strings and keys match. This tolerance is diagnostic only, not permission to substitute or rewrite the consumed bytes.

| Pair | CSV / XLSX rows | Same columns | Exact unequal cells by column | Unequal after numeric tolerance |
|---|---|---|---|---|
| `30years_data_1d_interval` | 127149 / 127149 | True | {"open": 152, "close": 186, "high": 158, "low": 171, "value": 2420} | {} |
| `10years_data_1d_interval` | 127100 / 127100 | True | {"open": 112, "close": 127, "high": 122, "low": 118, "value": 2458} | {} |
| `dividends/all` | 661 / 661 | True | {"value": 2} | {} |
| `dividends/dohodru/all_payments` | 1970 / 1970 | True | {} | {} |
| `ticker_lists/moex_stocks` | 263 / 262 | False | "not comparable" | "not comparable" |
| `ticker_lists/moex_full` | 4270 / 4270 | True | {} | {} |
| `ticker_lists/tickers_dates` | 263 / 263 | True | {} | {} |

## File inventory and schema evidence

All paths below are relative to the owner-confirmed producer root. The full byte hash identifies each file independently, including both formats. Each detailed schema includes every observed column and its null count; an absent field is unavailable, not zero-null.

| ID | Relative file | Bytes | Rows | Unique nonnull tickers | SHA-256 |
|---|---|---|---|---|---|
| F01 | `datasets/30years_data_1d_interval.csv` | 10903948 | 127149 | 263 | `9e1d957abd9d17cbd4f724dc01ac198d0bd14d8f82a90b21bf5ac6eb5ba44156` |
| F02 | `datasets/30years_data_1d_interval.xlsx` | 5976852 | 127149 | 263 | `897c5cf6e6afd19c0f7533046d9850a0cee13146b5f2d7ca84f8b4ca52fb48ae` |
| F03 | `datasets/10years_data_1d_interval.csv` | 10864330 | 127100 | 263 | `f80cceb60e5ca78eaca17eed0cacd27ceebdd37d5c6e3da9d1d7cabb7d7e85f4` |
| F04 | `datasets/10years_data_1d_interval.xlsx` | 5802845 | 127100 | 263 | `11906ad71809e39bc81f9ab7eb4647414ceac4729e549e3dd76c596536cb0780` |
| F05 | `datasets/dividends/all.csv` | 25840 | 661 | 69 | `08f47f9b0dfbe9182981bfa0c89e70621c0157604de18818aedd98286386c67a` |
| F06 | `datasets/dividends/all.xlsx` | 23944 | 661 | 69 | `6f12f4cab9b6608ea40d7cbac83743b4c18f154223d36ddcd0295c96ad0d31de` |
| F07 | `datasets/dividends/dohodru/all_payments.csv` | 178701 | 1970 | 115 | `cf89d6396c8faff1f19cfb6d6fe9d6029274364343b4d7f977dc93f17dae1397` |
| F08 | `datasets/dividends/dohodru/all_payments.xlsx` | 102823 | 1970 | 115 | `5bf9712d9e15b4c6898c4408b0570ee2973cb7a7f4221b45ee72ca4b387fe916` |
| F09 | `datasets/ticker_lists/moex_stocks.csv` | 562571 | 263 | 263 | `ecc5df3f303dfef7d4e8cf7b42b5746a754bcf3a2a388fb35cc56b445b542a47` |
| F10 | `datasets/ticker_lists/moex_stocks.xlsx` | 76154 | 262 | 261 | `0f8f33ea47cd4e4f7ffb7837429238a184f133295e131a6be4fb6638a7a0c43b` |
| F11 | `datasets/ticker_lists/moex_full.csv` | 43415424 | 4270 | 3853 | `24c53fffc551c4cd7cd96e9239191f79109eb75c07e371e6acbc9967e94e5287` |
| F12 | `datasets/ticker_lists/moex_full.xlsx` | 3606242 | 4270 | 3853 | `7ca3c92969284c31a94209d67aa3c19b3f96aec0de9ee11c4939df8703d49eef` |
| F13 | `datasets/ticker_lists/tickers_dates.csv` | 5496 | 263 | 263 | `87aa7ab7b07c1f833e6469d82d640c691aa6b6bbf655abe718da523db19f0d36` |
| F14 | `datasets/ticker_lists/tickers_dates.xlsx` | 11757 | 263 | 263 | `a0983fd1a0b1f9e14d2a492c1be66c0986d27a9db7ea3773a6e7ea7141e07351` |
| F15 | `datasets/ticker_lists/ticker_dates.xlsx` | 89742 | 3101 | 3101 | `b8cec0af4a27db94d3bfbd77d60d967cf0fd64a33b6516e046c581cf0cab5428` |

<details>
<summary>F01: datasets/30years_data_1d_interval.csv</summary>


Rows: 127149; columns: 9; exact duplicate excess (all columns / excluding export index): 0 / 0.

Mutation/versioning: Inspected writer overwrites this fixed filename; no versioned manifest supplied.

| Column (source order) | Observed dtype | Missing |
|---|---|---|
| `open` | float64 | 0 |
| `close` | float64 | 0 |
| `high` | float64 | 0 |
| `low` | float64 | 0 |
| `value` | float64 | 0 |
| `volume` | int64 | 0 |
| `begin` | object | 0 |
| `end` | object | 0 |
| `ticker` | object | 0 |

| Date field | Actual parsed min | Actual parsed max | Missing | Nonempty unparseable |
|---|---|---|---|---|
| `begin` | 2003-07-10 00:00:00 | 2026-06-30 00:00:00 | 0 | 0 |
| `end` | 2003-07-10 23:59:59 | 2026-06-30 19:21:34 | 0 | 0 |

Key check: `ticker, date`; missing-key rows 0; repeated groups 1; involved rows 2; excess rows 1.

</details>

<details>
<summary>F02: datasets/30years_data_1d_interval.xlsx</summary>


Rows: 127149; columns: 9; exact duplicate excess (all columns / excluding export index): 0 / 0.

Mutation/versioning: Inspected writer overwrites this fixed filename; no versioned manifest supplied.

| Column (source order) | Observed dtype | Missing |
|---|---|---|
| `open` | float64 | 0 |
| `close` | float64 | 0 |
| `high` | float64 | 0 |
| `low` | float64 | 0 |
| `value` | float64 | 0 |
| `volume` | int64 | 0 |
| `begin` | object | 0 |
| `end` | object | 0 |
| `ticker` | object | 0 |

| Date field | Actual parsed min | Actual parsed max | Missing | Nonempty unparseable |
|---|---|---|---|---|
| `begin` | 2003-07-10 00:00:00 | 2026-06-30 00:00:00 | 0 | 0 |
| `end` | 2003-07-10 23:59:59 | 2026-06-30 19:21:34 | 0 | 0 |

Key check: `ticker, date`; missing-key rows 0; repeated groups 1; involved rows 2; excess rows 1.

</details>

<details>
<summary>F03: datasets/10years_data_1d_interval.csv</summary>


Rows: 127100; columns: 9; exact duplicate excess (all columns / excluding export index): 0 / 0.

Mutation/versioning: Inspected writer overwrites this fixed filename; no versioned manifest supplied.

| Column (source order) | Observed dtype | Missing |
|---|---|---|
| `open` | float64 | 0 |
| `close` | float64 | 0 |
| `high` | float64 | 0 |
| `low` | float64 | 0 |
| `value` | float64 | 0 |
| `volume` | int64 | 0 |
| `begin` | object | 0 |
| `end` | object | 0 |
| `ticker` | object | 0 |

| Date field | Actual parsed min | Actual parsed max | Missing | Nonempty unparseable |
|---|---|---|---|---|
| `begin` | 2016-07-04 00:00:00 | 2026-06-30 00:00:00 | 0 | 0 |
| `end` | 2016-07-04 23:59:59 | 2026-06-30 18:16:54 | 0 | 0 |

Key check: `ticker, date`; missing-key rows 0; repeated groups 0; involved rows 0; excess rows 0.

</details>

<details>
<summary>F04: datasets/10years_data_1d_interval.xlsx</summary>


Rows: 127100; columns: 9; exact duplicate excess (all columns / excluding export index): 0 / 0.

Mutation/versioning: Inspected writer overwrites this fixed filename; no versioned manifest supplied.

| Column (source order) | Observed dtype | Missing |
|---|---|---|
| `open` | float64 | 0 |
| `close` | float64 | 0 |
| `high` | float64 | 0 |
| `low` | float64 | 0 |
| `value` | float64 | 0 |
| `volume` | int64 | 0 |
| `begin` | object | 0 |
| `end` | object | 0 |
| `ticker` | object | 0 |

| Date field | Actual parsed min | Actual parsed max | Missing | Nonempty unparseable |
|---|---|---|---|---|
| `begin` | 2016-07-04 00:00:00 | 2026-06-30 00:00:00 | 0 | 0 |
| `end` | 2016-07-04 23:59:59 | 2026-06-30 18:16:54 | 0 | 0 |

Key check: `ticker, date`; missing-key rows 0; repeated groups 0; involved rows 0; excess rows 0.

</details>

<details>
<summary>F05: datasets/dividends/all.csv</summary>


Rows: 661; columns: 5; exact duplicate excess (all columns / excluding export index): 0 / 0.

Mutation/versioning: Inspected writer overwrites this fixed filename; no versioned manifest supplied.

| Column (source order) | Observed dtype | Missing |
|---|---|---|
| `ISIN` | object | 0 |
| `TRADE_CODE` | object | 0 |
| `dt` | object | 0 |
| `value` | float64 | 0 |
| `currency` | object | 0 |

| Date field | Actual parsed min | Actual parsed max | Missing | Nonempty unparseable |
|---|---|---|---|---|
| `dt` | 2013-08-02 00:00:00 | 2111-01-01 00:00:00 | 0 | 0 |

Key check: `TRADE_CODE, dt`; missing-key rows 0; repeated groups 0; involved rows 0; excess rows 0.

</details>

<details>
<summary>F06: datasets/dividends/all.xlsx</summary>


Rows: 661; columns: 5; exact duplicate excess (all columns / excluding export index): 0 / 0.

Mutation/versioning: Inspected writer overwrites this fixed filename; no versioned manifest supplied.

| Column (source order) | Observed dtype | Missing |
|---|---|---|
| `ISIN` | object | 0 |
| `TRADE_CODE` | object | 0 |
| `dt` | object | 0 |
| `value` | float64 | 0 |
| `currency` | object | 0 |

| Date field | Actual parsed min | Actual parsed max | Missing | Nonempty unparseable |
|---|---|---|---|---|
| `dt` | 2013-08-02 00:00:00 | 2111-01-01 00:00:00 | 0 | 0 |

Key check: `TRADE_CODE, dt`; missing-key rows 0; repeated groups 0; involved rows 0; excess rows 0.

</details>

<details>
<summary>F07: datasets/dividends/dohodru/all_payments.csv</summary>


Rows: 1970; columns: 7; exact duplicate excess (all columns / excluding export index): 0 / 0.

Mutation/versioning: Inspected writer overwrites this fixed filename; no versioned manifest supplied.

| Column (source order) | Observed dtype | Missing |
|---|---|---|
| `Unnamed: 0` | int64 | 0 |
| `Дата объявления дивиденда` | object | 210 |
| `Дата закрытия реестра` | object | 0 |
| `Год для учета дивиденда` | float64 | 210 |
| `Дивиденд` | float64 | 0 |
| `page_url` | object | 0 |
| `ticker` | object | 0 |

| Date field | Actual parsed min | Actual parsed max | Missing | Nonempty unparseable |
|---|---|---|---|---|
| `Дата закрытия реестра` | 2000-04-24 00:00:00 | 2024-10-17 00:00:00 | 0 | 208 |
| `Дата объявления дивиденда` | 2000-03-31 00:00:00 | 2024-07-15 00:00:00 | 210 | 0 |

Key check: `ticker, Дата закрытия реестра`; missing-key rows 0; repeated groups 47; involved rows 94; excess rows 47.

</details>

<details>
<summary>F08: datasets/dividends/dohodru/all_payments.xlsx</summary>


Rows: 1970; columns: 7; exact duplicate excess (all columns / excluding export index): 0 / 0.

Mutation/versioning: Inspected writer overwrites this fixed filename; no versioned manifest supplied.

| Column (source order) | Observed dtype | Missing |
|---|---|---|
| `Unnamed: 0` | int64 | 0 |
| `Дата объявления дивиденда` | object | 210 |
| `Дата закрытия реестра` | object | 0 |
| `Год для учета дивиденда` | float64 | 210 |
| `Дивиденд` | float64 | 0 |
| `page_url` | object | 0 |
| `ticker` | object | 0 |

| Date field | Actual parsed min | Actual parsed max | Missing | Nonempty unparseable |
|---|---|---|---|---|
| `Дата закрытия реестра` | 2000-04-24 00:00:00 | 2024-10-17 00:00:00 | 0 | 208 |
| `Дата объявления дивиденда` | 2000-03-31 00:00:00 | 2024-07-15 00:00:00 | 210 | 0 |

Key check: `ticker, Дата закрытия реестра`; missing-key rows 0; repeated groups 47; involved rows 94; excess rows 47.

</details>

<details>
<summary>F09: datasets/ticker_lists/moex_stocks.csv</summary>


Rows: 263; columns: 48; exact duplicate excess (all columns / excluding export index): 0 / 0.

Mutation/versioning: Inspected writer overwrites this fixed filename; no versioned manifest supplied.

| Column (source order) | Observed dtype | Missing |
|---|---|---|
| `Unnamed: 0` | int64 | 0 |
| `DATESTAMP` | object | 0 |
| `INSTRUMENT_ID` | int64 | 0 |
| `LIST_SECTION` | object | 0 |
| `NPP` | int64 | 0 |
| `SUPERTYPE` | object | 0 |
| `INSTRUMENT_TYPE` | object | 0 |
| `INSTRUMENT_CATEGORY` | object | 0 |
| `TRADE_CODE` | object | 0 |
| `ISIN` | object | 0 |
| `REGISTRY_NUMBER` | object | 2 |
| `REGISTRY_DATE` | object | 2 |
| `EMITENT_FULL_NAME` | object | 0 |
| `INN` | float64 | 2 |
| `NOMINAL` | object | 1 |
| `CURRENCY` | object | 1 |
| `ISSUE_AMOUNT` | object | 1 |
| `DECISION_DATE` | object | 0 |
| `OKSM_EDR` | object | 262 |
| `ONLY_EMITENT_FULL_NAME` | object | 262 |
| `REG_COUNTRY` | object | 262 |
| `QUALIFIED_INVESTOR` | object | 261 |
| `HAS_PROSPECTUS` | object | 4 |
| `IS_CONCESSION_AGREEMENT` | float64 | 263 |
| `IS_MORTGAGE_AGENT` | float64 | 263 |
| `INCLUDED_DURING_CREATION` | float64 | 263 |
| `SECURITY_HAS_DEFAULT` | object | 261 |
| `SECURITY_HAS_TECH_DEFAULT` | object | 258 |
| `DISCLOSURE_IS_LIMITED` | object | 257 |
| `INCLUDED_WITHOUT_COMPLIANCE` | object | 262 |
| `RETAINED_WITHOUT_COMPLIANCE` | object | 208 |
| `HAS_RESTRICTION_CIRCULATION` | float64 | 263 |
| `LISTING_LEVEL_HIST` | object | 0 |
| `OBLIGATION_PROGRAM_RN` | float64 | 263 |
| `COUPON_PERCENT` | float64 | 263 |
| `EARLY_REPAYMENT` | float64 | 263 |
| `EARLY_REDEMPTION` | float64 | 263 |
| `ISS_BOARDS` | object | 0 |
| `OTHER_SECURITIES` | object | 105 |
| `DISCLOSURE_PART_PAGE` | object | 41 |
| `DISCLOSURE_RF_INFO_PAGE` | object | 1 |
| `INCLUDE_DATE` | object | 0 |
| `CFI_FOREIGN` | object | 261 |
| `ISIN_UNDERLYING_ASSET` | object | 262 |
| `CFI_UNDERLYING_ASSET` | object | 262 |
| `PIF_STATUS` | float64 | 263 |
| `PIF_STATUS_HIST` | float64 | 263 |
| `OBLIGATION_PROGRAM_DATE` | float64 | 263 |

| Date field | Actual parsed min | Actual parsed max | Missing | Nonempty unparseable |
|---|---|---|---|---|
| `DATESTAMP` | 2026-06-29 00:00:00 | 2026-06-29 00:00:00 | 0 | 0 |
| `REGISTRY_DATE` | 1992-11-19 00:00:00 | 2025-09-15 00:00:00 | 2 | 0 |
| `DECISION_DATE` | 2004-11-26 00:00:00 | 2026-06-10 00:00:00 | 0 | 0 |
| `INCLUDE_DATE` | 2014-06-09 00:00:00 | 2026-06-24 00:00:00 | 0 | 0 |

Key check: `TRADE_CODE`; missing-key rows 0; repeated groups 0; involved rows 0; excess rows 0.

</details>

<details>
<summary>F10: datasets/ticker_lists/moex_stocks.xlsx</summary>


Rows: 262; columns: 47; exact duplicate excess (all columns / excluding export index): 0 / 0.

Mutation/versioning: Current stock XLSX write is disabled; this retained older snapshot has no versioned manifest.

| Column (source order) | Observed dtype | Missing |
|---|---|---|
| `Unnamed: 0` | int64 | 0 |
| `DATESTAMP` | object | 0 |
| `INSTRUMENT_ID` | int64 | 0 |
| `LIST_SECTION` | object | 0 |
| `NPP` | int64 | 0 |
| `SUPERTYPE` | object | 0 |
| `INSTRUMENT_TYPE` | object | 0 |
| `INSTRUMENT_CATEGORY` | object | 0 |
| `TRADE_CODE` | object | 1 |
| `ISIN` | object | 0 |
| `REGISTRY_NUMBER` | object | 12 |
| `REGISTRY_DATE` | object | 12 |
| `EMITENT_FULL_NAME` | object | 0 |
| `INN` | float64 | 12 |
| `NOMINAL` | object | 10 |
| `CURRENCY` | object | 10 |
| `ISSUE_AMOUNT` | object | 10 |
| `DECISION_DATE` | object | 0 |
| `OKSM_EDR` | object | 252 |
| `ONLY_EMITENT_FULL_NAME` | object | 252 |
| `REG_COUNTRY` | object | 252 |
| `QUALIFIED_INVESTOR` | object | 261 |
| `HAS_PROSPECTUS` | object | 15 |
| `IS_CONCESSION_AGREEMENT` | float64 | 262 |
| `IS_MORTGAGE_AGENT` | float64 | 262 |
| `INCLUDED_DURING_CREATION` | float64 | 262 |
| `SECURITY_HAS_DEFAULT` | object | 260 |
| `SECURITY_HAS_TECH_DEFAULT` | object | 260 |
| `INCLUDED_WITHOUT_COMPLIANCE` | object | 261 |
| `RETAINED_WITHOUT_COMPLIANCE` | object | 217 |
| `HAS_RESTRICTION_CIRCULATION` | float64 | 262 |
| `LISTING_LEVEL_HIST` | object | 0 |
| `OBLIGATION_PROGRAM_RN` | float64 | 262 |
| `COUPON_PERCENT` | float64 | 262 |
| `EARLY_REPAYMENT` | float64 | 262 |
| `EARLY_REDEMPTION` | float64 | 262 |
| `ISS_BOARDS` | object | 1 |
| `OTHER_SECURITIES` | object | 106 |
| `DISCLOSURE_PART_PAGE` | object | 59 |
| `DISCLOSURE_RF_INFO_PAGE` | object | 1 |
| `INCLUDE_DATE` | object | 0 |
| `CFI_FOREIGN` | object | 250 |
| `ISIN_UNDERLYING_ASSET` | object | 252 |
| `CFI_UNDERLYING_ASSET` | object | 252 |
| `PIF_STATUS` | float64 | 262 |
| `PIF_STATUS_HIST` | float64 | 262 |
| `OBLIGATION_PROGRAM_DATE` | float64 | 262 |

| Date field | Actual parsed min | Actual parsed max | Missing | Nonempty unparseable |
|---|---|---|---|---|
| `DATESTAMP` | 2024-08-02 00:00:00 | 2024-08-02 00:00:00 | 0 | 0 |
| `REGISTRY_DATE` | 1992-11-19 00:00:00 | 2024-05-27 00:00:00 | 12 | 0 |
| `DECISION_DATE` | 2004-11-26 00:00:00 | 2024-07-22 00:00:00 | 0 | 0 |
| `INCLUDE_DATE` | 2014-06-09 00:00:00 | 2024-07-30 00:00:00 | 0 | 0 |

Key check: `TRADE_CODE`; missing-key rows 1; repeated groups 0; involved rows 0; excess rows 0.

</details>

<details>
<summary>F11: datasets/ticker_lists/moex_full.csv</summary>


Rows: 4270; columns: 48; exact duplicate excess (all columns / excluding export index): 0 / 0.

Mutation/versioning: Inspected writer overwrites this fixed filename; no versioned manifest supplied.

| Column (source order) | Observed dtype | Missing |
|---|---|---|
| `Unnamed: 0` | int64 | 0 |
| `DATESTAMP` | object | 0 |
| `INSTRUMENT_ID` | int64 | 0 |
| `LIST_SECTION` | object | 0 |
| `NPP` | int64 | 0 |
| `SUPERTYPE` | object | 0 |
| `INSTRUMENT_TYPE` | object | 0 |
| `INSTRUMENT_CATEGORY` | object | 0 |
| `TRADE_CODE` | object | 417 |
| `ISIN` | object | 417 |
| `REGISTRY_NUMBER` | object | 35 |
| `REGISTRY_DATE` | object | 35 |
| `EMITENT_FULL_NAME` | object | 0 |
| `INN` | float64 | 19 |
| `NOMINAL` | object | 238 |
| `CURRENCY` | object | 238 |
| `ISSUE_AMOUNT` | object | 238 |
| `DECISION_DATE` | object | 0 |
| `OKSM_EDR` | object | 4269 |
| `ONLY_EMITENT_FULL_NAME` | object | 4269 |
| `REG_COUNTRY` | object | 4253 |
| `QUALIFIED_INVESTOR` | object | 3275 |
| `HAS_PROSPECTUS` | object | 1848 |
| `IS_CONCESSION_AGREEMENT` | object | 4246 |
| `IS_MORTGAGE_AGENT` | object | 4170 |
| `INCLUDED_DURING_CREATION` | float64 | 4270 |
| `SECURITY_HAS_DEFAULT` | object | 4209 |
| `SECURITY_HAS_TECH_DEFAULT` | object | 3590 |
| `DISCLOSURE_IS_LIMITED` | object | 4240 |
| `INCLUDED_WITHOUT_COMPLIANCE` | object | 4269 |
| `RETAINED_WITHOUT_COMPLIANCE` | object | 3976 |
| `HAS_RESTRICTION_CIRCULATION` | object | 4266 |
| `LISTING_LEVEL_HIST` | object | 0 |
| `OBLIGATION_PROGRAM_RN` | object | 1438 |
| `COUPON_PERCENT` | object | 1135 |
| `EARLY_REPAYMENT` | object | 3668 |
| `EARLY_REDEMPTION` | object | 524 |
| `ISS_BOARDS` | object | 641 |
| `OTHER_SECURITIES` | object | 301 |
| `DISCLOSURE_PART_PAGE` | object | 336 |
| `DISCLOSURE_RF_INFO_PAGE` | object | 160 |
| `INCLUDE_DATE` | object | 0 |
| `CFI_FOREIGN` | object | 4235 |
| `ISIN_UNDERLYING_ASSET` | object | 4269 |
| `CFI_UNDERLYING_ASSET` | object | 4269 |
| `PIF_STATUS` | object | 4033 |
| `PIF_STATUS_HIST` | object | 4033 |
| `OBLIGATION_PROGRAM_DATE` | object | 1438 |

| Date field | Actual parsed min | Actual parsed max | Missing | Nonempty unparseable |
|---|---|---|---|---|
| `DATESTAMP` | 2026-06-29 00:00:00 | 2026-06-29 00:00:00 | 0 | 0 |
| `REGISTRY_DATE` | 1992-11-19 00:00:00 | 2026-06-29 00:00:00 | 35 | 0 |
| `DECISION_DATE` | 2004-11-26 00:00:00 | 2026-06-29 00:00:00 | 0 | 0 |
| `INCLUDE_DATE` | 2014-06-09 00:00:00 | 2026-06-29 00:00:00 | 0 | 0 |

Key check: `TRADE_CODE`; missing-key rows 417; repeated groups 0; involved rows 0; excess rows 0.

</details>

<details>
<summary>F12: datasets/ticker_lists/moex_full.xlsx</summary>


Rows: 4270; columns: 48; exact duplicate excess (all columns / excluding export index): 0 / 0.

Mutation/versioning: Inspected writer overwrites this fixed filename; no versioned manifest supplied.

| Column (source order) | Observed dtype | Missing |
|---|---|---|
| `Unnamed: 0` | int64 | 0 |
| `DATESTAMP` | object | 0 |
| `INSTRUMENT_ID` | int64 | 0 |
| `LIST_SECTION` | object | 0 |
| `NPP` | int64 | 0 |
| `SUPERTYPE` | object | 0 |
| `INSTRUMENT_TYPE` | object | 0 |
| `INSTRUMENT_CATEGORY` | object | 0 |
| `TRADE_CODE` | object | 417 |
| `ISIN` | object | 417 |
| `REGISTRY_NUMBER` | object | 35 |
| `REGISTRY_DATE` | object | 35 |
| `EMITENT_FULL_NAME` | object | 0 |
| `INN` | float64 | 19 |
| `NOMINAL` | object | 238 |
| `CURRENCY` | object | 238 |
| `ISSUE_AMOUNT` | object | 238 |
| `DECISION_DATE` | object | 0 |
| `OKSM_EDR` | object | 4269 |
| `ONLY_EMITENT_FULL_NAME` | object | 4269 |
| `REG_COUNTRY` | object | 4253 |
| `QUALIFIED_INVESTOR` | object | 3275 |
| `HAS_PROSPECTUS` | object | 1848 |
| `IS_CONCESSION_AGREEMENT` | object | 4246 |
| `IS_MORTGAGE_AGENT` | object | 4170 |
| `INCLUDED_DURING_CREATION` | float64 | 4270 |
| `SECURITY_HAS_DEFAULT` | object | 4209 |
| `SECURITY_HAS_TECH_DEFAULT` | object | 3590 |
| `DISCLOSURE_IS_LIMITED` | object | 4240 |
| `INCLUDED_WITHOUT_COMPLIANCE` | object | 4269 |
| `RETAINED_WITHOUT_COMPLIANCE` | object | 3976 |
| `HAS_RESTRICTION_CIRCULATION` | object | 4266 |
| `LISTING_LEVEL_HIST` | object | 0 |
| `OBLIGATION_PROGRAM_RN` | object | 1438 |
| `COUPON_PERCENT` | object | 1135 |
| `EARLY_REPAYMENT` | object | 3668 |
| `EARLY_REDEMPTION` | object | 524 |
| `ISS_BOARDS` | object | 641 |
| `OTHER_SECURITIES` | object | 301 |
| `DISCLOSURE_PART_PAGE` | object | 336 |
| `DISCLOSURE_RF_INFO_PAGE` | object | 160 |
| `INCLUDE_DATE` | object | 0 |
| `CFI_FOREIGN` | object | 4235 |
| `ISIN_UNDERLYING_ASSET` | object | 4269 |
| `CFI_UNDERLYING_ASSET` | object | 4269 |
| `PIF_STATUS` | object | 4033 |
| `PIF_STATUS_HIST` | object | 4033 |
| `OBLIGATION_PROGRAM_DATE` | object | 1438 |

| Date field | Actual parsed min | Actual parsed max | Missing | Nonempty unparseable |
|---|---|---|---|---|
| `DATESTAMP` | 2026-06-29 00:00:00 | 2026-06-29 00:00:00 | 0 | 0 |
| `REGISTRY_DATE` | 1992-11-19 00:00:00 | 2026-06-29 00:00:00 | 35 | 0 |
| `DECISION_DATE` | 2004-11-26 00:00:00 | 2026-06-29 00:00:00 | 0 | 0 |
| `INCLUDE_DATE` | 2014-06-09 00:00:00 | 2026-06-29 00:00:00 | 0 | 0 |

Key check: `TRADE_CODE`; missing-key rows 417; repeated groups 0; involved rows 0; excess rows 0.

</details>

<details>
<summary>F13: datasets/ticker_lists/tickers_dates.csv</summary>


Rows: 263; columns: 4; exact duplicate excess (all columns / excluding export index): 0 / 0.

Mutation/versioning: Inspected writer overwrites this fixed filename; no versioned manifest supplied.

| Column (source order) | Observed dtype | Missing |
|---|---|---|
| `Unnamed: 0` | int64 | 0 |
| `TRADE_CODE` | object | 0 |
| `issue_date` | object | 0 |
| `stopped_date` | float64 | 263 |

| Date field | Actual parsed min | Actual parsed max | Missing | Nonempty unparseable |
|---|---|---|---|---|
| `issue_date` | 2003-07-10 00:00:00 | 2026-06-24 00:00:00 | 0 | 0 |
| `stopped_date` | unavailable | unavailable | 263 | 0 |

Key check: `TRADE_CODE`; missing-key rows 0; repeated groups 0; involved rows 0; excess rows 0.

</details>

<details>
<summary>F14: datasets/ticker_lists/tickers_dates.xlsx</summary>


Rows: 263; columns: 4; exact duplicate excess (all columns / excluding export index): 0 / 0.

Mutation/versioning: Inspected writer overwrites this fixed filename; no versioned manifest supplied.

| Column (source order) | Observed dtype | Missing |
|---|---|---|
| `Unnamed: 0` | int64 | 0 |
| `TRADE_CODE` | object | 0 |
| `issue_date` | datetime64[ns] | 0 |
| `stopped_date` | float64 | 263 |

| Date field | Actual parsed min | Actual parsed max | Missing | Nonempty unparseable |
|---|---|---|---|---|
| `issue_date` | 2003-07-10 00:00:00 | 2026-06-24 00:00:00 | 0 | 0 |
| `stopped_date` | unavailable | unavailable | 263 | 0 |

Key check: `TRADE_CODE`; missing-key rows 0; repeated groups 0; involved rows 0; excess rows 0.

</details>

<details>
<summary>F15: datasets/ticker_lists/ticker_dates.xlsx</summary>


Rows: 3101; columns: 4; exact duplicate excess (all columns / excluding export index): 0 / 0.

Mutation/versioning: Producer/versioning behavior not identified in the inspected current entrypoints; distinct retained legacy file.

| Column (source order) | Observed dtype | Missing |
|---|---|---|
| `Unnamed: 0` | int64 | 0 |
| `ticker` | object | 0 |
| `date_from` | object | 116 |
| `date_till` | object | 116 |

| Date field | Actual parsed min | Actual parsed max | Missing | Nonempty unparseable |
|---|---|---|---|---|
| `date_from` | 1997-03-24 00:00:00 | 2025-03-24 00:00:00 | 116 | 0 |
| `date_till` | 2012-03-02 00:00:00 | 2025-03-24 00:00:00 | 116 | 0 |

Key check: `ticker`; missing-key rows 0; repeated groups 0; involved rows 0; excess rows 0.

</details>


## Inspected code identities

These hashes describe inspected code, not a verified producing commit. The local source tree remained dirty and untouched. `main.py` notebook conversion makes notebook identity material. Function AST equivalence was checked without executing code for all five `.py`/`.ipynb` pairs in main and local versions; all pairs matched within their respective version.

| Code source | Byte SHA-256 | Observation |
|---|---|---|
| `README.md` (main) | `c07d93e8c9080d7c00f4af5a1f69c500ac88df5082795c23b0d48ad10e201db4` | same local bytes |
| `main.py` (main) | `c76169e8db4bbed89f78fc513390bbab1b367da535c2f2ddd89ca0be0c0114a8` | same local bytes |
| `data_gathering.py` (main) | `0c505726dd99a57d9a1268374292e7d5a527cb6af957a80856da8efb292f85b8` | local differs |
| `data_gathering.py` (local) | `3dff4187655dde00129d4092cbecd1c126fa9ee99bf9e038e2d1b9ee7b305063` | uncommitted source version |
| `tech.py` (main) | `86f1a3f27af64e34a98a070d2ccff4eee6da138150d518d2971d486f1d9a9b28` | same local bytes |
| `dividends.py` (main) | `85fb54b52646d9db6890ac69269466e2dac699d641b51c934e84e319cd1f4bf4` | same local bytes |
| `dohodru_data.py` (main) | `b0a583a096c91679fce6e746e8a1cd056426216ffd2cdd50823819a0607ce04c` | same local bytes |
| `upload.py` (main) | `95945e89d8bc766f934132270a8ee5df75d5382cad86ff44771eb352d5e5acb1` | same local bytes |
| `settings/datasets_config.json` (main) | `96450fd87fed808d5ea0708430bb947c15561efdcc23d02f7ab4ece7e5eea3b7` | same local bytes |
| `datasets/README.md` (main) | `bee0bb7037b280e39f58b928f091fd68b8dc22068aeb6ad9981b7b18dac20144` | same local bytes |
| `data_gathering.ipynb` (main) | `2918db7c6039d48126dd45f3b818c070d0103bb07b2943298238a8ec88df403b` | function AST matches paired script |
| `data_gathering.ipynb` (local) | `412869096e48be99f4aebf6c72625d93875954b4075c806fff1db6513d0f6a2c` | function AST matches paired script |
| `tech.ipynb` (main) | `ed0ab182b1b21e0a72a12d75780b8d21ccba6a716fa82daf833295458de2457a` | function AST matches paired script |
| `tech.ipynb` (local) | `ed0ab182b1b21e0a72a12d75780b8d21ccba6a716fa82daf833295458de2457a` | function AST matches paired script |
| `dividends.ipynb` (main) | `fe887853568ef1c31d36e5c43e317c7c8255adb531fcc0c55600a885f72e4bd5` | function AST matches paired script |
| `dividends.ipynb` (local) | `fe887853568ef1c31d36e5c43e317c7c8255adb531fcc0c55600a885f72e4bd5` | function AST matches paired script |
| `dohodru_data.ipynb` (main) | `0344e364e69d6ab0bf6b579b81d8a45a3bd9f4a5b03599725bc75b3d0c4be9e0` | function AST matches paired script |
| `dohodru_data.ipynb` (local) | `0344e364e69d6ab0bf6b579b81d8a45a3bd9f4a5b03599725bc75b3d0c4be9e0` | function AST matches paired script |
| `upload.ipynb` (main) | `95b150039cd365dd222ea893b16f629c6ba2b0e3b4367b09c9e3b99396a6aa76` | function AST matches paired script |
| `upload.ipynb` (local) | `95b150039cd365dd222ea893b16f629c6ba2b0e3b4367b09c9e3b99396a6aa76` | function AST matches paired script |

## Verification and remaining limits

- All 15 candidates parsed successfully, with independently checked row totals, key duplicates and selected anomalies. All source-byte hashes matched after reads and at final verification.
- Schema/date tables distinguish missing values from unparseable nonempty text; all 208 Dohod non-date strings are explained by preserved forecast annotations. Reference Russian datetimes are accepted explicitly, not incorrectly classified as malformed.
- Daily cap counts, ABIO conflicting-key counts, zero-price rows, forecast counts and announcement-order checks were cross-checked independently. Pair comparisons preserve exact vs tolerant equality and report the stale stock XLSX separately.
- Changed-path allowlist is only `docs/research_export_audit.md`. Whitespace and public-path/secret/data-extract checks are required on the final diff. Audit tools, raw provider checks, source exports and detailed private evidence remain outside the PR.
- Expert checklist reviewed for applicability: no runtime, model, splits, portfolio or trading behavior changed. Existing mechanics validation is not new research evidence. No product tests were added for this documentation change; source analysis and document checks are the relevant verification.
- No supplier run manifest, original MOEX dividend response schema, complete historical universe, adjusted price basis or complete event timing was supplied. Hypotheses with absent evidence remain unverified. The primary endpoint result is a dated observation, not a claim about all future availability.
- The draft audit PR and coordinator PR remain unmerged. Follow-up recommendations are not launched workstreams, and the research-readiness level remains unchanged.

## Field availability matrix

| Category | Observed fields/evidence | Consumer consequence |
|---|---|---|
| available and usable as-is | Exact source bytes and SHA-256; supplied nominal amounts and currency where present; Dohod `page_url`; original indicator-free candle values as source evidence | Preserve unchanged with provenance. Usable as source evidence does not imply canonical or research eligibility. |
| available but requires mapping | `begin/end` and explicit calendar formats; `ticker`/`TRADE_CODE`; reference `INSTRUMENT_ID`/ISIN and `SUPERTYPE`; Dohod announcement/registry text and forecast annotations; serialized index | Deterministic alias/type mapping with original values retained. Current identity/classification only; no as-of or board inference. MOEX `dt` stays unmapped until its source semantics are verified. |
| available but requires upstream enrichment | Truncated candle history; reference board-description/listing-history text; incomplete announcement dates and currency contributions; differing same-date dividend evidence; mutable file outputs | Supply full coverage, structured effective identity/board evidence, reconciled event semantics and versioned calculation/export provenance. Resolve source anomalies without consumer fabrication. |
| unavailable / blocker | Supplied `RSI14` in all daily candidates; verified research RSI/method/basis; adjusted OHLC/factors and exclusion of cash dividends; row-level stable historical identity/board; verified MOEX `dt` metadata; canonical ex/eligibility/payment dates; event revision/cancellation IDs; complete inactive universe and zero-event/period attestations | NO-GO for the corresponding canonical/strict capabilities until evidence is supplied and independently validated. No silent RSI recalculation, raw-price substitution, guessed dates or research-grade claim. |
