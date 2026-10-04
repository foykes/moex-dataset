# Dataset contracts — F0-01

Status: **PROPOSED documentation interfaces**, prepared under the accepted R3 F0-01 review, identifier SHA256 e6635da5a65f47f4b2fbd051caa5a7040347eb17e90a24f4d055b2d42d2ab92b. Nothing here declares a runtime helper, logger, fixture, adapter or completed release. Existing public functions remain unchanged. All illustrative examples below are invented documentation data and were not executed.

## Existing behavior and preserved public data

Sources are the repository files at base 5e4eb07fa24e9ecf1c77c2fefb0adfde453ff1c4. [Dataset config](../settings/datasets_config.json) defines the eight legacy names. [data_gathering.py](../data_gathering.py) writes candle files with index=False (466–467, 557–558); [tech.py](../tech.py) independently reads/writes formats (151–154, 847–850), and [upload.py](../upload.py) filters the Google view (50–60), clears before writing (72–75), catches errors (76–77) and joins publication workers without aggregating their outcomes (173–178). [main.py](../main.py) performs notebook conversion before its main guard (8–15). These observed behaviors do not satisfy the proposed release gate; F0 changes none of them.

Historical OHLC are raw MOEX ISS only: no dividend/split/reorganization adjustment, third-party backfill or invented candles. Calculate once on the maximally available preceding sequence, then slice all time/30y/10y and derive CSV/XLSX from that canonical snapshot. Matching overlaps have matching values at one as_of. Natural keys live in metadata/sidecars: engine, market, source_scope, board, raw_instrument_id, interval, begin. End and pandas row index are not key fields. Preserve raw identities, separate logical successor/display ticker, and accept mappings only with evidence. Retain both conflicting raw rows, provenance and quarantine reasons; evidenced source revision is a different case. Ordinary non-trading gaps do not imply defective candles.

### Time, sidecars and schema version

| Concern | Observed / target / unknown contract |
|---|---|
| Window arithmetic | OBSERVED: data_gathering.py:431–458 uses local datetime, years multiplied by 365, issue_date and stopped_date. F0 does not replace this arithmetic with calendar years. TARGET: record the requested window against one aware UTC as_of; preserve exchange timestamp representation. |
| Interval | OBSERVED IDs 24, 60, 10, 1 mean daily, hourly, ten-minute and one-minute candles. An indicator period counts bars, not calendar days. |
| Request and source boundaries | Requested, instrument, approved-source available and actual accepted boundaries are distinct. Endpoint inclusive/exclusive behavior, board/session timezone and missing lifecycle facts remain UNKNOWN without evidence; do not infer them from MIN/MAX or copy issue_date into first_trade. |
| Current bar | Completion requires interval/board/session evidence against the common cutoff. HTTP 200, a populated end value or a fresh file timestamp alone does not establish a complete bar; missing completion evidence stays unverified. |

**PROPOSED** sidecar: `<dataset_stem>.metadata.json` carries the declared schema version, run/snapshot correlation, raw identity/provenance, boundaries and quality/quarantine evidence links. Large raw/quality artifacts are referenced with hashes rather than added as legacy CSV columns. F0 creates no sidecar or data file.

`contract_version=1` versions these procedural records. `schema_version` identifies the ordered public schema. Changes to names, order, logical types, units, missing-value meaning or calculation meaning need an explicit reviewed contract/version change and consumer migration agreement; they are never silently inferred from a new file. The miniature example schema is an illustrative identifier only. Emission and final exported-schema conformance remain later package work.

## PROPOSED types, correlation and state rules

**R** is required non-null; **R?** is required nullable; **O** is optional. Missing required keys violate the contract. Zero is measured zero; null means not measured, unknown or not applicable with an explicit reason or documented state. Empty lists do not prove completeness. UTC timestamps are ISO 8601 with timezone; examples use Z. Exchange timestamps retain MOEX representation; Europe/Moscow interpretation requires source/board/session evidence and an unverified timezone remains unknown. Acquisition UTC is separate. Positive limits cannot use zero as a sentinel. All fields/enums/signatures in this specification are **PROPOSED**. Freshness CURRENT/STALE/UNKNOWN is an explicitly recorded evidence-backed label; F0 defines no TTL/recency threshold and does not automatically recompute or refresh it after failure.

Safe refs are logical aliases or relative paths; no path traversal, secret-bearing URL, private absolute path or credentials. Optional null_reasons maps may be attached to structures to explain nullable fields when no field-specific state/reason already does so. Error/event null_reasons maps are required. Such maps only describe declared nullable field paths.

Run ID identifies an execution attempt; snapshot ID identifies fixed canonical content at one as_of; dataset ID identifies a logical horizon/interval; artifact ID identifies one representation; target ID is the safe configured destination alias. Changed canonical content needs a new snapshot ID. No consumer allocates replacement run/snapshot IDs. Consumers check IDs, ordered schema and same-basis content evidence before combining results. Data from different snapshots cannot form one release.

| PROPOSED enum | Allowed values |
|---|---|
| Format | csv, xlsx, google_sheets |
| CollectionStatus | NOT_RUN, COMPLETE, VALID_EMPTY, NOT_AVAILABLE_APPROVED_SOURCE, INCOMPLETE, FAILED |
| ValidationStatus | NOT_RUN, PASS, FAIL, NOT_VERIFIED, NEEDS_DECISION |
| ArtifactStatus | NOT_RUN, WRITTEN, VERIFIED, FAILED |
| PreflightStatus | NOT_RUN, PASS, FAIL, NOT_VERIFIED |
| ExportStatus | NOT_RUN, SUCCEEDED, FAILED |
| PublicationStatus | NOT_ATTEMPTED, ACKNOWLEDGED, FAILED, UNKNOWN |
| ReadbackStatus | NOT_ATTEMPTED, VERIFIED, MISMATCH, FAILED, UNKNOWN |
| Generation | CANDIDATE, PREVIOUS, EMPTY, OTHER, UNKNOWN |
| PreviousVerification | VERIFIED, UNVERIFIED, ABSENT, UNKNOWN |
| Preservation | NOT_TOUCHED, PRESERVED_VERIFIED, CHANGED_VERIFIED, UNKNOWN, NOT_APPLICABLE |
| Freshness | CURRENT, STALE, UNKNOWN |
| ReleaseStatus | CANDIDATE, BLOCKED, PUBLISHING, CURRENT_VERIFIED, PARTIAL_REMOTE, REMOTE_UNKNOWN |
| CapabilityVerification | VERIFIED, NOT_VERIFIED, NOT_APPLICABLE |
| CoverageStatus | COMPLETE, INCOMPLETE, NOT_AVAILABLE_APPROVED_SOURCE, NOT_VERIFIED |
| ErrorCategory | CAPACITY, CAPACITY_NOT_VERIFIED, QUOTA_429, TIMEOUT, DISK_FULL, MEMORY_ERROR, ACCESS, QUALITY, SOURCE, CONFIG, IO, REMOTE, UNKNOWN |

<!-- CONTRACT spec -->
### PROPOSED spec

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| contract_version | integer | R | PROPOSED literal 1. |
| dataset_id | string | R | Catalogue logical filename stem. |
| stem | string | R | Equals dataset_id; legacy eight stems stay unchanged. |
| horizon | enum: 10years, 30years, all_time | R | Declared logical horizon; all_data_* is not renamed all time. |
| interval | integer: 24, 60, 10, 1 | R | MOEX interval in catalogue. |
| schema_version | string | R | Explicit schema identifier; incompatible public changes require separate approval/version. |
| columns | ordered array of column | R | Exact preserved public order; source-derived inventory is not verified enriched export schema. |
| key_fields | array of strings | R | Raw sidecar natural key listed below, excluding end and dataframe index. |
| sort_fields | array of strings | R | Raw scope/instrument/interval/begin processing order; never rely on dataframe position. |
| filenames | filenames | R | Exact csv/xlsx catalogue names. |
| required_exports | array of export_spec | R | All declared mandatory formats/destinations; no caller downgrade. |
| empty_policy | literal REQUIRE_NONEMPTY | R | A valid-empty source response does not permit a mandatory empty candle release. |
| notes | redacted string | O | Absent means no extra human explanation; no inferred state. |

<!-- CONTRACT run_context -->
### PROPOSED run_context

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| contract_version | integer | R | PROPOSED literal 1. |
| run_id | string | R | Entrypoint allocation once per execution attempt. |
| snapshot_id | string | R | Canonical handoff allocation, fixed with as_of; only available after handoff. |
| as_of | UTC string | R | One canonical cutoff; preserved for retry/recovery. |
| producer_sha | safe revision string | R | Actual producer revision; examples use labelled ILLUSTRATIVE_SHA. |
| environment | safe alias string | R | Logical profile, not a dump of environment variables. |
| package | string | R | Explicit lane/package. |
| roots | artifact_roots | R | Isolated configured aliases, no real private absolute paths in public manifest. |
| required_targets | array of strings | R | Safe logical configured destinations; no credentials. |
| notes | redacted string | O | Absent means not supplied. |

<!-- CONTRACT metadata -->
### PROPOSED metadata

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| contract_version | integer | R | PROPOSED literal 1; same across context/spec/handoffs, not deployed runtime. |
| run_id | nonempty string | R | Explicit run_context; validator receives the copy in metadata. |
| snapshot_id | nonempty string | R | Explicit immutable canonical snapshot from run_context; no consumer-generated replacement. |
| dataset_id | nonempty string | R | Matches spec and canonical metadata/intents. |
| as_of | UTC ISO 8601 string | R | Common run cutoff; never substituted with acquisition/publication clock time. |
| schema_version | string | R | Matches spec.schema_version. |
| source | literal MOEX_ISS_RAW | R | Approved raw historical OHLC only. |
| source_identities | array of identity | R | Separate raw identity/scope from display ticker/successor. |
| provenance | array of provenance | R | Safe request/source evidence; no secret-bearing URL. |
| requested_bounds | bounds | R | Declared request cutoff/horizon. |
| instrument_bounds | instrument_boundaries | R | Issue/listing/admission/first-last-trade/delisting separate. |
| available_bounds | bounds | R | Approved-source identity/interval/board availability. |
| actual_bounds | bounds | R | Measured accepted canonical output bounds; successful empty has EMPTY endpoints. |
| collection_status | CollectionStatus | R | Missing block/failure never becomes VALID_EMPTY. |
| coverage_status | CoverageStatus | R | Separate completeness evidence; MIN/MAX alone cannot make COMPLETE. |
| counts | counts | R | Measured counts or nullable unknowns, separately from coverage. |
| gaps | array of gap | R | Ordinary non-trading gap differs from quarantined defect. |
| logical_successor | string or null | R? | Null without evidenced mapping; similarity is insufficient. |
| mapping_evidence_ref | safe ref or null | R? | Non-null whenever successor is declared; otherwise null means no mapping evidence. |
| quality_ref | safe ref or null | R? | Null until quality evidence exists. |
| quarantine_ref | safe ref or null | R? | Null if no artifact; checks/states explain unevaluated quarantine. |
| completion_evidence_ref | safe ref or null | R? | Null if last/current bar completion not verified; HTTP200 is not evidence. |
| errors | array of error | R | Same full common error as event.error; empty array is not completeness proof. |

<!-- CONTRACT quality_result -->
### PROPOSED quality_result

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| contract_version | integer | R | PROPOSED literal 1; same across context/spec/handoffs, not deployed runtime. |
| run_id | nonempty string | R | Explicit run_context; validator receives the copy in metadata. |
| snapshot_id | nonempty string | R | Explicit immutable canonical snapshot from run_context; no consumer-generated replacement. |
| dataset_id | nonempty string | R | Matches spec and canonical metadata/intents. |
| as_of | UTC ISO 8601 string | R | Common run cutoff; never substituted with acquisition/publication clock time. |
| status | ValidationStatus | R | PASS after required checks; NOT_VERIFIED for missing evidence; NEEDS_DECISION for unresolved affected calculation. |
| checks | array of check | R | Named checks; empty cannot produce PASS. |
| rows_checked | nonnegative integer or null | R? | Null if validation not run; measured zero differs. |
| accepted_rows | nonnegative integer or null | R? | Null if not measured. |
| quarantined_rows | nonnegative integer or null | R? | Null if not measured. |
| quarantine_ref | safe ref or null | R? | Null without retained artifact; raw conflicts still require later evidence. |
| evidence_ref | safe ref or null | R? | Null without actual validation evidence. |
| errors | array of error | R | Full shared shape, no raw exception objects. |

<!-- CONTRACT artifact_result -->
### PROPOSED artifact_result

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| contract_version | integer | R | PROPOSED literal 1; same across context/spec/handoffs, not deployed runtime. |
| run_id | nonempty string | R | Explicit run_context; validator receives the copy in metadata. |
| snapshot_id | nonempty string | R | Explicit immutable canonical snapshot from run_context; no consumer-generated replacement. |
| dataset_id | nonempty string | R | Matches spec and canonical metadata/intents. |
| as_of | UTC ISO 8601 string | R | Common run cutoff; never substituted with acquisition/publication clock time. |
| artifact_id | string | R | Caller artifact_intent; selected representation only. |
| format | enum: csv, xlsx | R | Exactly one writer format. |
| status | ArtifactStatus | R | Planned ID/NOT_RUN is not a file; WRITTEN is not VERIFIED. |
| expected_shape | shape | R | Canonical input counts/schema width checked before writer. |
| expected_schema_version | string | R | Matches spec. |
| expected_columns | array of strings | R | Matches ordered spec.columns names. |
| relative_path | safe relative path or null | R? | Null before usable candidate exists; must stay within candidate_root. |
| actual_rows | nonnegative integer or null | R? | Null until serialized output measured. |
| bytes | nonnegative integer or null | R? | Null until byte length measured. |
| sha256 | 64 lowercase hex string or null | R? | Actual file bytes; no same-hash requirement between csv and xlsx. |
| canonical_fingerprint | fingerprint or null | R? | Null until canonical content basis identified/measured. |
| verification_evidence_ref | safe ref or null | R? | VERIFIED requires actual roundtrip/schema/value evidence; otherwise null. |
| errors | array of error | R | Preserve preflight/write/verification errors. |

<!-- CONTRACT export_result -->
### PROPOSED export_result

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| contract_version | integer | R | PROPOSED literal 1; same across context/spec/handoffs, not deployed runtime. |
| run_id | nonempty string | R | Explicit run_context; validator receives the copy in metadata. |
| snapshot_id | nonempty string | R | Explicit immutable canonical snapshot from run_context; no consumer-generated replacement. |
| dataset_id | nonempty string | R | Matches spec and canonical metadata/intents. |
| as_of | UTC ISO 8601 string | R | Common run cutoff; never substituted with acquisition/publication clock time. |
| artifact_id | string | R | Exact caller export_intent, no directory enumeration. |
| target_id | string | R | Safe destination alias from export_intent/configuration. |
| format | Format | R | Matches intent and assessment. |
| required | boolean | R | Matches spec.required_exports. |
| preflight_status | PreflightStatus | R | Assessment only; independent from actual preparation. |
| export_status | ExportStatus | R | Caller initializes NOT_RUN; actual export preparation is SUCCEEDED/FAILED. |
| capacity_assessment | capacity_assessment | R | Complete pure result, no invented identity. |
| measured_shape | shape or null | R? | Null before actual export prepared/measured. |
| export_verification_ref | safe ref or null | R? | Null unless actual preparation verification exists. |
| errors | array of error | R | Assessment plus actual preparation errors; publication/readback in per_target_outcome. |

<!-- CONTRACT release_result -->
### PROPOSED release_result

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| contract_version | integer | R | Same PROPOSED version. |
| run_id | string | R | Candidate execution attempt. |
| snapshot_id | string | R | One canonical snapshot across mandatory artifacts. |
| as_of | UTC string | R | Candidate cutoff, never completion timestamp. |
| candidate_release_id | string | R | Coordinator allocation once. |
| status | ReleaseStatus | R | All required-stage gates and candidate readbacks before CURRENT_VERIFIED. |
| stage_outcomes | stage_outcomes | R | Collection/validation/artifact/preflight/export/publication/readback separately. |
| required_export_ids | array of strings | R | Safe unique IDs of mandatory obligations. |
| failed_required_exports | array of strings | R | All unresolved mandatory obligations; cannot be cleared on acknowledgement alone. |
| per_target_outcomes | array of per_target_outcome | R | Honest outcomes for all required targets. |
| candidate_manifest_ref | safe ref or null | R? | Null if not saved. |
| current_manifest_ref | safe ref or null | R? | Null if no identified manifest; do not invent previous. |
| previous_manifest_ref | safe ref or null | R? | Null can be UNKNOWN; does not imply ABSENT. |
| published_as_of | UTC string or null | R? | Prior cutoff remains unchanged on failed candidate; null if absent/unknown. |
| exit_status | integer or null | R? | Null pending; 0 only successful verified current, nonzero terminal blocked/partial/unknown. |
| errors | array of error | R | Retain historical attempt errors even after preservation recovery. |

<!-- CONTRACT per_target_outcome -->
### PROPOSED per_target_outcome

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| target_id | string | R | Matches planned/export capability binding. |
| required | boolean | R | Declared destination obligation. |
| planned_artifact_ids | array of strings | R | Explicit planned representations; existence not implied. |
| publication_status | PublicationStatus | R | Remote mutation acknowledgement separate from preparation/readback. |
| readback_status | ReadbackStatus | R | VERIFIED is content comparison, independent of previous data quality. |
| observed_generation | Generation | R | Identified observed content; UNKNOWN until convincing readback. |
| previous_verification | PreviousVerification | R | Previous DATA QUALITY only; never upgraded by matching baseline bytes. |
| preservation_outcome | Preservation | R | PRESERVED_VERIFIED only matches known baseline; no prior VERIFIED quality prerequisite. |
| previous_release_id | string or null | R? | Null alone is not ABSENT; ABSENT requires evidence. |
| previous_as_of | UTC string or null | R? | No refresh on candidate failure/recovery. |
| previous_freshness | Freshness | R | Preserved independently from content verification. |
| baseline | fingerprint or null | R? | Known pre-mutation content basis; null if not known. |
| previous_evidence_ref | safe ref or null | R? | ABSENT requires evidence; UNVERIFIED may still have content evidence. |
| readback_evidence | readback_evidence or null | R? | VERIFIED/MISMATCH require actual evidence, not acknowledgement. |
| mutation_at | UTC string or null | R? | Null exactly when no mutating attempt. |
| readback_at | UTC string or null | R? | Null if no readback. |
| errors | array of error | R | Preserve TIMEOUT even when current contents are resolved. |

## PROPOSED nested fields and explicit inputs

<!-- CONTRACT column -->
### PROPOSED column

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| name | nonempty string | R | Public column name; order from spec.columns. |
| logical_type | enum: number, string, exchange_timestamp, unknown | R | Declared source-derived logical type; unknown is explicit, not guessed from incomplete samples. |
| nullable | boolean | R | Public missing-value allowance; null-unit status does not change this. |
| unit | string or null | R? | Null when units unknown/not applicable; use unit_status and reason. |
| unit_status | enum: KNOWN, UNKNOWN, NOT_APPLICABLE | R | KNOWN requires non-null unit and supporting evidence. |
| evidence_ref | safe ref or null | R? | Null if not yet validated; source-only inventory not enriched export proof. |
| null_reason | redacted string or null | R? | Non-null explanation when unit/evidence unknown; null if all known. |

<!-- CONTRACT filenames -->
### PROPOSED filenames

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| csv | string | R | stem + .csv. |
| xlsx | string | R | stem + .xlsx; never parts/additional sheets as overflow bypass. |

<!-- CONTRACT export_spec -->
### PROPOSED export_spec

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| export_id | string | R | Unique mandatory obligation ID. |
| target_id | safe logical alias | R | From approved prepared target configuration. |
| format | Format | R | csv/xlsx for every dataset; google_sheets only 10y daily. |
| required | boolean | R | Mandatory obligations true; do not make formats optional. |
| source_dataset_id | string | R | Equals spec.dataset_id. |
| source_artifact | string | R | Selected csv/xlsx filename or canonical Google projection. |
| interface | external_interface | R | Preserved destination projection/layout. |

<!-- CONTRACT external_interface -->
### PROPOSED external_interface

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| columns | ordered array of strings | R | Exact projected schema; no row index. |
| header_rows | nonnegative integer | R | Existing contract 1. |
| index_columns | nonnegative integer | R | Existing candle contract 0. |
| anchor | string or null | R? | A1 for Google; null not applicable for files. |
| nan_token | string or null | R? | Literal NaN for current Google nan='NaN'; file missing semantics remain source-derived/not silently changed. |
| configuration_ref | safe alias or null | R? | Preserved production spreadsheet/worksheet identity ref for Google; null for plain candidate file. |

<!-- CONTRACT identity -->
### PROPOSED identity

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| engine | string | R | Raw source engine. |
| market | string | R | Raw source market. |
| raw_instrument_id | string | R | Raw MOEX SECID/instrument identity. |
| source_scope | enum: BOARD, AGGREGATE | R | Boardless source explicitly AGGREGATE. |
| board | string or null | R? | BOARD requires ID; AGGREGATE requires null and reason. |
| isin | string or null | R? | Null if unknown; no inferred successor. |
| display_ticker | string | R | Visible ticker distinct from raw identity. |

<!-- CONTRACT provenance -->
### PROPOSED provenance

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| endpoint | sanitized endpoint string | R | No userinfo/secret query parameters/fragments. |
| section | string | R | Expected response section/block, e.g. candles. |
| acquired_at | UTC string or null | R? | Null if request never completed/unknown timing. |
| source_scope | string | R | Matches raw scope. |
| metadata_sha256 | 64-hex string or null | R? | Null if not acquired. |
| response_sha256 | 64-hex string or null | R? | Null if no retained response fingerprint. |
| evidence_ref | safe ref or null | R? | Null if no permitted evidence artifact. |

<!-- CONTRACT boundary -->
### PROPOSED boundary

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| value | exchange timestamp/date string or null | R? | Non-null only KNOWN. |
| state | enum: KNOWN, UNKNOWN, OPEN_END, EMPTY, NOT_AVAILABLE_APPROVED_SOURCE | R | OPEN_END only unfinished lifecycle end; EMPTY only successfully empty actual boundaries. |
| precision | enum: DATE, SECOND, UNKNOWN | R | Native source precision; no invented intraday precision. |
| timezone | string or null | R? | Europe/Moscow for exchange seconds; null for DATE/unknown with reason. |
| evidence_ref | safe ref or null | R? | KNOWN requires evidence; null when unknown/unavailable. |
| reason | redacted string or null | R? | Required for non-KNOWN state or absent timezone; otherwise null. |

<!-- CONTRACT bounds -->
### PROPOSED bounds

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| start | boundary | R | Left source/requested/actual endpoint. |
| end | boundary | R | Right endpoint; excluded from natural key. |

<!-- CONTRACT instrument_boundaries -->
### PROPOSED instrument_boundaries

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| issue_date | boundary | R | Distinct lifecycle fact; unknown is not copied from another boundary. |
| listing_date | boundary | R | Distinct lifecycle fact; unknown is not copied from another boundary. |
| admission_date | boundary | R | Distinct lifecycle fact; unknown is not copied from another boundary. |
| first_trade | boundary | R | Distinct lifecycle fact; unknown is not copied from another boundary. |
| last_trade | boundary | R | Distinct lifecycle fact; unknown is not copied from another boundary. |
| delisting_date | boundary | R | Distinct lifecycle fact; unknown is not copied from another boundary. |

<!-- CONTRACT artifact_roots -->
### PROPOSED artifact_roots

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| data | safe configured alias | R | Isolated package-specific data/log/temp/evidence; examples use ILLUSTRATIVE aliases. |
| log | safe configured alias | R | Isolated package-specific data/log/temp/evidence; examples use ILLUSTRATIVE aliases. |
| temp | safe configured alias | R | Isolated package-specific data/log/temp/evidence; examples use ILLUSTRATIVE aliases. |
| evidence | safe configured alias | R | Isolated package-specific data/log/temp/evidence; examples use ILLUSTRATIVE aliases. |

<!-- CONTRACT counts -->
### PROPOSED counts

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| rows_received | nonnegative integer or null | R? | Null if source outcome not measured; 0 at FAILED is not valid-empty. |
| rows_accepted | nonnegative integer or null | R? | Null if quality/canonical selection not measured. |
| rows_quarantined | nonnegative integer or null | R? | Null if not evaluated. |
| pages | nonnegative integer or null | R? | Null if paging evidence unknown. |
| instruments | nonnegative integer or null | R? | Null if universe completeness unknown. |

<!-- CONTRACT gap -->
### PROPOSED gap

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| start | exchange timestamp | R | Observed gap. |
| end | exchange timestamp | R | Observed gap. |
| kind | enum: NON_TRADING, QUARANTINED, UNEXPLAINED | R | Ordinary non-trading gap is not defective candle. |
| evidence_ref | safe ref or null | R? | Null for UNEXPLAINED. |
| reason | redacted string | R | Never silently reset indicators. |

<!-- CONTRACT check -->
### PROPOSED check

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| check_id | string | R | Stable required invariant. |
| status | enum: PASS, FAIL, NOT_VERIFIED | R | NOT_VERIFIED prevents aggregate PASS. |
| evidence_ref | safe ref or null | R? | Null if not verified. |
| reason | redacted string or null | R? | Non-null for FAIL/NOT_VERIFIED, otherwise null. |

<!-- CONTRACT shape -->
### PROPOSED shape

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| data_rows | nonnegative integer or null | R? | Null if unmeasured; reason required and no preflight PASS. |
| data_columns | positive integer or null | R? | Null if schema width unknown; reason required and no PASS. |
| header_rows | nonnegative integer | R | Explicit 1 for existing exports. |
| index_columns | nonnegative integer | R | Explicit 0 for candle datasets; row index is not data/key. |

<!-- CONTRACT target_capabilities -->
### PROPOSED target_capabilities

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| capabilities_ref | safe ref | R | Caller-owned exact capability record identity. |
| target_id | safe logical alias | R | Configured destination; not an API-generated replacement. |
| format | Format | R | One selected format assessed. |
| verification | CapabilityVerification | R | VERIFIED requires applicable numeric/evidence confirmation; NOT_APPLICABLE for absent CSV format ceiling. |
| row_limit | positive integer or null | R? | Null non-applicable/unknown, never zero sentinel. |
| column_limit | positive integer or null | R? | Same semantics. |
| cell_limit | positive integer or null | R? | Effective confirmed target limit; documented 20m is not substituted. |
| grid | array of sheet_grid or null | R? | Complete retained Google workbook allocation; null absent/unverified/non-applicable. |
| limit_evidence_ref | safe ref or null | R? | Null means applicable numeric target limit unverified. |
| grid_evidence_ref | safe ref or null | R? | Null means whole workbook grid unverified. |
| verified_at | UTC string or null | R? | Null if effective applicable limits not verified. |
| verification_scope | enum: DOCUMENTED_FORMAT, CONFIRMED_TARGET, INJECTED, UNVERIFIED, NOT_APPLICABLE | R | INJECTED only illustrative offline capability; target application checked by D5. |

<!-- CONTRACT sheet_grid -->
### PROPOSED sheet_grid

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| is_target | boolean | R | Exactly one selected target; actual worksheet ID retained in configuration. |
| rows | positive integer | R | Allocated rows including blank/header. |
| columns | positive integer | R | Allocated columns including blank cells. |

<!-- CONTRACT capacity_assessment -->
### PROPOSED capacity_assessment

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| format | Format | R | Selected capability format. |
| status | enum: PASS, FAIL, NOT_VERIFIED | R | Actual pure returned assessment only; caller/stage preflight may be NOT_RUN before this function runs. |
| capabilities_ref | safe ref | R | Exact input capabilities binding, no run identity. |
| source_shape | shape | R | Exact assessed shape, writer checks it. |
| required_rows | nonnegative integer or null | R? | rows + header; null if rows unmeasured. |
| required_columns | positive integer or null | R? | columns + index; null if width unknown. |
| required_cells | nonnegative integer or null | R? | Product; null if either dimension unknown. |
| row_limit | positive integer or null | R? | Copied applicable limit. |
| column_limit | positive integer or null | R? | Copied applicable limit. |
| cell_limit | positive integer or null | R? | Copied effective target limit. |
| workbook_cells_before | nonnegative integer or null | R? | All retained Sheets grid cells; null non-applicable/unknown. |
| workbook_cells_after | nonnegative integer or null | R? | Sum with selected grid expanded and other grids retained; null unknown/non-applicable. |
| limit_evidence_ref | safe ref or null | R? | Copied available limit evidence. |
| grid_evidence_ref | safe ref or null | R? | Copied available grid evidence. |
| errors | array of error | R | Full common error shape; pure preflight correlation IDs nullable with reasons. |

<!-- CONTRACT artifact_intent -->
### PROPOSED artifact_intent

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| dataset_id | string | R | Equals spec.dataset_id. |
| snapshot_id | string | R | Equals canonical run_context snapshot. |
| artifact_id | string | R | Caller allocation once per selected representation. |
| format | enum: csv, xlsx | R | One chosen writer format. |
| candidate_relative_path | safe relative path | R | Within explicit candidate_root, filename matches spec.filenames[format]. |

<!-- CONTRACT export_intent -->
### PROPOSED export_intent

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| dataset_id | string | R | Spec/artifact agreement. |
| snapshot_id | string | R | Canonical snapshot agreement. |
| artifact_id | string | R | Selected artifact or projection. |
| target_id | safe alias | R | Matches spec/capabilities logical destination. |
| format | Format | R | Matches selected assessment. |
| required | boolean | R | Matches declared spec export. |
| capabilities_ref | safe ref | R | Exact caller-owned capability association. |

<!-- CONTRACT fingerprint -->
### PROPOSED fingerprint

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| basis | enum: FILE_BYTES_SHA256, LOGICAL_ROWS_SHA256 | R | Compare same basis only; CSV/XLSX byte hashes may differ. |
| value | 64 lowercase hex string | R | Measured digest; example digests explicitly invented. |
| schema_version | string | R | Identifies normalized schema basis for logical content. |

<!-- CONTRACT readback_evidence -->
### PROPOSED readback_evidence

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| expected | fingerprint | R | Known candidate/previous baseline. |
| observed | fingerprint | R | Measured same-basis target content. |
| evidence_ref | safe ref | R | Actual redacted comparison evidence. |
| scope | string | R | Full required target content, not a few sample rows. |

<!-- CONTRACT stage_outcomes -->
### PROPOSED stage_outcomes

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| collection | CollectionStatus | R | Aggregate collection. |
| validation | ValidationStatus | R | Aggregate quality. |
| artifact | ArtifactStatus | R | All mandatory artifact verification. |
| preflight | PreflightStatus | R | All required targets before current mutations. |
| export | ExportStatus | R | Actual preparation. |
| publication | PublicationStatus | R | Acknowledgements separate from content. |
| readback | ReadbackStatus | R | All required candidate/current readbacks. |

<!-- CONTRACT error -->
### PROPOSED error

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| category | ErrorCategory | R | Distinct failure taxonomy. |
| code | string | R | Stable redacted error code. |
| stage | string | R | Failed stage; pure preflight stage is preflight. |
| retryable | boolean | R | Bounded retry policy; overflow not fixed by repeating same size. |
| final | boolean | R | Attempt chain ended; recovery does not remove it. |
| message | redacted string | R | No direct raw exception interpolation. |
| run_id | safe string or null | R? | Null when not known/applicable; supply null_reasons entry. Endpoint strips userinfo/secret queries; files are safe relative refs. |
| snapshot_id | safe string or null | R? | Null when not known/applicable; supply null_reasons entry. Endpoint strips userinfo/secret queries; files are safe relative refs. |
| dataset_id | safe string or null | R? | Null when not known/applicable; supply null_reasons entry. Endpoint strips userinfo/secret queries; files are safe relative refs. |
| instrument | safe string or null | R? | Null when not known/applicable; supply null_reasons entry. Endpoint strips userinfo/secret queries; files are safe relative refs. |
| interval | integer or null | R? | Null when not known/applicable; supply null_reasons entry. Endpoint strips userinfo/secret queries; files are safe relative refs. |
| page | integer/string or null | R? | Null when not known/applicable; supply null_reasons entry. Endpoint strips userinfo/secret queries; files are safe relative refs. |
| file | safe string or null | R? | Null when not known/applicable; supply null_reasons entry. Endpoint strips userinfo/secret queries; files are safe relative refs. |
| artifact_id | safe string or null | R? | Null when not known/applicable; supply null_reasons entry. Endpoint strips userinfo/secret queries; files are safe relative refs. |
| target_id | safe string or null | R? | Null when not known/applicable; supply null_reasons entry. Endpoint strips userinfo/secret queries; files are safe relative refs. |
| endpoint | safe string or null | R? | Null when not known/applicable; supply null_reasons entry. Endpoint strips userinfo/secret queries; files are safe relative refs. |
| attempt | integer or null | R? | Null when not known/applicable; supply null_reasons entry. Endpoint strips userinfo/secret queries; files are safe relative refs. |
| exception_type | safe string or null | R? | Null when not known/applicable; supply null_reasons entry. Endpoint strips userinfo/secret queries; files are safe relative refs. |
| null_reasons | map of nullable-field name to redacted reason | R | Every null field has reason; never put credentials in explanation. |

<!-- CONTRACT event -->
### PROPOSED event

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| timestamp_utc | UTC string | R | Emission time distinct from data cutoff. |
| level | enum: DEBUG, INFO, WARNING, ERROR | R | Capacity blockers ERROR. |
| event | string | R | Stable event name. |
| stage | string | R | Startup/stage identifier; no artifact requirement. |
| message | redacted string | R | Readable original error message, not dependent on output artifacts. |
| run_id | safe string or null | R? | Startup/general unknown or inapplicable context remains null with null_reasons; no globals/fabrication. |
| producer_sha | safe string or null | R? | Startup/general unknown or inapplicable context remains null with null_reasons; no globals/fabrication. |
| process | safe string or null | R? | Startup/general unknown or inapplicable context remains null with null_reasons; no globals/fabrication. |
| worker | safe string or null | R? | Startup/general unknown or inapplicable context remains null with null_reasons; no globals/fabrication. |
| snapshot_id | safe string or null | R? | Startup/general unknown or inapplicable context remains null with null_reasons; no globals/fabrication. |
| release_id | safe string or null | R? | Startup/general unknown or inapplicable context remains null with null_reasons; no globals/fabrication. |
| dataset_id | safe string or null | R? | Startup/general unknown or inapplicable context remains null with null_reasons; no globals/fabrication. |
| instrument | safe string or null | R? | Startup/general unknown or inapplicable context remains null with null_reasons; no globals/fabrication. |
| interval | integer or null | R? | Startup/general unknown or inapplicable context remains null with null_reasons; no globals/fabrication. |
| page | integer/string or null | R? | Startup/general unknown or inapplicable context remains null with null_reasons; no globals/fabrication. |
| file | safe string or null | R? | Startup/general unknown or inapplicable context remains null with null_reasons; no globals/fabrication. |
| artifact_id | safe string or null | R? | Startup/general unknown or inapplicable context remains null with null_reasons; no globals/fabrication. |
| target_id | safe string or null | R? | Startup/general unknown or inapplicable context remains null with null_reasons; no globals/fabrication. |
| outcome | safe string or null | R? | Startup/general unknown or inapplicable context remains null with null_reasons; no globals/fabrication. |
| elapsed_ms | nonnegative number or null | R? | Null before measured elapsed time. |
| duration_ms | nonnegative number or null | R? | Null before measured stage/action duration. |
| counts | counts or null | R? | Null if not measured/applicable; rows_received zero at failure is not completeness. |
| retries | retries or null | R? | Null without retry context. |
| error | error or null | R? | Exactly same full error record as manifest errors arrays; null when no error. |
| fields | event_fields | R | Typed additions; empty for ordinary/startup event. |
| null_reasons | map of field name to redacted reason | R | Every null event envelope field has reason. |

<!-- CONTRACT retries -->
### PROPOSED retries

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| attempt | nonnegative integer | R | Actual attempt. |
| limit | positive integer | R | Configured finite bound. |

<!-- CONTRACT event_fields -->
### PROPOSED event_fields

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| capacity | capacity_event_fields | O | Required when capacity event; extends rather than replaces generic envelope. |

<!-- CONTRACT capacity_event_fields -->
### PROPOSED capacity_event_fields

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| assessment | capacity_assessment | R | Full assessed geometry/limits/evidence. |
| horizon | enum: 10years, 30years, all_time | R | Logical horizon. |
| candidate_snapshot_id | string | R | Candidate context. |
| saved_candidate_artifact_ids | array of strings | R | Only actual saved artifacts. |
| saved_candidate_rows | nonnegative integer or null | R? | Null if no saved count measured. |
| previous_release_id | string or null | R? | Null is not absence without explicit evidence. |
| previous_as_of | UTC string or null | R? | Previous cutoff unchanged. |
| previous_verification | PreviousVerification | R | Previous data-quality state independent from readback. |
| previous_freshness | Freshness | R | Do not refresh previous. |
| preservation_outcome | Preservation | R | Content-only evidence or NOT_TOUCHED/UNKNOWN. |
| preservation_evidence_ref | safe ref or null | R? | Null absent content readback. |
| export_status | ExportStatus | R | Actual preparation independent of preflight. |
| publication_status | PublicationStatus | R | Acknowledgement independent. |
| readback_status | ReadbackStatus | R | Independent comparison. |
| observed_generation | Generation | R | Actual content disposition. |
| failed_required_exports | array of strings | R | Unresolved mandatory obligations. |
| release_status | ReleaseStatus | R | Coordinator outcome. |
| exit_status | integer or null | R? | Null pending, final required blocker nonzero. |

<!-- CONTRACT null_reasons -->
### PROPOSED null_reasons

| Field | Type / allowed values | Required | Meaning and null / absence semantics |
|---|---|---|---|
| field path | redacted string | O | Required conditionally for a nullable field not already justified by its documented state/reason. No executable or secret-bearing text. |

## Preserved filename catalogue and data dictionary

| Interval | Existing 10y stem | Existing 30y stem | PROPOSED all-time stem |
|---|---|---|---|
| 24 | 10years_data_1d_interval | 30years_data_1d_interval | all_time_data_1d_interval |
| 60 | 10years_data_1h_interval | 30years_data_1h_interval | all_time_data_1h_interval |
| 10 | 10years_data_10m_interval | 30years_data_10m_interval | all_time_data_10m_interval |
| 1 | 10years_data_1m_interval | 30years_data_1m_interval | all_time_data_1m_interval |

Each stem has mandatory .csv and .xlsx files. All-time names are additions, not renaming all.py or all_data_*. The configuration evidence is settings/datasets_config.json:3–47. No dataset/config changes are implemented here.

<!-- RAW_COLUMNS -->
| Position | Raw public column | Source-derived logical type | Meaning / unit evidence | Missing / NaN meaning and evidence |
|---|---|---|---|---|
| 1 | open | number (source-derived) | Raw opening price; source instrument/currency denomination, not universally RUB. | Source numeric missing/NaN is distinct from not-measured metadata; raw missing meaning/serialization UNKNOWN pending ISS/export evidence, never zero-filled. |
| 2 | close | number (source-derived) | Raw closing price; same source denomination. | Source numeric missing/NaN is distinct from not-measured metadata; raw missing meaning/serialization UNKNOWN pending ISS/export evidence, never zero-filled. |
| 3 | high | number (source-derived) | Raw highest price; same source denomination. | Source numeric missing/NaN is distinct from not-measured metadata; raw missing meaning/serialization UNKNOWN pending ISS/export evidence, never zero-filled. |
| 4 | low | number (source-derived) | Raw lowest price; same source denomination. | Source numeric missing/NaN is distinct from not-measured metadata; raw missing meaning/serialization UNKNOWN pending ISS/export evidence, never zero-filled. |
| 5 | value | number (source-derived) | Source traded value; exact currency/scale require instrument/source evidence. | Source numeric missing/NaN is distinct from not-measured metadata; raw missing meaning/serialization UNKNOWN pending ISS/export evidence, never zero-filled. |
| 6 | volume | number (source-derived) | Source traded quantity; exact unit/scale require instrument/source evidence. | Source numeric missing/NaN is distinct from not-measured metadata; raw missing meaning/serialization UNKNOWN pending ISS/export evidence, never zero-filled. |
| 7 | begin | exchange_timestamp (source-derived) | Source bar start; source string precision/timezone retained. | Missing required identity/time is not guessed; source-cell nullability/export representation UNKNOWN pending schema evidence. |
| 8 | end | exchange_timestamp (source-derived) | Source bar end; completion evidence separate; excluded from natural key. | Missing required identity/time is not guessed; source-cell nullability/export representation UNKNOWN pending schema evidence. |
| 9 | ticker | string (source-derived) | Visible source ticker; raw identity/mapping remain sidecar facts. | Missing required identity/time is not guessed; source-cell nullability/export representation UNKNOWN pending schema evidence. |

Checked-in all_data_* candle headers support this nine-column prefix. They do not verify enriched exports, dtype precision, indicator units or every missing-value rule. Public file/schema order remains unchanged until an explicitly approved schema change. Final units, all column types and exported contents remain issue #63 integration work.

<!-- INDICATOR_INVENTORY count=79 -->
### Source-derived indicator inventory — 79 declared calculation-backed outputs

This list preserves source append order and names. Parameters are the actual calls in tech.py, not validated recommendations. Logical intent is a numeric series; actual serialized dtype, unit, NaN/warm-up semantics and formula/output correctness are **NOT VERIFIED** here. Library-default parameters are recorded as defaults, not silently assigned numbers. RSI14 means fourteen bars, not days; F0 does not change the existing formula.

| # | Output name | TA-Lib source call / parameters | Call line / append-name line | Verification |
|---|---|---|---|---|
| 1 | BBANDS_upperband | BBANDS: close; timeperiod=5, nbdevup=2, nbdevdn=2, matype=0 | tech.py:284 / 556 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 2 | BBANDS_middleband | BBANDS: close; timeperiod=5, nbdevup=2, nbdevdn=2, matype=0 | tech.py:284 / 559 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 3 | BBANDS_lowerband | BBANDS: close; timeperiod=5, nbdevup=2, nbdevdn=2, matype=0 | tech.py:284 / 562 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 4 | DEMA | DEMA: close; timeperiod=30 | tech.py:290 / 565 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 5 | EMA | EMA: close; timeperiod=30 | tech.py:293 / 568 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 6 | HT_TRENDLINE | HT_TRENDLINE: close; library defaults | tech.py:296 / 571 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 7 | KAMA | KAMA: close; timeperiod=30 | tech.py:299 / 574 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 8 | MA | MA: close; timeperiod=30, matype=0 | tech.py:302 / 577 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 9 | MIDPOINT | MIDPOINT: close; timeperiod=14 | tech.py:314 / 592 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 10 | MIDPRICE | MIDPRICE: high, low; timeperiod=14 | tech.py:317 / 595 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 11 | SAR | SAR: high, low; acceleration=0, maximum=0 | tech.py:320 / 598 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 12 | SAREXT | SAREXT: high, low; startvalue=0, offsetonreverse=0, all acceleration/max parameters=0 | tech.py:323 / 601 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 13 | T3 | T3: high; timeperiod=5, vfactor=0 | tech.py:326 / 607 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 14 | TEMA | TEMA: close; timeperiod=30 | tech.py:329 / 610 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 15 | TRIMA | TRIMA: close; timeperiod=30 | tech.py:332 / 613 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 16 | WMA | WMA: close; timeperiod=30 | tech.py:335 / 616 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 17 | ADX | ADX: high, low, close; timeperiod=14 | tech.py:341 / 622 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 18 | ADXR | ADXR: high, low, close; timeperiod=14 | tech.py:344 / 625 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 19 | APO | APO: close; fastperiod=12, slowperiod=26, matype=0 | tech.py:347 / 628 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 20 | AROON_down | AROON: high, low; timeperiod=14 | tech.py:350 / 631 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 21 | AROON_up | AROON: high, low; timeperiod=14 | tech.py:350 / 634 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 22 | AROONOSC | AROONOSC: high, low; timeperiod=14 | tech.py:355 / 637 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 23 | BOP | BOP: open, high, low, close; library defaults | tech.py:358 / 640 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 24 | CCI | CCI: high, low, close; timeperiod=14 | tech.py:361 / 643 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 25 | CMO | CMO: close; timeperiod=14 | tech.py:364 / 646 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 26 | DX | DX: high, low, close; timeperiod=14 | tech.py:367 / 649 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 27 | MACD_real | MACD: close; fastperiod=12, slowperiod=26, signalperiod=9 | tech.py:370 / 652 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 28 | MACD_signal | MACD: close; fastperiod=12, slowperiod=26, signalperiod=9 | tech.py:370 / 655 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 29 | MACD_hist | MACD: close; fastperiod=12, slowperiod=26, signalperiod=9 | tech.py:370 / 658 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 30 | MACDEXT_real | MACDEXT: close; fastperiod=12, fastmatype=0, slowperiod=26, slowmatype=0, signalperiod=9, signalmatype=0 | tech.py:376 / 661 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 31 | MACDEXT_signal | MACDEXT: close; fastperiod=12, fastmatype=0, slowperiod=26, slowmatype=0, signalperiod=9, signalmatype=0 | tech.py:376 / 664 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 32 | MACDEXT_hist | MACDEXT: close; fastperiod=12, fastmatype=0, slowperiod=26, slowmatype=0, signalperiod=9, signalmatype=0 | tech.py:376 / 667 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 33 | MACDFIX_real | MACDFIX: close; signalperiod=9 | tech.py:382 / 670 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 34 | MACDFIX_signal | MACDFIX: close; signalperiod=9 | tech.py:382 / 673 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 35 | MACDFIX_hist | MACDFIX: close; signalperiod=9 | tech.py:382 / 676 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 36 | MFI | MFI: high, low, close, volume; timeperiod=14 | tech.py:388 / 679 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 37 | MINUS_DI | MINUS_DI: high, low, close; timeperiod=14 | tech.py:391 / 682 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 38 | MINUS_DM | MINUS_DM: high, low; timeperiod=14 | tech.py:394 / 685 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 39 | MOM | MOM: close; timeperiod=100 | tech.py:397 / 688 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 40 | PLUS_DI | PLUS_DI: high, low, close; timeperiod=14 | tech.py:400 / 691 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 41 | PLUS_DM | PLUS_DM: high, low; timeperiod=14 | tech.py:403 / 694 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 42 | PPO | PPO: close; fastperiod=12, slowperiod=26, matype=0 | tech.py:406 / 697 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 43 | ROC | ROC: close; timeperiod=10 | tech.py:409 / 700 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 44 | ROCP | ROCP: close; timeperiod=10 | tech.py:412 / 703 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 45 | ROCR | ROCR: close; timeperiod=10 | tech.py:415 / 706 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 46 | ROCR100 | ROCR100: close; timeperiod=10 | tech.py:418 / 709 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 47 | RSI14 | RSI: close; timeperiod=14 | tech.py:421 / 712 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 48 | STOCH_slowk | STOCH: high, low, close; fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0 | tech.py:424 / 715 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 49 | STOCH_slowd | STOCH: high, low, close; fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0 | tech.py:424 / 718 | SOURCE ONLY; slowk appended to slowd at427 |
| 50 | STOCHF_fastk | STOCHF: high, low, close; fastk_period=5, fastd_period=3, fastd_matype=0 | tech.py:429 / 721 | SOURCE ONLY; self-accumulator at431 |
| 51 | STOCHF_fastd | STOCHF: high, low, close; fastk_period=5, fastd_period=3, fastd_matype=0 | tech.py:429 / 724 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 52 | STOCHRSI_fastk | STOCHRSI: close; timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0 | tech.py:434 / 727 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 53 | STOCHRSI_fastd | STOCHRSI: close; timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0 | tech.py:434 / 730 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 54 | ULTOSC | ULTOSC: high, low, close; timeperiod1=7, timeperiod2=14, timeperiod3=28 | tech.py:442 / 736 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 55 | WILLR | WILLR: high, low, close; timeperiod=14 | tech.py:445 / 739 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 56 | ATR | ATR: high, low, close; timeperiod=14 | tech.py:451 / 745 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 57 | NATR | NATR: high, low, close; timeperiod=14 | tech.py:454 / 748 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 58 | TRANGE | TRANGE: high, low, close; library defaults | tech.py:457 / 751 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 59 | AD | AD: high, low, close, volume; library defaults | tech.py:464 / 757 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 60 | ADOSC | ADOSC: high, low, close, volume; fastperiod=3, slowperiod=10 | tech.py:467 / 760 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 61 | OBV | OBV: close, volume; library defaults | tech.py:470 / 763 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 62 | AVGPRICE | AVGPRICE: open, high, low, close; library defaults | tech.py:476 / 769 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 63 | MEDPRICE | MEDPRICE: high, low; library defaults | tech.py:479 / 772 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 64 | TYPPRICE | TYPPRICE: high, low, close; library defaults | tech.py:482 / 775 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 65 | WCLPRICE | WCLPRICE: high, low, close; library defaults | tech.py:485 / 778 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 66 | HT_DCPERIOD | HT_DCPERIOD: close; library defaults | tech.py:489 / 784 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 67 | HT_DCPHASE | HT_DCPHASE: close; library defaults | tech.py:492 / 787 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 68 | HT_PHASOR_inphase | HT_PHASOR: close; library defaults | tech.py:495 / 790 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 69 | HT_PHASOR_quadrature | HT_PHASOR: close; library defaults | tech.py:495 / 793 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 70 | HT_SINE_sine | HT_SINE: close; library defaults | tech.py:500 / 797 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 71 | HT_SINE_leadsine | HT_SINE: close; library defaults | tech.py:500 / 801 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 72 | HT_TRENDMODE | HT_TRENDMODE: close; library defaults | tech.py:505 / 804 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 73 | LINEARREG | LINEARREG: close; timeperiod=14 | tech.py:522 / 816 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 74 | LINEARREG_ANGLE | LINEARREG_ANGLE: close; timeperiod=14 | tech.py:525 / 819 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 75 | LINEARREG_INTERCEPT | LINEARREG_INTERCEPT: close; timeperiod=14 | tech.py:528 / 822 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 76 | LINEARREG_SLOPE | LINEARREG_SLOPE: close; timeperiod=14 | tech.py:531 / 825 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 77 | STDDEV | STDDEV: close; timeperiod=5, nbdev=1 | tech.py:534 / 828 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 78 | TSF | TSF: close; timeperiod=14 | tech.py:537 / 831 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |
| 79 | VAR | VAR: close; timeperiod=5, nbdev=1 | tech.py:540 / 834 | SOURCE ONLY; unit/NaN/export NOT VERIFIED |

The full source also has seven legacy empty/defectively wired headings: MAVP (call commented at311, joins duplicated at586/589), SMA (604, no active call), TRIX (439 stores into unused _full; append733), BETA (commented514, append810), CORREL (commented518, append813), HighsLows (commented547, append840), BullBearPower (commented551, append843). Thus 86 unique active rename/join labels exist, but the 79-row calculation inventory is **not** proof of a successful 79-column enriched export or permission to remove those seven names. Reconcile the final public schema/units with actual verified outputs and approved fixes before adoption. The paired tech.py/tech.ipynb nonblank code was inspected consistently; F0 performs no synchronization.

### Preserved Google view

Source remains 10years_data_1d_interval.csv. Ordered ten columns are open, close, high, low, value, volume, begin, end, ticker, RSI14. Existing filtering preserves input order, so the source/candidate schema must preserve the expected order. Keep A1, header, copy_index=False, copy_head=True and nan='NaN' (literal text NaN). fit=False and escape_formulae=False are OBSERVED in upload.py:75, not mandatory unsafe settings for a future adapter. Target adapters fix escaping/write conditions under #34/#35/#36 while preserving visible literal text, numeric values, NaN semantics and retained-grid accounting. Do not silently turn NaN into blank/zero or change visible column order. Keep the existing production spreadsheet and worksheet identity from configured evidence. Positional google_doc[0] is not a verified numeric sheet ID; D5 resolves/verifies the actual identity/grid without changing the target. All-time datasets never replace this view.

## PROPOSED procedural interfaces and field origins

```text
validate_dataset(frame, spec, metadata) -> quality_result
export_preflight(spec, shape, target_capabilities) -> capacity_assessment
write_dataset(frame, spec, artifact_intent, capacity_assessment,
              candidate_root, run_context) -> artifact_result
emit_event(context, level, event, message, fields)
```

Validation reads the explicit canonical metadata and returns quality facts; it neither recollects nor repairs raw data. The pure preflight evaluates exactly one capabilities.format, uses no globals, performs no files/network/writers/events and allocates no run/snapshot/artifact/target IDs. Its source_shape contains dimensions only. Capability reference and logical target are explicit inputs; assessment retains the capability reference, not hidden run context.

Caller explicitly assembles export_result from run_context + export_intent + capacity_assessment + actual export-preparation observations. Publication/readback then belong to per_target_outcome. This is a responsibility of procedural callers, not an extra implemented framework/helper. Validate target/capability binding, dataset/schema, snapshot and format before action. Contextualized errors remain the identical full error type.

One writer call selects one csv OR xlsx and returns one artifact_result. Two formats require two intents/calls with distinct artifact IDs/paths and a common snapshot. Writer checks actual frame shape/schema against spec/assessment/intent, resolves the filename under explicit candidate_root and rejects escape/current paths. It only writes/verifies the selected candidate; no collection, indicator recalculation, current switch or Google adapter. A FAILED mandatory format blocks overall release even if another candidate exists.

| Result field group | Exact value source |
|---|---|
| contract_version, run_id, snapshot_id, as_of | Explicit run_context; validation receives its copy in metadata |
| dataset_id, expected ordered schema | spec, checked against metadata/intents |
| artifact_id, selected format | artifact_intent/export_intent; format checked against assessment |
| target_id, required | export_intent checked against spec.required_exports and target_capabilities |
| preflight_status, geometry, limits, evidence | capacity_assessment, independently of actual actions |
| candidate path | artifact_intent + explicit candidate_root, checked for containment |
| actual shape/bytes/hash/verification | Actual writer/preparation observations; unmeasured stays null |
| quality_result status/checks/counts/refs | Actual validation observations over explicit frame/spec/metadata; unexecuted counts/refs remain null |
| artifact_result status/expected_shape/schema/columns | Writer observations plus explicit spec/frame/assessment and chosen artifact_intent |
| release candidate_release_id/stage_outcomes/required_export_ids/per-target obligations | Explicit coordinator allocation, declared spec obligations and actual stage/per-target observations |
| previous quality/ID/as_of/freshness/baseline | Explicitly captured pre-attempt metadata/content evidence; matching readback never recomputes quality/freshness |
| export_status | Caller initially NOT_RUN; actual preparation SUCCEEDED/FAILED |
| publication/readback/generation/preservation | Actual per-target mutation/measurement evidence, never capacity PASS |
| errors | Pure assessment plus actual errors, retained after recovery |
| overall status, failed required exports, exit_status | Coordinator aggregate across all mandatory obligations |

File-byte hashes certify that file content identity; logical fingerprints compare a defined schema/ordered canonical-value representation. Never compare CSV bytes to XLSX bytes. A LOGICAL_ROWS_SHA256 implementation/evidence must declare normalization of schema/order/types/missing values and compare canonical values after format roundtrip; absent such evidence remains NOT_VERIFIED. This docs-only proposal does not assert that helper or normalization exists.

## Capacity and release gate

Predictable required-export overflow is checked before modifying **any** current target. No truncation, shorter horizon, XLSX parts/additional sheets/Power Pivot, optional-format downgrade or mixed-generation release. Save candidate separately where feasible. Required NOT_VERIFIED capability blocks transition; it is neither PASS nor proven overflow. Keep previous date/cutoff unchanged, identify absent/unverified/unknown previous honestly, and finish terminal required failure nonzero. No Google+hosting distributed transaction is promised.

Required rows = data_rows + header_rows; required columns = data_columns + index_columns. For Google, whole-workbook cells before = sum(rows*columns) for every retained sheet. After = same sum, replacing only target dimensions with max(existing_rows,required_rows) and max(existing_columns,required_columns). Blank allocation and other worksheets count. No automatic shrink or target-identity replacement is authorized.

[Excel limits](https://support.microsoft.com/en-us/excel/excel-specifications-and-limits) are 1,048,576 rows and 16,384 columns; a header permits 1,048,575 data rows. [Google Drive limits](https://support.google.com/drive/answer/37603?hl=en) document up to 20 million cells or 100 MB, without verifying this production target. Effective target capacity, grid and applicability remain external D5 evidence; do not substitute the legacy 10m comment or silently assume 20m. The cell oracle does not certify other applicable byte/resource constraints.

<!-- BOUNDARY_ORACLE -->
| Independent oracle | Input | Expected capacity outcome | Overall-release implication |
|---|---|---|---|
| XLSX exact row boundary | 1,048,575 data +1 header, 10 columns, 0 index | PASS: 1,048,576 rows | Only this format boundary passes |
| XLSX boundary+1 | 1,048,576 data +1 header | FAIL, CAPACITY | Required blocker, no current mutation |
| XLSX exact column boundary / +1 | 16,384 / 16,385 total columns | PASS / FAIL | Independent width oracle |
| Sheets injected exact whole-grid limit | L=101; target10x10 plus other1x1, 9 data+header | PASS:101 allocated cells | Only known-capacity oracle passes |
| Sheets injected whole-grid limit+1 | L=101; target10x10 plus other1x2 | FAIL:102 allocated cells | Other sheet cannot be ignored |
| Sheets target expansion | L=100; target10x10;10 data+header | FAIL:11x10=110 | Header/allocated width count |
| Sheets missing limit/grid/evidence | Any required fact unavailable | NOT_VERIFIED | Blocked; no claimed overflow/PASS |
| All required exports pass and verify candidate | Complete stages plus full readbacks at same snapshot | Gate may pass | This separate aggregate is needed for CURRENT_VERIFIED |

### Independent previous quality and content preservation — P04

previous_verification is previous **data quality**: VERIFIED, UNVERIFIED, ABSENT or UNKNOWN. Readback VERIFIED is a trustworthy content comparison. PRESERVED_VERIFIED requires a matching known previous baseline, irrespective of whether prior data quality was VERIFIED. It is incorrect to withhold preservation evidence merely because prior quality was unverified; it is equally incorrect to upgrade prior quality by matching content. NOT_TOUCHED means no mutating call in this run and is invalid after any mutating attempt. CHANGED_VERIFIED means measured difference from baseline, not automatically valid new data. UNKNOWN applies only while actual content remains unidentified; convincing later readback resolves that content uncertainty without erasing the attempt error.

Known baseline + previous UNVERIFIED + mutating TIMEOUT + matching readback yields VERIFIED readback, PREVIOUS generation and PRESERVED_VERIFIED content. Prior quality/ID/date/as_of/freshness stay unchanged; timeout remains in errors. Candidate is not current and required failure remains nonzero. One target's preservation says nothing about another target. PARTIAL_REMOTE records conclusively different required-target generations; REMOTE_UNKNOWN records unresolved target content after possible mutation. Historical timeout alone cannot force REMOTE_UNKNOWN after convincing readback.

## Observability, privacy and open indicator decision

Event envelope includes startup/general nullable correlation, producer/process/worker, timing, stage, counts/retries and identical full errors. A startup failure needs no snapshot/artifact/output root/JSONL/diagnostic bundle; safe console reporting of the original failure cannot depend on successful artifact creation. capacity_event_fields add geometry/limit evidence, saved candidate, previous quality/freshness/content evidence and separate export/publication/readback/summary outcomes. ERROR appears in console, JSONL and summary once instrumentation exists; final required capacity refusal is nonzero, while pending exit_status is null.

For a capacity export event, applicable allocated run_id, snapshot_id, dataset_id, artifact_id, target_id and interval in the general envelope must be non-null and agree with intent/assessment/configuration. They remain nullable for ordinary/startup events. The selected format/capability reference is in assessment; pending exit status remains null until the coordinator finalizes the run.

| Failure category | Meaning and handling |
|---|---|
| CAPACITY | Proven applicable format/grid limit exceeded; no same-volume blind retry/truncation |
| CAPACITY_NOT_VERIFIED | Missing applicable capability evidence; blocked, not proven overflow |
| QUOTA_429 | Rate/quota response; finite retry/backoff, no paid quota/billing |
| TIMEOUT | Finite retry where safe; mutating acknowledgement/content may be unknown until readback |
| DISK_FULL | Write/resource failure; no candidate/current success claim |
| MEMORY_ERROR | Allocation failure; no reduced dataset disguised as complete |
| ACCESS | Authorization/authentication/filesystem access failure; redact, avoid endless retries |
| SOURCE, QUALITY, CONFIG, IO, REMOTE, UNKNOWN | Preserve exact failure distinction and evidence |

Manifest errors and event.error are **the same full error shape**; the short illustrative error in the approved plan is expanded here. Redaction covers messages, URL userinfo/secret query parameters, all nested fields/lists, exception details, tracebacks and diagnostic bundles. Never serialize credentials, keys, tokens, full environment or private paths. Logs/traceback/bundles are excluded from automatic public data manifests; reviewers receive separately sanitized evidence. Runtime handlers/queues/retention/canary tests/instrumentation remain F-LOG, outside F0.

**DECISION REQUIRED → B2:** illustrative clean closes100..114 establish the preceding sequence; after a quarantined conflict, the next clean close110 gives average gain13/14 and loss4/14 and RSI≈76.47 if continuity is retained. After reset with fewer than14 post-gap changes RSI is NaN. Neither continuation nor reset/warm-up is approved. Preserve raw conflict/quarantine facts, record NEEDS_DECISION for the affected future calculation, and keep independent F0 contracts moving. Do not silently select a reset policy.

## Complete inline examples — illustrative, not executed

Every ID, timestamp, hash, root and evidence reference in these blocks is **ILLUSTRATIVE**. The miniature raw9+RSI14 schema is for contract checking only, not a proposed reduction of the full production schema. No block proves actual source/export/target readiness. Complete objects are explicit, with no inheritance/implicit defaults.

Example wrapper fields are example_id:string, family:string, illustrative:true, description:string, expect:object, objects:array. Each objects item has name:string, type:string naming one CONTRACT table, value:complete instance. All objects instances must validate. Optional negative_case has input_contract:string naming a CONTRACT, input:complete or deliberately defective object, expected_error:string and optional consumer_context:run_context. E04 deliberately omits collection_status; E13 is schema-valid but deliberately violates the supplied consumer snapshot. expect.valid records the advertised overall oracle, not permission to overlook other invalid objects. Correlation/state assertions also apply after field/type validation. Compact one-line values keep repeated complete shapes reviewable without an inheritance DSL.

<!-- EXAMPLE E01 01_CANONICAL_SUCCESS -->
### E01 — Complete miniature canonical handoff, separately verified artifacts/preparation/publication; every identifier/hash/evidence is illustrative.

One illustrative canonical frame, in the miniature spec order, is shown below. Every row links through the sidecar to metadata.source_identities[0] (aggregate source scope, ILLUSTRATIVE_SECID); no identity columns are added to old public data. These toy values, missing indicators and hashes are not actual MOEX data or a formula test. NaN here denotes a missing indicator value; Google preserves its observed literal NaN token. No CSV/XLSX missing-value policy is changed by this illustration.

| open | close | high | low | value | volume | begin | end | ticker | RSI14 |
|---|---|---|---|---|---|---|---|---|---|
| 100 | 101 | 102 | 99 | 1000 | 10 | 2024-01-01 10:00:00 | 2024-01-01 18:45:00 | ILLUSTRATIVE_TICKER | NaN |
| 101 | 102 | 103 | 100 | 1100 | 11 | 2024-01-02 10:00:00 | 2024-01-02 18:45:00 | ILLUSTRATIVE_TICKER | NaN |
| 102 | 103 | 104 | 101 | 1200 | 12 | 2024-01-03 10:00:00 | 2024-01-03 18:45:00 | ILLUSTRATIVE_TICKER | NaN |

```json
{
  "example_id": "E01",
  "family": "01_CANONICAL_SUCCESS",
  "illustrative": true,
  "description": "Complete miniature canonical handoff, separately verified artifacts/preparation/publication; every identifier/hash/evidence is illustrative.",
  "expect": {"valid":true,"invariants":["same run/snapshot/as_of/schema across results","distinct selected-format artifact IDs","preparation and full readback separately evidenced","CURRENT_VERIFIED simulated only after all required gates"]},
  "objects": [
    {"name":"spec","type":"spec","value":{"contract_version":1,"dataset_id":"10years_data_1d_interval","stem":"10years_data_1d_interval","horizon":"10years","interval":24,"schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14","columns":[{"name":"open","logical_type":"number","nullable":false,"unit":null,"unit_status":"UNKNOWN","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"close","logical_type":"number","nullable":false,"unit":null,"unit_status":"UNKNOWN","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"high","logical_type":"number","nullable":false,"unit":null,"unit_status":"UNKNOWN","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"low","logical_type":"number","nullable":false,"unit":null,"unit_status":"UNKNOWN","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"value","logical_type":"number","nullable":false,"unit":null,"unit_status":"UNKNOWN","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"volume","logical_type":"number","nullable":false,"unit":null,"unit_status":"UNKNOWN","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"begin","logical_type":"exchange_timestamp","nullable":false,"unit":null,"unit_status":"NOT_APPLICABLE","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"end","logical_type":"exchange_timestamp","nullable":false,"unit":null,"unit_status":"NOT_APPLICABLE","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"ticker","logical_type":"string","nullable":false,"unit":null,"unit_status":"NOT_APPLICABLE","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"RSI14","logical_type":"number","nullable":true,"unit":null,"unit_status":"UNKNOWN","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"}],"key_fields":["engine","market","source_scope","board","raw_instrument_id","interval","begin"],"sort_fields":["engine","market","source_scope","board","raw_instrument_id","interval","begin"],"filenames":{"csv":"10years_data_1d_interval.csv","xlsx":"10years_data_1d_interval.xlsx"},"required_exports":[{"export_id":"ILLUSTRATIVE_REQUIRED_CSV","target_id":"ILLUSTRATIVE_HOST_CSV","format":"csv","required":true,"source_dataset_id":"10years_data_1d_interval","source_artifact":"10years_data_1d_interval.csv","interface":{"columns":["open","close","high","low","value","volume","begin","end","ticker","RSI14"],"header_rows":1,"index_columns":0,"anchor":null,"nan_token":null,"configuration_ref":null}},{"export_id":"ILLUSTRATIVE_REQUIRED_XLSX","target_id":"ILLUSTRATIVE_HOST_XLSX","format":"xlsx","required":true,"source_dataset_id":"10years_data_1d_interval","source_artifact":"10years_data_1d_interval.xlsx","interface":{"columns":["open","close","high","low","value","volume","begin","end","ticker","RSI14"],"header_rows":1,"index_columns":0,"anchor":null,"nan_token":null,"configuration_ref":null}},{"export_id":"ILLUSTRATIVE_REQUIRED_GOOGLE_SHEETS","target_id":"ILLUSTRATIVE_SHEETS","format":"google_sheets","required":true,"source_dataset_id":"10years_data_1d_interval","source_artifact":"ILLUSTRATIVE_CANONICAL_GOOGLE_PROJECTION","interface":{"columns":["open","close","high","low","value","volume","begin","end","ticker","RSI14"],"header_rows":1,"index_columns":0,"anchor":"A1","nan_token":"NaN","configuration_ref":"ILLUSTRATIVE/retained-production-sheet-alias"}}],"empty_policy":"REQUIRE_NONEMPTY","notes":"ILLUSTRATIVE reduced raw9+RSI14 schema, not a replacement for full production enriched schema."}},
    {"name":"run","type":"run_context","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","as_of":"2026-10-04T09:00:00Z","producer_sha":"ILLUSTRATIVE_SHA","environment":"ILLUSTRATIVE_OFFLINE","package":"ILLUSTRATIVE_F0","roots":{"data":"ILLUSTRATIVE/data","log":"ILLUSTRATIVE/log","temp":"ILLUSTRATIVE/temp","evidence":"ILLUSTRATIVE/evidence"},"required_targets":["ILLUSTRATIVE_HOST_CSV","ILLUSTRATIVE_HOST_XLSX","ILLUSTRATIVE_SHEETS"]}},
    {"name":"metadata","type":"metadata","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14","source":"MOEX_ISS_RAW","source_identities":[{"engine":"stock","market":"shares","raw_instrument_id":"ILLUSTRATIVE_SECID","source_scope":"AGGREGATE","board":null,"isin":null,"display_ticker":"ILLUSTRATIVE_TICKER","null_reasons":{"board":"Explicit board-less aggregate endpoint","isin":"UNKNOWN"}}],"provenance":[{"endpoint":"https://iss.moex.com/iss/engines/stock/markets/shares/securities/ILLUSTRATIVE_SECID/candles.csv","section":"candles","acquired_at":"2026-10-04T09:00:00Z","source_scope":"AGGREGATE","metadata_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","response_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","evidence_ref":"ILLUSTRATIVE/source-response"}],"requested_bounds":{"start":{"value":"2024-01-01 00:00:00","state":"KNOWN","precision":"SECOND","timezone":"Europe/Moscow","evidence_ref":"ILLUSTRATIVE/source-time-and-zone-evidence","reason":null},"end":{"value":"2026-10-04 12:00:00","state":"KNOWN","precision":"SECOND","timezone":"Europe/Moscow","evidence_ref":"ILLUSTRATIVE/source-time-and-zone-evidence","reason":null}},"instrument_bounds":{"issue_date":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"},"listing_date":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"},"admission_date":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"},"first_trade":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"},"last_trade":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"},"delisting_date":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"}},"available_bounds":{"start":{"value":"2024-01-01 10:00:00","state":"KNOWN","precision":"SECOND","timezone":"Europe/Moscow","evidence_ref":"ILLUSTRATIVE/source-time-and-zone-evidence","reason":null},"end":{"value":"2024-01-03 18:45:00","state":"KNOWN","precision":"SECOND","timezone":"Europe/Moscow","evidence_ref":"ILLUSTRATIVE/source-time-and-zone-evidence","reason":null}},"actual_bounds":{"start":{"value":"2024-01-01 10:00:00","state":"KNOWN","precision":"SECOND","timezone":"Europe/Moscow","evidence_ref":"ILLUSTRATIVE/source-time-and-zone-evidence","reason":null},"end":{"value":"2024-01-03 18:45:00","state":"KNOWN","precision":"SECOND","timezone":"Europe/Moscow","evidence_ref":"ILLUSTRATIVE/source-time-and-zone-evidence","reason":null}},"collection_status":"COMPLETE","coverage_status":"COMPLETE","counts":{"rows_received":3,"rows_accepted":3,"rows_quarantined":0,"pages":1,"instruments":1},"gaps":[],"logical_successor":null,"mapping_evidence_ref":null,"quality_ref":"ILLUSTRATIVE/quality","quarantine_ref":null,"completion_evidence_ref":"ILLUSTRATIVE/completed-bars","errors":[],"null_reasons":{"logical_successor":"NO_EVIDENCED_MAPPING","mapping_evidence_ref":"NO_EVIDENCED_MAPPING","quarantine_ref":"NO_QUARANTINE_ARTIFACT"}}},
    {"name":"quality","type":"quality_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","status":"PASS","checks":[{"check_id":"ILLUSTRATIVE_KEYS_SCHEMA_COVERAGE","status":"PASS","evidence_ref":"ILLUSTRATIVE/quality","reason":null}],"rows_checked":3,"accepted_rows":3,"quarantined_rows":0,"quarantine_ref":null,"evidence_ref":"ILLUSTRATIVE/quality","errors":[],"null_reasons":{"quarantine_ref":"NO_QUARANTINE"}}},
    {"name":"csv_intent","type":"artifact_intent","value":{"dataset_id":"10years_data_1d_interval","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_CSV","format":"csv","candidate_relative_path":"10years_data_1d_interval.csv"}},
    {"name":"csv_caps","type":"target_capabilities","value":{"capabilities_ref":"ILLUSTRATIVE/caps/csv","target_id":"ILLUSTRATIVE_HOST_CSV","format":"csv","verification":"NOT_APPLICABLE","row_limit":null,"column_limit":null,"cell_limit":null,"grid":null,"limit_evidence_ref":null,"grid_evidence_ref":null,"verified_at":null,"verification_scope":"NOT_APPLICABLE","null_reasons":{"cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","verified_at":"NOT_APPLICABLE_TO_SELECTED_FORMAT","limit_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT","row_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","column_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}}},
    {"name":"csv_assessment","type":"capacity_assessment","value":{"format":"csv","status":"PASS","capabilities_ref":"ILLUSTRATIVE/caps/csv","source_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":4,"required_columns":10,"required_cells":40,"row_limit":null,"column_limit":null,"cell_limit":null,"workbook_cells_before":null,"workbook_cells_after":null,"limit_evidence_ref":null,"grid_evidence_ref":null,"errors":[],"null_reasons":{"workbook_cells_before":"NOT_APPLICABLE_TO_SELECTED_FORMAT","cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","limit_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT","workbook_cells_after":"NOT_APPLICABLE_TO_SELECTED_FORMAT","row_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","column_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}}},
    {"name":"csv_artifact","type":"artifact_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_CSV","format":"csv","status":"VERIFIED","expected_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"expected_schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14","expected_columns":["open","close","high","low","value","volume","begin","end","ticker","RSI14"],"relative_path":"10years_data_1d_interval.csv","actual_rows":3,"bytes":1234,"sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","canonical_fingerprint":{"basis":"LOGICAL_ROWS_SHA256","value":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"verification_evidence_ref":"ILLUSTRATIVE/roundtrip/csv","errors":[]}},
    {"name":"xlsx_intent","type":"artifact_intent","value":{"dataset_id":"10years_data_1d_interval","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_XLSX","format":"xlsx","candidate_relative_path":"10years_data_1d_interval.xlsx"}},
    {"name":"xlsx_caps","type":"target_capabilities","value":{"capabilities_ref":"ILLUSTRATIVE/caps/xlsx","target_id":"ILLUSTRATIVE_HOST_XLSX","format":"xlsx","verification":"VERIFIED","row_limit":1048576,"column_limit":16384,"cell_limit":null,"grid":null,"limit_evidence_ref":"ILLUSTRATIVE/limits/xlsx","grid_evidence_ref":null,"verified_at":"2026-10-04T09:00:00Z","verification_scope":"DOCUMENTED_FORMAT","null_reasons":{"grid":"NOT_APPLICABLE_TO_SELECTED_FORMAT","cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}}},
    {"name":"xlsx_assessment","type":"capacity_assessment","value":{"format":"xlsx","status":"PASS","capabilities_ref":"ILLUSTRATIVE/caps/xlsx","source_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":4,"required_columns":10,"required_cells":40,"row_limit":1048576,"column_limit":16384,"cell_limit":null,"workbook_cells_before":null,"workbook_cells_after":null,"limit_evidence_ref":"ILLUSTRATIVE/limits/xlsx","grid_evidence_ref":null,"errors":[],"null_reasons":{"workbook_cells_before":"NOT_APPLICABLE_TO_SELECTED_FORMAT","cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT","workbook_cells_after":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}}},
    {"name":"xlsx_artifact","type":"artifact_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_XLSX","format":"xlsx","status":"VERIFIED","expected_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"expected_schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14","expected_columns":["open","close","high","low","value","volume","begin","end","ticker","RSI14"],"relative_path":"10years_data_1d_interval.xlsx","actual_rows":3,"bytes":1234,"sha256":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","canonical_fingerprint":{"basis":"LOGICAL_ROWS_SHA256","value":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"verification_evidence_ref":"ILLUSTRATIVE/roundtrip/xlsx","errors":[]}},
    {"name":"csv_export_intent","type":"export_intent","value":{"dataset_id":"10years_data_1d_interval","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_CSV","target_id":"ILLUSTRATIVE_HOST_CSV","format":"csv","required":true,"capabilities_ref":"ILLUSTRATIVE/caps/csv"}},
    {"name":"csv_export_result","type":"export_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_CSV","target_id":"ILLUSTRATIVE_HOST_CSV","format":"csv","required":true,"preflight_status":"PASS","export_status":"SUCCEEDED","capacity_assessment":{"format":"csv","status":"PASS","capabilities_ref":"ILLUSTRATIVE/caps/csv","source_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":4,"required_columns":10,"required_cells":40,"row_limit":null,"column_limit":null,"cell_limit":null,"workbook_cells_before":null,"workbook_cells_after":null,"limit_evidence_ref":null,"grid_evidence_ref":null,"errors":[],"null_reasons":{"workbook_cells_before":"NOT_APPLICABLE_TO_SELECTED_FORMAT","cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","limit_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT","workbook_cells_after":"NOT_APPLICABLE_TO_SELECTED_FORMAT","row_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","column_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}},"measured_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"export_verification_ref":"ILLUSTRATIVE/prepared/csv","errors":[]}},
    {"name":"xlsx_export_intent","type":"export_intent","value":{"dataset_id":"10years_data_1d_interval","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_XLSX","target_id":"ILLUSTRATIVE_HOST_XLSX","format":"xlsx","required":true,"capabilities_ref":"ILLUSTRATIVE/caps/xlsx"}},
    {"name":"xlsx_export_result","type":"export_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_XLSX","target_id":"ILLUSTRATIVE_HOST_XLSX","format":"xlsx","required":true,"preflight_status":"PASS","export_status":"SUCCEEDED","capacity_assessment":{"format":"xlsx","status":"PASS","capabilities_ref":"ILLUSTRATIVE/caps/xlsx","source_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":4,"required_columns":10,"required_cells":40,"row_limit":1048576,"column_limit":16384,"cell_limit":null,"workbook_cells_before":null,"workbook_cells_after":null,"limit_evidence_ref":"ILLUSTRATIVE/limits/xlsx","grid_evidence_ref":null,"errors":[],"null_reasons":{"workbook_cells_before":"NOT_APPLICABLE_TO_SELECTED_FORMAT","cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT","workbook_cells_after":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}},"measured_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"export_verification_ref":"ILLUSTRATIVE/prepared/xlsx","errors":[]}},
    {"name":"google_sheets_export_intent","type":"export_intent","value":{"dataset_id":"10years_data_1d_interval","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_GOOGLE_SHEETS","target_id":"ILLUSTRATIVE_SHEETS","format":"google_sheets","required":true,"capabilities_ref":"ILLUSTRATIVE/caps/google_sheets"}},
    {"name":"google_sheets_export_result","type":"export_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_GOOGLE_SHEETS","target_id":"ILLUSTRATIVE_SHEETS","format":"google_sheets","required":true,"preflight_status":"PASS","export_status":"SUCCEEDED","capacity_assessment":{"format":"google_sheets","status":"PASS","capabilities_ref":"ILLUSTRATIVE/caps/google_sheets","source_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":4,"required_columns":10,"required_cells":40,"row_limit":null,"column_limit":null,"cell_limit":100,"workbook_cells_before":40,"workbook_cells_after":40,"limit_evidence_ref":"ILLUSTRATIVE/limits/google_sheets","grid_evidence_ref":"ILLUSTRATIVE/complete-workbook-grid","errors":[],"null_reasons":{"row_limit":"NOT_APPLICABLE_TO_ILLUSTRATIVE_CELL_ONLY_ORACLE","column_limit":"NOT_APPLICABLE_TO_ILLUSTRATIVE_CELL_ONLY_ORACLE"}},"measured_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"export_verification_ref":"ILLUSTRATIVE/prepared/google_sheets","errors":[]}},
    {"name":"google_caps","type":"target_capabilities","value":{"capabilities_ref":"ILLUSTRATIVE/caps/google_sheets","target_id":"ILLUSTRATIVE_SHEETS","format":"google_sheets","verification":"VERIFIED","row_limit":null,"column_limit":null,"cell_limit":100,"grid":[{"is_target":true,"rows":4,"columns":10}],"limit_evidence_ref":"ILLUSTRATIVE/limits/google_sheets","grid_evidence_ref":"ILLUSTRATIVE/complete-workbook-grid","verified_at":"2026-10-04T09:00:00Z","verification_scope":"INJECTED","null_reasons":{"row_limit":"NOT_APPLICABLE_TO_ILLUSTRATIVE_CELL_ONLY_ORACLE","column_limit":"NOT_APPLICABLE_TO_ILLUSTRATIVE_CELL_ONLY_ORACLE"}}},
    {"name":"google_assessment","type":"capacity_assessment","value":{"format":"google_sheets","status":"PASS","capabilities_ref":"ILLUSTRATIVE/caps/google_sheets","source_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":4,"required_columns":10,"required_cells":40,"row_limit":null,"column_limit":null,"cell_limit":100,"workbook_cells_before":40,"workbook_cells_after":40,"limit_evidence_ref":"ILLUSTRATIVE/limits/google_sheets","grid_evidence_ref":"ILLUSTRATIVE/complete-workbook-grid","errors":[],"null_reasons":{"row_limit":"NOT_APPLICABLE_TO_ILLUSTRATIVE_CELL_ONLY_ORACLE","column_limit":"NOT_APPLICABLE_TO_ILLUSTRATIVE_CELL_ONLY_ORACLE"}}},
    {"name":"release","type":"release_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","as_of":"2026-10-04T09:00:00Z","candidate_release_id":"ILLUSTRATIVE_CANDIDATE_ILLUSTRATIVE_RUN_A","status":"CURRENT_VERIFIED","stage_outcomes":{"collection":"COMPLETE","validation":"PASS","artifact":"VERIFIED","preflight":"PASS","export":"SUCCEEDED","publication":"ACKNOWLEDGED","readback":"VERIFIED"},"required_export_ids":["ILLUSTRATIVE_REQUIRED_CSV","ILLUSTRATIVE_REQUIRED_XLSX","ILLUSTRATIVE_REQUIRED_GOOGLE_SHEETS"],"failed_required_exports":[],"per_target_outcomes":[{"target_id":"ILLUSTRATIVE_HOST_CSV","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_CSV"],"publication_status":"ACKNOWLEDGED","readback_status":"VERIFIED","observed_generation":"CANDIDATE","previous_verification":"VERIFIED","preservation_outcome":"CHANGED_VERIFIED","previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_freshness":"STALE","baseline":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"previous_evidence_ref":"ILLUSTRATIVE/previous/VERIFIED","readback_evidence":{"expected":{"basis":"LOGICAL_ROWS_SHA256","value":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"observed":{"basis":"LOGICAL_ROWS_SHA256","value":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"evidence_ref":"ILLUSTRATIVE/candidate-readback/csv","scope":"FULL_REQUIRED_TARGET"},"mutation_at":"2026-10-04T09:00:00Z","readback_at":"2026-10-04T09:05:00Z","errors":[]},{"target_id":"ILLUSTRATIVE_HOST_XLSX","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_XLSX"],"publication_status":"ACKNOWLEDGED","readback_status":"VERIFIED","observed_generation":"CANDIDATE","previous_verification":"VERIFIED","preservation_outcome":"CHANGED_VERIFIED","previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_freshness":"STALE","baseline":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"previous_evidence_ref":"ILLUSTRATIVE/previous/VERIFIED","readback_evidence":{"expected":{"basis":"LOGICAL_ROWS_SHA256","value":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"observed":{"basis":"LOGICAL_ROWS_SHA256","value":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"evidence_ref":"ILLUSTRATIVE/candidate-readback/xlsx","scope":"FULL_REQUIRED_TARGET"},"mutation_at":"2026-10-04T09:00:00Z","readback_at":"2026-10-04T09:05:00Z","errors":[]},{"target_id":"ILLUSTRATIVE_SHEETS","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_GOOGLE_SHEETS"],"publication_status":"ACKNOWLEDGED","readback_status":"VERIFIED","observed_generation":"CANDIDATE","previous_verification":"VERIFIED","preservation_outcome":"CHANGED_VERIFIED","previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_freshness":"STALE","baseline":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"previous_evidence_ref":"ILLUSTRATIVE/previous/VERIFIED","readback_evidence":{"expected":{"basis":"LOGICAL_ROWS_SHA256","value":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"observed":{"basis":"LOGICAL_ROWS_SHA256","value":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"evidence_ref":"ILLUSTRATIVE/candidate-readback/google_sheets","scope":"FULL_REQUIRED_TARGET"},"mutation_at":"2026-10-04T09:00:00Z","readback_at":"2026-10-04T09:05:00Z","errors":[]}],"candidate_manifest_ref":"ILLUSTRATIVE/candidate-manifest","current_manifest_ref":"ILLUSTRATIVE/candidate-manifest","previous_manifest_ref":"ILLUSTRATIVE/previous-manifest","published_as_of":"2026-10-04T09:00:00Z","exit_status":0,"errors":[]}}
  ]
}
```

<!-- EXAMPLE E02 02_EMPTY_FAILED_MISSING -->
### E02 — VALID_EMPTY is a successful source response, yet required candle dataset nonempty check fails.

```json
{
  "example_id": "E02",
  "family": "02_EMPTY_FAILED_MISSING",
  "illustrative": true,
  "description": "VALID_EMPTY is a successful source response, yet required candle dataset nonempty check fails.",
  "expect": {"valid":true,"invariants":["collection VALID_EMPTY distinct from failure","zero counts measured","required nonempty gate FAIL"]},
  "objects": [
    {"name":"spec","type":"spec","value":{"contract_version":1,"dataset_id":"10years_data_1d_interval","stem":"10years_data_1d_interval","horizon":"10years","interval":24,"schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14","columns":[{"name":"open","logical_type":"number","nullable":false,"unit":null,"unit_status":"UNKNOWN","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"close","logical_type":"number","nullable":false,"unit":null,"unit_status":"UNKNOWN","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"high","logical_type":"number","nullable":false,"unit":null,"unit_status":"UNKNOWN","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"low","logical_type":"number","nullable":false,"unit":null,"unit_status":"UNKNOWN","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"value","logical_type":"number","nullable":false,"unit":null,"unit_status":"UNKNOWN","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"volume","logical_type":"number","nullable":false,"unit":null,"unit_status":"UNKNOWN","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"begin","logical_type":"exchange_timestamp","nullable":false,"unit":null,"unit_status":"NOT_APPLICABLE","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"end","logical_type":"exchange_timestamp","nullable":false,"unit":null,"unit_status":"NOT_APPLICABLE","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"ticker","logical_type":"string","nullable":false,"unit":null,"unit_status":"NOT_APPLICABLE","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"RSI14","logical_type":"number","nullable":true,"unit":null,"unit_status":"UNKNOWN","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"}],"key_fields":["engine","market","source_scope","board","raw_instrument_id","interval","begin"],"sort_fields":["engine","market","source_scope","board","raw_instrument_id","interval","begin"],"filenames":{"csv":"10years_data_1d_interval.csv","xlsx":"10years_data_1d_interval.xlsx"},"required_exports":[{"export_id":"ILLUSTRATIVE_REQUIRED_CSV","target_id":"ILLUSTRATIVE_HOST_CSV","format":"csv","required":true,"source_dataset_id":"10years_data_1d_interval","source_artifact":"10years_data_1d_interval.csv","interface":{"columns":["open","close","high","low","value","volume","begin","end","ticker","RSI14"],"header_rows":1,"index_columns":0,"anchor":null,"nan_token":null,"configuration_ref":null}},{"export_id":"ILLUSTRATIVE_REQUIRED_XLSX","target_id":"ILLUSTRATIVE_HOST_XLSX","format":"xlsx","required":true,"source_dataset_id":"10years_data_1d_interval","source_artifact":"10years_data_1d_interval.xlsx","interface":{"columns":["open","close","high","low","value","volume","begin","end","ticker","RSI14"],"header_rows":1,"index_columns":0,"anchor":null,"nan_token":null,"configuration_ref":null}},{"export_id":"ILLUSTRATIVE_REQUIRED_GOOGLE_SHEETS","target_id":"ILLUSTRATIVE_SHEETS","format":"google_sheets","required":true,"source_dataset_id":"10years_data_1d_interval","source_artifact":"ILLUSTRATIVE_CANONICAL_GOOGLE_PROJECTION","interface":{"columns":["open","close","high","low","value","volume","begin","end","ticker","RSI14"],"header_rows":1,"index_columns":0,"anchor":"A1","nan_token":"NaN","configuration_ref":"ILLUSTRATIVE/retained-production-sheet-alias"}}],"empty_policy":"REQUIRE_NONEMPTY","notes":"ILLUSTRATIVE reduced raw9+RSI14 schema, not a replacement for full production enriched schema."}},
    {"name":"metadata","type":"metadata","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14","source":"MOEX_ISS_RAW","source_identities":[{"engine":"stock","market":"shares","raw_instrument_id":"ILLUSTRATIVE_SECID","source_scope":"AGGREGATE","board":null,"isin":null,"display_ticker":"ILLUSTRATIVE_TICKER","null_reasons":{"board":"Explicit board-less aggregate endpoint","isin":"UNKNOWN"}}],"provenance":[{"endpoint":"https://iss.moex.com/iss/engines/stock/markets/shares/securities/ILLUSTRATIVE_SECID/candles.csv","section":"candles","acquired_at":"2026-10-04T09:00:00Z","source_scope":"AGGREGATE","metadata_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","response_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","evidence_ref":"ILLUSTRATIVE/source-response"}],"requested_bounds":{"start":{"value":"2024-01-01 00:00:00","state":"KNOWN","precision":"SECOND","timezone":"Europe/Moscow","evidence_ref":"ILLUSTRATIVE/source-time-and-zone-evidence","reason":null},"end":{"value":"2026-10-04 12:00:00","state":"KNOWN","precision":"SECOND","timezone":"Europe/Moscow","evidence_ref":"ILLUSTRATIVE/source-time-and-zone-evidence","reason":null}},"instrument_bounds":{"issue_date":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"},"listing_date":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"},"admission_date":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"},"first_trade":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"},"last_trade":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"},"delisting_date":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"}},"available_bounds":{"start":{"value":"2024-01-01 10:00:00","state":"KNOWN","precision":"SECOND","timezone":"Europe/Moscow","evidence_ref":"ILLUSTRATIVE/source-time-and-zone-evidence","reason":null},"end":{"value":"2024-01-03 18:45:00","state":"KNOWN","precision":"SECOND","timezone":"Europe/Moscow","evidence_ref":"ILLUSTRATIVE/source-time-and-zone-evidence","reason":null}},"actual_bounds":{"start":{"value":null,"state":"EMPTY","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"Successful empty source response"},"end":{"value":null,"state":"EMPTY","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"Successful empty source response"}},"collection_status":"VALID_EMPTY","coverage_status":"COMPLETE","counts":{"rows_received":0,"rows_accepted":0,"rows_quarantined":0,"pages":1,"instruments":1},"gaps":[],"logical_successor":null,"mapping_evidence_ref":null,"quality_ref":"ILLUSTRATIVE/quality","quarantine_ref":null,"completion_evidence_ref":"ILLUSTRATIVE/completed-bars","errors":[],"null_reasons":{"logical_successor":"NO_EVIDENCED_MAPPING","mapping_evidence_ref":"NO_EVIDENCED_MAPPING","quarantine_ref":"NO_QUARANTINE_ARTIFACT"}}},
    {"name":"quality","type":"quality_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","status":"FAIL","checks":[{"check_id":"REQUIRED_NONEMPTY","status":"FAIL","evidence_ref":"ILLUSTRATIVE/empty-response","reason":"VALID_EMPTY response violates mandatory dataset nonempty gate"}],"rows_checked":0,"accepted_rows":0,"quarantined_rows":0,"quarantine_ref":null,"evidence_ref":"ILLUSTRATIVE/quality","errors":[{"category":"QUALITY","code":"REQUIRED_DATASET_EMPTY","stage":"export","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted REQUIRED_DATASET_EMPTY","run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","instrument":null,"interval":24,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE","attempt":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}],"null_reasons":{"quarantine_ref":"NO_QUARANTINE"}}}
  ]
}
```

<!-- EXAMPLE E03 02_EMPTY_FAILED_MISSING -->
### E03 — Failed request with zero received rows stays FAILED, coverage NOT_VERIFIED and quality FAIL.

```json
{
  "example_id": "E03",
  "family": "02_EMPTY_FAILED_MISSING",
  "illustrative": true,
  "description": "Failed request with zero received rows stays FAILED, coverage NOT_VERIFIED and quality FAIL.",
  "expect": {"valid":true},
  "objects": [
    {"name":"metadata","type":"metadata","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14","source":"MOEX_ISS_RAW","source_identities":[{"engine":"stock","market":"shares","raw_instrument_id":"ILLUSTRATIVE_SECID","source_scope":"AGGREGATE","board":null,"isin":null,"display_ticker":"ILLUSTRATIVE_TICKER","null_reasons":{"board":"Explicit board-less aggregate endpoint","isin":"UNKNOWN"}}],"provenance":[{"endpoint":"https://iss.moex.com/iss/engines/stock/markets/shares/securities/ILLUSTRATIVE_SECID/candles.csv","section":"candles","acquired_at":"2026-10-04T09:00:00Z","source_scope":"AGGREGATE","metadata_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","response_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","evidence_ref":"ILLUSTRATIVE/source-response"}],"requested_bounds":{"start":{"value":"2024-01-01 00:00:00","state":"KNOWN","precision":"SECOND","timezone":"Europe/Moscow","evidence_ref":"ILLUSTRATIVE/source-time-and-zone-evidence","reason":null},"end":{"value":"2026-10-04 12:00:00","state":"KNOWN","precision":"SECOND","timezone":"Europe/Moscow","evidence_ref":"ILLUSTRATIVE/source-time-and-zone-evidence","reason":null}},"instrument_bounds":{"issue_date":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"},"listing_date":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"},"admission_date":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"},"first_trade":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"},"last_trade":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"},"delisting_date":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"}},"available_bounds":{"start":{"value":"2024-01-01 10:00:00","state":"KNOWN","precision":"SECOND","timezone":"Europe/Moscow","evidence_ref":"ILLUSTRATIVE/source-time-and-zone-evidence","reason":null},"end":{"value":"2024-01-03 18:45:00","state":"KNOWN","precision":"SECOND","timezone":"Europe/Moscow","evidence_ref":"ILLUSTRATIVE/source-time-and-zone-evidence","reason":null}},"actual_bounds":{"start":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"Failed request; no verified actual output"},"end":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"Failed request; no verified actual output"}},"collection_status":"FAILED","coverage_status":"NOT_VERIFIED","counts":{"rows_received":0,"rows_accepted":null,"rows_quarantined":null,"pages":null,"instruments":null,"null_reasons":{"rows_accepted":"NOT_MEASURED_AFTER_FAILED_COLLECTION","rows_quarantined":"NOT_MEASURED_AFTER_FAILED_COLLECTION","pages":"NOT_MEASURED_AFTER_FAILED_COLLECTION","instruments":"NOT_MEASURED_AFTER_FAILED_COLLECTION"}},"gaps":[],"logical_successor":null,"mapping_evidence_ref":null,"quality_ref":"ILLUSTRATIVE/quality","quarantine_ref":null,"completion_evidence_ref":null,"errors":[{"category":"SOURCE","code":"REQUEST_FAILED","stage":"candles","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted REQUEST_FAILED","run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","instrument":null,"interval":24,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE","attempt":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}],"null_reasons":{"logical_successor":"NO_EVIDENCED_MAPPING","mapping_evidence_ref":"NO_EVIDENCED_MAPPING","quarantine_ref":"NO_QUARANTINE_ARTIFACT","completion_evidence_ref":"NOT_VERIFIED_AFTER_FAILURE"}}},
    {"name":"quality","type":"quality_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","status":"FAIL","checks":[{"check_id":"SOURCE_COMPLETE","status":"FAIL","evidence_ref":"ILLUSTRATIVE/failed-request","reason":"Failed request is not empty success"}],"rows_checked":0,"accepted_rows":null,"quarantined_rows":null,"quarantine_ref":null,"evidence_ref":"ILLUSTRATIVE/quality","errors":[{"category":"SOURCE","code":"REQUEST_FAILED","stage":"candles","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted REQUEST_FAILED","run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","instrument":null,"interval":24,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE","attempt":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}],"null_reasons":{"quarantine_ref":"NO_QUARANTINE"}}}
  ]
}
```

<!-- EXAMPLE E04 02_EMPTY_FAILED_MISSING -->
### E04 — Missing candles response block is FAILED. A separate deliberate missing contract field must be rejected.

```json
{
  "example_id": "E04",
  "family": "02_EMPTY_FAILED_MISSING",
  "illustrative": true,
  "description": "Missing candles response block is FAILED. A separate deliberate missing contract field must be rejected.",
  "expect": {"valid":false,"negative_case":"REQUIRED_FIELD_REJECTION","valid_object":"valid_missing_block"},
  "negative_case": {"input_contract":"metadata","input":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14","source":"MOEX_ISS_RAW","source_identities":[{"engine":"stock","market":"shares","raw_instrument_id":"ILLUSTRATIVE_SECID","source_scope":"AGGREGATE","board":null,"isin":null,"display_ticker":"ILLUSTRATIVE_TICKER","null_reasons":{"board":"Explicit board-less aggregate endpoint","isin":"UNKNOWN"}}],"provenance":[{"endpoint":"https://iss.moex.com/iss/engines/stock/markets/shares/securities/ILLUSTRATIVE_SECID/candles.csv","section":"candles","acquired_at":"2026-10-04T09:00:00Z","source_scope":"AGGREGATE","metadata_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","response_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","evidence_ref":"ILLUSTRATIVE/source-response"}],"requested_bounds":{"start":{"value":"2024-01-01 00:00:00","state":"KNOWN","precision":"SECOND","timezone":"Europe/Moscow","evidence_ref":"ILLUSTRATIVE/source-time-and-zone-evidence","reason":null},"end":{"value":"2026-10-04 12:00:00","state":"KNOWN","precision":"SECOND","timezone":"Europe/Moscow","evidence_ref":"ILLUSTRATIVE/source-time-and-zone-evidence","reason":null}},"instrument_bounds":{"issue_date":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"},"listing_date":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"},"admission_date":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"},"first_trade":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"},"last_trade":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"},"delisting_date":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"}},"available_bounds":{"start":{"value":"2024-01-01 10:00:00","state":"KNOWN","precision":"SECOND","timezone":"Europe/Moscow","evidence_ref":"ILLUSTRATIVE/source-time-and-zone-evidence","reason":null},"end":{"value":"2024-01-03 18:45:00","state":"KNOWN","precision":"SECOND","timezone":"Europe/Moscow","evidence_ref":"ILLUSTRATIVE/source-time-and-zone-evidence","reason":null}},"actual_bounds":{"start":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"Failed request; no verified actual output"},"end":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"Failed request; no verified actual output"}},"coverage_status":"NOT_VERIFIED","counts":{"rows_received":0,"rows_accepted":null,"rows_quarantined":null,"pages":null,"instruments":null,"null_reasons":{"rows_accepted":"NOT_MEASURED_AFTER_FAILED_COLLECTION","rows_quarantined":"NOT_MEASURED_AFTER_FAILED_COLLECTION","pages":"NOT_MEASURED_AFTER_FAILED_COLLECTION","instruments":"NOT_MEASURED_AFTER_FAILED_COLLECTION"}},"gaps":[],"logical_successor":null,"mapping_evidence_ref":null,"quality_ref":"ILLUSTRATIVE/quality","quarantine_ref":null,"completion_evidence_ref":null,"errors":[{"category":"SOURCE","code":"MISSING_REQUIRED_BLOCK","stage":"export","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted MISSING_REQUIRED_BLOCK","run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","instrument":null,"interval":24,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE","attempt":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}],"null_reasons":{"logical_successor":"NO_EVIDENCED_MAPPING","mapping_evidence_ref":"NO_EVIDENCED_MAPPING","quarantine_ref":"NO_QUARANTINE_ARTIFACT","completion_evidence_ref":"NOT_VERIFIED_AFTER_FAILURE"}},"expected_error":"MISSING_REQUIRED_FIELD:collection_status"},
  "objects": [
    {"name":"valid_missing_block","type":"metadata","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14","source":"MOEX_ISS_RAW","source_identities":[{"engine":"stock","market":"shares","raw_instrument_id":"ILLUSTRATIVE_SECID","source_scope":"AGGREGATE","board":null,"isin":null,"display_ticker":"ILLUSTRATIVE_TICKER","null_reasons":{"board":"Explicit board-less aggregate endpoint","isin":"UNKNOWN"}}],"provenance":[{"endpoint":"https://iss.moex.com/iss/engines/stock/markets/shares/securities/ILLUSTRATIVE_SECID/candles.csv","section":"candles","acquired_at":"2026-10-04T09:00:00Z","source_scope":"AGGREGATE","metadata_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","response_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","evidence_ref":"ILLUSTRATIVE/source-response"}],"requested_bounds":{"start":{"value":"2024-01-01 00:00:00","state":"KNOWN","precision":"SECOND","timezone":"Europe/Moscow","evidence_ref":"ILLUSTRATIVE/source-time-and-zone-evidence","reason":null},"end":{"value":"2026-10-04 12:00:00","state":"KNOWN","precision":"SECOND","timezone":"Europe/Moscow","evidence_ref":"ILLUSTRATIVE/source-time-and-zone-evidence","reason":null}},"instrument_bounds":{"issue_date":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"},"listing_date":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"},"admission_date":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"},"first_trade":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"},"last_trade":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"},"delisting_date":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"ILLUSTRATIVE source fact unavailable"}},"available_bounds":{"start":{"value":"2024-01-01 10:00:00","state":"KNOWN","precision":"SECOND","timezone":"Europe/Moscow","evidence_ref":"ILLUSTRATIVE/source-time-and-zone-evidence","reason":null},"end":{"value":"2024-01-03 18:45:00","state":"KNOWN","precision":"SECOND","timezone":"Europe/Moscow","evidence_ref":"ILLUSTRATIVE/source-time-and-zone-evidence","reason":null}},"actual_bounds":{"start":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"Failed request; no verified actual output"},"end":{"value":null,"state":"UNKNOWN","precision":"UNKNOWN","timezone":null,"evidence_ref":null,"reason":"Failed request; no verified actual output"}},"collection_status":"FAILED","coverage_status":"NOT_VERIFIED","counts":{"rows_received":0,"rows_accepted":null,"rows_quarantined":null,"pages":null,"instruments":null,"null_reasons":{"rows_accepted":"NOT_MEASURED_AFTER_FAILED_COLLECTION","rows_quarantined":"NOT_MEASURED_AFTER_FAILED_COLLECTION","pages":"NOT_MEASURED_AFTER_FAILED_COLLECTION","instruments":"NOT_MEASURED_AFTER_FAILED_COLLECTION"}},"gaps":[],"logical_successor":null,"mapping_evidence_ref":null,"quality_ref":"ILLUSTRATIVE/quality","quarantine_ref":null,"completion_evidence_ref":null,"errors":[{"category":"SOURCE","code":"MISSING_REQUIRED_BLOCK","stage":"candles","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted MISSING_REQUIRED_BLOCK","run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","instrument":null,"interval":24,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE","attempt":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}],"null_reasons":{"logical_successor":"NO_EVIDENCED_MAPPING","mapping_evidence_ref":"NO_EVIDENCED_MAPPING","quarantine_ref":"NO_QUARANTINE_ARTIFACT","completion_evidence_ref":"NOT_VERIFIED_AFTER_FAILURE"}}}
  ]
}
```

<!-- EXAMPLE E05 03_OVERFLOW_PREVIOUS_STATES -->
### E05 — Boundary+1 overflow; explicit previous VERIFIED quality, no mutation and no invented preservation readback.

```json
{
  "example_id": "E05",
  "family": "03_OVERFLOW_PREVIOUS_STATES",
  "illustrative": true,
  "description": "Boundary+1 overflow; explicit previous VERIFIED quality, no mutation and no invented preservation readback.",
  "expect": {"valid":true,"invariants":["1048576 data +1 header =1048577 rows","preflight FAIL","required failure nonzero","previous identity/cutoff unchanged","ABSENT requires explicit evidence, UNVERIFIED never upgraded"]},
  "objects": [
    {"name":"shape","type":"shape","value":{"data_rows":1048576,"data_columns":10,"header_rows":1,"index_columns":0}},
    {"name":"caps","type":"target_capabilities","value":{"capabilities_ref":"ILLUSTRATIVE/caps/xlsx","target_id":"ILLUSTRATIVE_HOST_XLSX","format":"xlsx","verification":"VERIFIED","row_limit":1048576,"column_limit":16384,"cell_limit":null,"grid":null,"limit_evidence_ref":"ILLUSTRATIVE/limits/xlsx","grid_evidence_ref":null,"verified_at":"2026-10-04T09:00:00Z","verification_scope":"DOCUMENTED_FORMAT","null_reasons":{"grid":"NOT_APPLICABLE_TO_SELECTED_FORMAT","cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}}},
    {"name":"assessment","type":"capacity_assessment","value":{"format":"xlsx","status":"FAIL","capabilities_ref":"ILLUSTRATIVE/caps/xlsx","source_shape":{"data_rows":1048576,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":1048577,"required_columns":10,"required_cells":10485770,"row_limit":1048576,"column_limit":16384,"cell_limit":null,"workbook_cells_before":null,"workbook_cells_after":null,"limit_evidence_ref":"ILLUSTRATIVE/limits/xlsx","grid_evidence_ref":null,"errors":[{"category":"CAPACITY","code":"REQUIRED_EXPORT_OVERFLOW","stage":"preflight","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted REQUIRED_EXPORT_OVERFLOW","run_id":null,"snapshot_id":null,"dataset_id":null,"instrument":null,"interval":null,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"run_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","snapshot_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","dataset_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","interval":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE","attempt":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}],"null_reasons":{"workbook_cells_before":"NOT_APPLICABLE_TO_SELECTED_FORMAT","cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT","workbook_cells_after":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}}},
    {"name":"artifact","type":"artifact_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_XLSX","format":"xlsx","status":"NOT_RUN","expected_shape":{"data_rows":1048576,"data_columns":10,"header_rows":1,"index_columns":0},"expected_schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14","expected_columns":["open","close","high","low","value","volume","begin","end","ticker","RSI14"],"relative_path":null,"actual_rows":null,"bytes":null,"sha256":null,"canonical_fingerprint":null,"verification_evidence_ref":null,"errors":[{"category":"CAPACITY","code":"REQUIRED_EXPORT_OVERFLOW","stage":"preflight","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted REQUIRED_EXPORT_OVERFLOW","run_id":null,"snapshot_id":null,"dataset_id":null,"instrument":null,"interval":null,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"run_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","snapshot_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","dataset_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","interval":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE","attempt":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}]}},
    {"name":"export","type":"export_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_XLSX","target_id":"ILLUSTRATIVE_HOST_XLSX","format":"xlsx","required":true,"preflight_status":"FAIL","export_status":"NOT_RUN","capacity_assessment":{"format":"xlsx","status":"FAIL","capabilities_ref":"ILLUSTRATIVE/caps/xlsx","source_shape":{"data_rows":1048576,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":1048577,"required_columns":10,"required_cells":10485770,"row_limit":1048576,"column_limit":16384,"cell_limit":null,"workbook_cells_before":null,"workbook_cells_after":null,"limit_evidence_ref":"ILLUSTRATIVE/limits/xlsx","grid_evidence_ref":null,"errors":[{"category":"CAPACITY","code":"REQUIRED_EXPORT_OVERFLOW","stage":"preflight","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted REQUIRED_EXPORT_OVERFLOW","run_id":null,"snapshot_id":null,"dataset_id":null,"instrument":null,"interval":null,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"run_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","snapshot_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","dataset_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","interval":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE","attempt":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}],"null_reasons":{"workbook_cells_before":"NOT_APPLICABLE_TO_SELECTED_FORMAT","cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT","workbook_cells_after":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}},"measured_shape":null,"export_verification_ref":null,"errors":[{"category":"CAPACITY","code":"REQUIRED_EXPORT_OVERFLOW","stage":"preflight","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted REQUIRED_EXPORT_OVERFLOW","run_id":null,"snapshot_id":null,"dataset_id":null,"instrument":null,"interval":null,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"run_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","snapshot_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","dataset_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","interval":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE","attempt":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}]}},
    {"name":"target","type":"per_target_outcome","value":{"target_id":"ILLUSTRATIVE_HOST_XLSX","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_XLSX"],"publication_status":"NOT_ATTEMPTED","readback_status":"NOT_ATTEMPTED","observed_generation":"UNKNOWN","previous_verification":"VERIFIED","preservation_outcome":"NOT_TOUCHED","previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_freshness":"STALE","baseline":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"previous_evidence_ref":"ILLUSTRATIVE/previous/VERIFIED","readback_evidence":null,"mutation_at":null,"readback_at":null,"errors":[]}},
    {"name":"release","type":"release_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","as_of":"2026-10-04T09:00:00Z","candidate_release_id":"ILLUSTRATIVE_CANDIDATE_ILLUSTRATIVE_RUN_A","status":"BLOCKED","stage_outcomes":{"collection":"COMPLETE","validation":"PASS","artifact":"NOT_RUN","preflight":"FAIL","export":"NOT_RUN","publication":"NOT_ATTEMPTED","readback":"NOT_ATTEMPTED"},"required_export_ids":["ILLUSTRATIVE_REQUIRED_CSV","ILLUSTRATIVE_REQUIRED_XLSX","ILLUSTRATIVE_REQUIRED_GOOGLE_SHEETS"],"failed_required_exports":["ILLUSTRATIVE_REQUIRED_CSV","ILLUSTRATIVE_REQUIRED_XLSX","ILLUSTRATIVE_REQUIRED_GOOGLE_SHEETS"],"per_target_outcomes":[{"target_id":"ILLUSTRATIVE_HOST_CSV","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_CSV"],"publication_status":"NOT_ATTEMPTED","readback_status":"NOT_ATTEMPTED","observed_generation":"UNKNOWN","previous_verification":"VERIFIED","preservation_outcome":"NOT_TOUCHED","previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_freshness":"STALE","baseline":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"previous_evidence_ref":"ILLUSTRATIVE/previous/VERIFIED","readback_evidence":null,"mutation_at":null,"readback_at":null,"errors":[]},{"target_id":"ILLUSTRATIVE_HOST_XLSX","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_XLSX"],"publication_status":"NOT_ATTEMPTED","readback_status":"NOT_ATTEMPTED","observed_generation":"UNKNOWN","previous_verification":"VERIFIED","preservation_outcome":"NOT_TOUCHED","previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_freshness":"STALE","baseline":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"previous_evidence_ref":"ILLUSTRATIVE/previous/VERIFIED","readback_evidence":null,"mutation_at":null,"readback_at":null,"errors":[]},{"target_id":"ILLUSTRATIVE_SHEETS","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_GOOGLE_SHEETS"],"publication_status":"NOT_ATTEMPTED","readback_status":"NOT_ATTEMPTED","observed_generation":"UNKNOWN","previous_verification":"VERIFIED","preservation_outcome":"NOT_TOUCHED","previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_freshness":"STALE","baseline":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"previous_evidence_ref":"ILLUSTRATIVE/previous/VERIFIED","readback_evidence":null,"mutation_at":null,"readback_at":null,"errors":[]}],"candidate_manifest_ref":"ILLUSTRATIVE/candidate-manifest","current_manifest_ref":"ILLUSTRATIVE/previous-manifest","previous_manifest_ref":"ILLUSTRATIVE/previous-manifest","published_as_of":"2026-10-03T09:00:00Z","exit_status":1,"errors":[{"category":"CAPACITY","code":"REQUIRED_EXPORT_OVERFLOW","stage":"preflight","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted REQUIRED_EXPORT_OVERFLOW","run_id":null,"snapshot_id":null,"dataset_id":null,"instrument":null,"interval":null,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"run_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","snapshot_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","dataset_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","interval":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE","attempt":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}]}}
  ]
}
```

<!-- EXAMPLE E06 03_OVERFLOW_PREVIOUS_STATES -->
### E06 — Boundary+1 overflow; explicit previous ABSENT quality, no mutation and no invented preservation readback.

```json
{
  "example_id": "E06",
  "family": "03_OVERFLOW_PREVIOUS_STATES",
  "illustrative": true,
  "description": "Boundary+1 overflow; explicit previous ABSENT quality, no mutation and no invented preservation readback.",
  "expect": {"valid":true,"invariants":["1048576 data +1 header =1048577 rows","preflight FAIL","required failure nonzero","previous identity/cutoff unchanged","ABSENT requires explicit evidence, UNVERIFIED never upgraded"]},
  "objects": [
    {"name":"shape","type":"shape","value":{"data_rows":1048576,"data_columns":10,"header_rows":1,"index_columns":0}},
    {"name":"caps","type":"target_capabilities","value":{"capabilities_ref":"ILLUSTRATIVE/caps/xlsx","target_id":"ILLUSTRATIVE_HOST_XLSX","format":"xlsx","verification":"VERIFIED","row_limit":1048576,"column_limit":16384,"cell_limit":null,"grid":null,"limit_evidence_ref":"ILLUSTRATIVE/limits/xlsx","grid_evidence_ref":null,"verified_at":"2026-10-04T09:00:00Z","verification_scope":"DOCUMENTED_FORMAT","null_reasons":{"grid":"NOT_APPLICABLE_TO_SELECTED_FORMAT","cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}}},
    {"name":"assessment","type":"capacity_assessment","value":{"format":"xlsx","status":"FAIL","capabilities_ref":"ILLUSTRATIVE/caps/xlsx","source_shape":{"data_rows":1048576,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":1048577,"required_columns":10,"required_cells":10485770,"row_limit":1048576,"column_limit":16384,"cell_limit":null,"workbook_cells_before":null,"workbook_cells_after":null,"limit_evidence_ref":"ILLUSTRATIVE/limits/xlsx","grid_evidence_ref":null,"errors":[{"category":"CAPACITY","code":"REQUIRED_EXPORT_OVERFLOW","stage":"preflight","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted REQUIRED_EXPORT_OVERFLOW","run_id":null,"snapshot_id":null,"dataset_id":null,"instrument":null,"interval":null,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"run_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","snapshot_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","dataset_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","interval":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE","attempt":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}],"null_reasons":{"workbook_cells_before":"NOT_APPLICABLE_TO_SELECTED_FORMAT","cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT","workbook_cells_after":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}}},
    {"name":"artifact","type":"artifact_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_XLSX","format":"xlsx","status":"NOT_RUN","expected_shape":{"data_rows":1048576,"data_columns":10,"header_rows":1,"index_columns":0},"expected_schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14","expected_columns":["open","close","high","low","value","volume","begin","end","ticker","RSI14"],"relative_path":null,"actual_rows":null,"bytes":null,"sha256":null,"canonical_fingerprint":null,"verification_evidence_ref":null,"errors":[{"category":"CAPACITY","code":"REQUIRED_EXPORT_OVERFLOW","stage":"preflight","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted REQUIRED_EXPORT_OVERFLOW","run_id":null,"snapshot_id":null,"dataset_id":null,"instrument":null,"interval":null,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"run_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","snapshot_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","dataset_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","interval":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE","attempt":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}]}},
    {"name":"export","type":"export_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_XLSX","target_id":"ILLUSTRATIVE_HOST_XLSX","format":"xlsx","required":true,"preflight_status":"FAIL","export_status":"NOT_RUN","capacity_assessment":{"format":"xlsx","status":"FAIL","capabilities_ref":"ILLUSTRATIVE/caps/xlsx","source_shape":{"data_rows":1048576,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":1048577,"required_columns":10,"required_cells":10485770,"row_limit":1048576,"column_limit":16384,"cell_limit":null,"workbook_cells_before":null,"workbook_cells_after":null,"limit_evidence_ref":"ILLUSTRATIVE/limits/xlsx","grid_evidence_ref":null,"errors":[{"category":"CAPACITY","code":"REQUIRED_EXPORT_OVERFLOW","stage":"preflight","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted REQUIRED_EXPORT_OVERFLOW","run_id":null,"snapshot_id":null,"dataset_id":null,"instrument":null,"interval":null,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"run_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","snapshot_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","dataset_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","interval":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE","attempt":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}],"null_reasons":{"workbook_cells_before":"NOT_APPLICABLE_TO_SELECTED_FORMAT","cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT","workbook_cells_after":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}},"measured_shape":null,"export_verification_ref":null,"errors":[{"category":"CAPACITY","code":"REQUIRED_EXPORT_OVERFLOW","stage":"preflight","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted REQUIRED_EXPORT_OVERFLOW","run_id":null,"snapshot_id":null,"dataset_id":null,"instrument":null,"interval":null,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"run_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","snapshot_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","dataset_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","interval":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE","attempt":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}]}},
    {"name":"target","type":"per_target_outcome","value":{"target_id":"ILLUSTRATIVE_HOST_XLSX","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_XLSX"],"publication_status":"NOT_ATTEMPTED","readback_status":"NOT_ATTEMPTED","observed_generation":"UNKNOWN","previous_verification":"ABSENT","preservation_outcome":"NOT_APPLICABLE","previous_release_id":null,"previous_as_of":null,"previous_freshness":"UNKNOWN","baseline":null,"previous_evidence_ref":"ILLUSTRATIVE/previous/ABSENT","readback_evidence":null,"mutation_at":null,"readback_at":null,"errors":[]}},
    {"name":"release","type":"release_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","as_of":"2026-10-04T09:00:00Z","candidate_release_id":"ILLUSTRATIVE_CANDIDATE_ILLUSTRATIVE_RUN_A","status":"BLOCKED","stage_outcomes":{"collection":"COMPLETE","validation":"PASS","artifact":"NOT_RUN","preflight":"FAIL","export":"NOT_RUN","publication":"NOT_ATTEMPTED","readback":"NOT_ATTEMPTED"},"required_export_ids":["ILLUSTRATIVE_REQUIRED_CSV","ILLUSTRATIVE_REQUIRED_XLSX","ILLUSTRATIVE_REQUIRED_GOOGLE_SHEETS"],"failed_required_exports":["ILLUSTRATIVE_REQUIRED_CSV","ILLUSTRATIVE_REQUIRED_XLSX","ILLUSTRATIVE_REQUIRED_GOOGLE_SHEETS"],"per_target_outcomes":[{"target_id":"ILLUSTRATIVE_HOST_CSV","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_CSV"],"publication_status":"NOT_ATTEMPTED","readback_status":"NOT_ATTEMPTED","observed_generation":"UNKNOWN","previous_verification":"ABSENT","preservation_outcome":"NOT_APPLICABLE","previous_release_id":null,"previous_as_of":null,"previous_freshness":"UNKNOWN","baseline":null,"previous_evidence_ref":"ILLUSTRATIVE/previous/ABSENT","readback_evidence":null,"mutation_at":null,"readback_at":null,"errors":[]},{"target_id":"ILLUSTRATIVE_HOST_XLSX","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_XLSX"],"publication_status":"NOT_ATTEMPTED","readback_status":"NOT_ATTEMPTED","observed_generation":"UNKNOWN","previous_verification":"ABSENT","preservation_outcome":"NOT_APPLICABLE","previous_release_id":null,"previous_as_of":null,"previous_freshness":"UNKNOWN","baseline":null,"previous_evidence_ref":"ILLUSTRATIVE/previous/ABSENT","readback_evidence":null,"mutation_at":null,"readback_at":null,"errors":[]},{"target_id":"ILLUSTRATIVE_SHEETS","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_GOOGLE_SHEETS"],"publication_status":"NOT_ATTEMPTED","readback_status":"NOT_ATTEMPTED","observed_generation":"UNKNOWN","previous_verification":"ABSENT","preservation_outcome":"NOT_APPLICABLE","previous_release_id":null,"previous_as_of":null,"previous_freshness":"UNKNOWN","baseline":null,"previous_evidence_ref":"ILLUSTRATIVE/previous/ABSENT","readback_evidence":null,"mutation_at":null,"readback_at":null,"errors":[]}],"candidate_manifest_ref":"ILLUSTRATIVE/candidate-manifest","current_manifest_ref":null,"previous_manifest_ref":null,"published_as_of":null,"exit_status":1,"errors":[{"category":"CAPACITY","code":"REQUIRED_EXPORT_OVERFLOW","stage":"preflight","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted REQUIRED_EXPORT_OVERFLOW","run_id":null,"snapshot_id":null,"dataset_id":null,"instrument":null,"interval":null,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"run_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","snapshot_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","dataset_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","interval":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE","attempt":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}]}}
  ]
}
```

<!-- EXAMPLE E07 03_OVERFLOW_PREVIOUS_STATES -->
### E07 — Boundary+1 overflow; explicit previous UNVERIFIED quality, no mutation and no invented preservation readback.

```json
{
  "example_id": "E07",
  "family": "03_OVERFLOW_PREVIOUS_STATES",
  "illustrative": true,
  "description": "Boundary+1 overflow; explicit previous UNVERIFIED quality, no mutation and no invented preservation readback.",
  "expect": {"valid":true,"invariants":["1048576 data +1 header =1048577 rows","preflight FAIL","required failure nonzero","previous identity/cutoff unchanged","ABSENT requires explicit evidence, UNVERIFIED never upgraded"]},
  "objects": [
    {"name":"shape","type":"shape","value":{"data_rows":1048576,"data_columns":10,"header_rows":1,"index_columns":0}},
    {"name":"caps","type":"target_capabilities","value":{"capabilities_ref":"ILLUSTRATIVE/caps/xlsx","target_id":"ILLUSTRATIVE_HOST_XLSX","format":"xlsx","verification":"VERIFIED","row_limit":1048576,"column_limit":16384,"cell_limit":null,"grid":null,"limit_evidence_ref":"ILLUSTRATIVE/limits/xlsx","grid_evidence_ref":null,"verified_at":"2026-10-04T09:00:00Z","verification_scope":"DOCUMENTED_FORMAT","null_reasons":{"grid":"NOT_APPLICABLE_TO_SELECTED_FORMAT","cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}}},
    {"name":"assessment","type":"capacity_assessment","value":{"format":"xlsx","status":"FAIL","capabilities_ref":"ILLUSTRATIVE/caps/xlsx","source_shape":{"data_rows":1048576,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":1048577,"required_columns":10,"required_cells":10485770,"row_limit":1048576,"column_limit":16384,"cell_limit":null,"workbook_cells_before":null,"workbook_cells_after":null,"limit_evidence_ref":"ILLUSTRATIVE/limits/xlsx","grid_evidence_ref":null,"errors":[{"category":"CAPACITY","code":"REQUIRED_EXPORT_OVERFLOW","stage":"preflight","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted REQUIRED_EXPORT_OVERFLOW","run_id":null,"snapshot_id":null,"dataset_id":null,"instrument":null,"interval":null,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"run_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","snapshot_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","dataset_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","interval":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE","attempt":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}],"null_reasons":{"workbook_cells_before":"NOT_APPLICABLE_TO_SELECTED_FORMAT","cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT","workbook_cells_after":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}}},
    {"name":"artifact","type":"artifact_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_XLSX","format":"xlsx","status":"NOT_RUN","expected_shape":{"data_rows":1048576,"data_columns":10,"header_rows":1,"index_columns":0},"expected_schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14","expected_columns":["open","close","high","low","value","volume","begin","end","ticker","RSI14"],"relative_path":null,"actual_rows":null,"bytes":null,"sha256":null,"canonical_fingerprint":null,"verification_evidence_ref":null,"errors":[{"category":"CAPACITY","code":"REQUIRED_EXPORT_OVERFLOW","stage":"preflight","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted REQUIRED_EXPORT_OVERFLOW","run_id":null,"snapshot_id":null,"dataset_id":null,"instrument":null,"interval":null,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"run_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","snapshot_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","dataset_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","interval":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE","attempt":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}]}},
    {"name":"export","type":"export_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_XLSX","target_id":"ILLUSTRATIVE_HOST_XLSX","format":"xlsx","required":true,"preflight_status":"FAIL","export_status":"NOT_RUN","capacity_assessment":{"format":"xlsx","status":"FAIL","capabilities_ref":"ILLUSTRATIVE/caps/xlsx","source_shape":{"data_rows":1048576,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":1048577,"required_columns":10,"required_cells":10485770,"row_limit":1048576,"column_limit":16384,"cell_limit":null,"workbook_cells_before":null,"workbook_cells_after":null,"limit_evidence_ref":"ILLUSTRATIVE/limits/xlsx","grid_evidence_ref":null,"errors":[{"category":"CAPACITY","code":"REQUIRED_EXPORT_OVERFLOW","stage":"preflight","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted REQUIRED_EXPORT_OVERFLOW","run_id":null,"snapshot_id":null,"dataset_id":null,"instrument":null,"interval":null,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"run_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","snapshot_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","dataset_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","interval":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE","attempt":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}],"null_reasons":{"workbook_cells_before":"NOT_APPLICABLE_TO_SELECTED_FORMAT","cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT","workbook_cells_after":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}},"measured_shape":null,"export_verification_ref":null,"errors":[{"category":"CAPACITY","code":"REQUIRED_EXPORT_OVERFLOW","stage":"preflight","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted REQUIRED_EXPORT_OVERFLOW","run_id":null,"snapshot_id":null,"dataset_id":null,"instrument":null,"interval":null,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"run_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","snapshot_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","dataset_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","interval":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE","attempt":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}]}},
    {"name":"target","type":"per_target_outcome","value":{"target_id":"ILLUSTRATIVE_HOST_XLSX","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_XLSX"],"publication_status":"NOT_ATTEMPTED","readback_status":"NOT_ATTEMPTED","observed_generation":"UNKNOWN","previous_verification":"UNVERIFIED","preservation_outcome":"NOT_TOUCHED","previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_freshness":"STALE","baseline":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"previous_evidence_ref":"ILLUSTRATIVE/previous/UNVERIFIED","readback_evidence":null,"mutation_at":null,"readback_at":null,"errors":[]}},
    {"name":"release","type":"release_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","as_of":"2026-10-04T09:00:00Z","candidate_release_id":"ILLUSTRATIVE_CANDIDATE_ILLUSTRATIVE_RUN_A","status":"BLOCKED","stage_outcomes":{"collection":"COMPLETE","validation":"PASS","artifact":"NOT_RUN","preflight":"FAIL","export":"NOT_RUN","publication":"NOT_ATTEMPTED","readback":"NOT_ATTEMPTED"},"required_export_ids":["ILLUSTRATIVE_REQUIRED_CSV","ILLUSTRATIVE_REQUIRED_XLSX","ILLUSTRATIVE_REQUIRED_GOOGLE_SHEETS"],"failed_required_exports":["ILLUSTRATIVE_REQUIRED_CSV","ILLUSTRATIVE_REQUIRED_XLSX","ILLUSTRATIVE_REQUIRED_GOOGLE_SHEETS"],"per_target_outcomes":[{"target_id":"ILLUSTRATIVE_HOST_CSV","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_CSV"],"publication_status":"NOT_ATTEMPTED","readback_status":"NOT_ATTEMPTED","observed_generation":"UNKNOWN","previous_verification":"UNVERIFIED","preservation_outcome":"NOT_TOUCHED","previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_freshness":"STALE","baseline":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"previous_evidence_ref":"ILLUSTRATIVE/previous/UNVERIFIED","readback_evidence":null,"mutation_at":null,"readback_at":null,"errors":[]},{"target_id":"ILLUSTRATIVE_HOST_XLSX","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_XLSX"],"publication_status":"NOT_ATTEMPTED","readback_status":"NOT_ATTEMPTED","observed_generation":"UNKNOWN","previous_verification":"UNVERIFIED","preservation_outcome":"NOT_TOUCHED","previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_freshness":"STALE","baseline":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"previous_evidence_ref":"ILLUSTRATIVE/previous/UNVERIFIED","readback_evidence":null,"mutation_at":null,"readback_at":null,"errors":[]},{"target_id":"ILLUSTRATIVE_SHEETS","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_GOOGLE_SHEETS"],"publication_status":"NOT_ATTEMPTED","readback_status":"NOT_ATTEMPTED","observed_generation":"UNKNOWN","previous_verification":"UNVERIFIED","preservation_outcome":"NOT_TOUCHED","previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_freshness":"STALE","baseline":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"previous_evidence_ref":"ILLUSTRATIVE/previous/UNVERIFIED","readback_evidence":null,"mutation_at":null,"readback_at":null,"errors":[]}],"candidate_manifest_ref":"ILLUSTRATIVE/candidate-manifest","current_manifest_ref":"ILLUSTRATIVE/previous-manifest","previous_manifest_ref":"ILLUSTRATIVE/previous-manifest","published_as_of":"2026-10-03T09:00:00Z","exit_status":1,"errors":[{"category":"CAPACITY","code":"REQUIRED_EXPORT_OVERFLOW","stage":"preflight","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted REQUIRED_EXPORT_OVERFLOW","run_id":null,"snapshot_id":null,"dataset_id":null,"instrument":null,"interval":null,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"run_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","snapshot_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","dataset_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","interval":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE","attempt":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}]}}
  ]
}
```






<!-- EXAMPLE E08 04_NOT_TOUCHED_AND_READBACK -->
### E08 — Before mutation NOT_TOUCHED is not VERIFIED readback; after timeout matching baseline proves preservation only.

```json
{
  "example_id": "E08",
  "family": "04_NOT_TOUCHED_AND_READBACK",
  "illustrative": true,
  "description": "Before mutation NOT_TOUCHED is not VERIFIED readback; after timeout matching baseline proves preservation only.",
  "expect": {"valid":true,"invariants":["untouched mutation_at null and readback NOT_ATTEMPTED","after mutation NOT_TOUCHED forbidden","matching baseline means PRESERVED_VERIFIED, not overall candidate success"]},
  "objects": [
    {"name":"untouched","type":"per_target_outcome","value":{"target_id":"ILLUSTRATIVE_HOST_CSV","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_CSV"],"publication_status":"NOT_ATTEMPTED","readback_status":"NOT_ATTEMPTED","observed_generation":"UNKNOWN","previous_verification":"VERIFIED","preservation_outcome":"NOT_TOUCHED","previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_freshness":"STALE","baseline":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"previous_evidence_ref":"ILLUSTRATIVE/previous/VERIFIED","readback_evidence":null,"mutation_at":null,"readback_at":null,"errors":[]}},
    {"name":"mutated_then_read_back","type":"per_target_outcome","value":{"target_id":"ILLUSTRATIVE_HOST_CSV","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_CSV"],"publication_status":"UNKNOWN","readback_status":"VERIFIED","observed_generation":"PREVIOUS","previous_verification":"VERIFIED","preservation_outcome":"PRESERVED_VERIFIED","previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_freshness":"STALE","baseline":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"previous_evidence_ref":"ILLUSTRATIVE/previous/VERIFIED","readback_evidence":{"expected":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"observed":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"evidence_ref":"ILLUSTRATIVE/matching-previous-readback/csv","scope":"FULL_REQUIRED_TARGET"},"mutation_at":"2026-10-04T09:00:00Z","readback_at":"2026-10-04T09:05:00Z","errors":[{"category":"TIMEOUT","code":"MUTATING_TIMEOUT","stage":"export","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted MUTATING_TIMEOUT","run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","instrument":null,"interval":24,"page":null,"file":null,"artifact_id":null,"target_id":"ILLUSTRATIVE_HOST_CSV","endpoint":null,"attempt":1,"exception_type":"TimeoutError","null_reasons":{"instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}]}}
  ]
}
```

<!-- EXAMPLE E09 05_PARTIAL_REMOTE_AND_UNKNOWN -->
### E09 — CSV candidate and other targets conclusively previous: no coherent new release.

```json
{
  "example_id": "E09",
  "family": "05_PARTIAL_REMOTE_AND_UNKNOWN",
  "illustrative": true,
  "description": "CSV candidate and other targets conclusively previous: no coherent new release.",
  "expect": {"valid":true,"invariants":["all target content comparisons verified","required generations differ","PARTIAL_REMOTE/nonzero, previous dates unchanged"]},
  "objects": [
    {"name":"release","type":"release_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","as_of":"2026-10-04T09:00:00Z","candidate_release_id":"ILLUSTRATIVE_CANDIDATE_ILLUSTRATIVE_RUN_A","status":"PARTIAL_REMOTE","stage_outcomes":{"collection":"COMPLETE","validation":"PASS","artifact":"VERIFIED","preflight":"PASS","export":"SUCCEEDED","publication":"ACKNOWLEDGED","readback":"MISMATCH"},"required_export_ids":["ILLUSTRATIVE_REQUIRED_CSV","ILLUSTRATIVE_REQUIRED_XLSX","ILLUSTRATIVE_REQUIRED_GOOGLE_SHEETS"],"failed_required_exports":["ILLUSTRATIVE_REQUIRED_CSV","ILLUSTRATIVE_REQUIRED_XLSX","ILLUSTRATIVE_REQUIRED_GOOGLE_SHEETS"],"per_target_outcomes":[{"target_id":"ILLUSTRATIVE_HOST_CSV","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_CSV"],"publication_status":"ACKNOWLEDGED","readback_status":"VERIFIED","observed_generation":"CANDIDATE","previous_verification":"VERIFIED","preservation_outcome":"CHANGED_VERIFIED","previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_freshness":"STALE","baseline":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"previous_evidence_ref":"ILLUSTRATIVE/previous/VERIFIED","readback_evidence":{"expected":{"basis":"LOGICAL_ROWS_SHA256","value":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"observed":{"basis":"LOGICAL_ROWS_SHA256","value":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"evidence_ref":"ILLUSTRATIVE/candidate-readback/csv","scope":"FULL_REQUIRED_TARGET"},"mutation_at":"2026-10-04T09:00:00Z","readback_at":"2026-10-04T09:05:00Z","errors":[]},{"target_id":"ILLUSTRATIVE_HOST_XLSX","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_XLSX"],"publication_status":"ACKNOWLEDGED","readback_status":"VERIFIED","observed_generation":"PREVIOUS","previous_verification":"VERIFIED","preservation_outcome":"PRESERVED_VERIFIED","previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_freshness":"STALE","baseline":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"previous_evidence_ref":"ILLUSTRATIVE/previous/VERIFIED","readback_evidence":{"expected":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"observed":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"evidence_ref":"ILLUSTRATIVE/matching-previous-readback/xlsx","scope":"FULL_REQUIRED_TARGET"},"mutation_at":"2026-10-04T09:00:00Z","readback_at":"2026-10-04T09:05:00Z","errors":[]},{"target_id":"ILLUSTRATIVE_SHEETS","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_GOOGLE_SHEETS"],"publication_status":"ACKNOWLEDGED","readback_status":"VERIFIED","observed_generation":"PREVIOUS","previous_verification":"VERIFIED","preservation_outcome":"PRESERVED_VERIFIED","previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_freshness":"STALE","baseline":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"previous_evidence_ref":"ILLUSTRATIVE/previous/VERIFIED","readback_evidence":{"expected":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"observed":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"evidence_ref":"ILLUSTRATIVE/matching-previous-readback/google_sheets","scope":"FULL_REQUIRED_TARGET"},"mutation_at":"2026-10-04T09:00:00Z","readback_at":"2026-10-04T09:05:00Z","errors":[]}],"candidate_manifest_ref":"ILLUSTRATIVE/candidate-manifest","current_manifest_ref":"ILLUSTRATIVE/previous-manifest","previous_manifest_ref":"ILLUSTRATIVE/previous-manifest","published_as_of":"2026-10-03T09:00:00Z","exit_status":1,"errors":[]}}
  ]
}
```

<!-- EXAMPLE E10 05_PARTIAL_REMOTE_AND_UNKNOWN -->
### E10 — After possible mutation one required target remains unidentified; another's verification cannot resolve it.

```json
{
  "example_id": "E10",
  "family": "05_PARTIAL_REMOTE_AND_UNKNOWN",
  "illustrative": true,
  "description": "After possible mutation one required target remains unidentified; another's verification cannot resolve it.",
  "expect": {"valid":true,"invariants":["unknown mutated target has preservation UNKNOWN","REMOTE_UNKNOWN/nonzero","no global preservation claim"]},
  "objects": [
    {"name":"release","type":"release_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","as_of":"2026-10-04T09:00:00Z","candidate_release_id":"ILLUSTRATIVE_CANDIDATE_ILLUSTRATIVE_RUN_A","status":"REMOTE_UNKNOWN","stage_outcomes":{"collection":"COMPLETE","validation":"PASS","artifact":"VERIFIED","preflight":"PASS","export":"SUCCEEDED","publication":"UNKNOWN","readback":"UNKNOWN"},"required_export_ids":["ILLUSTRATIVE_REQUIRED_CSV","ILLUSTRATIVE_REQUIRED_XLSX","ILLUSTRATIVE_REQUIRED_GOOGLE_SHEETS"],"failed_required_exports":["ILLUSTRATIVE_REQUIRED_CSV","ILLUSTRATIVE_REQUIRED_XLSX","ILLUSTRATIVE_REQUIRED_GOOGLE_SHEETS"],"per_target_outcomes":[{"target_id":"ILLUSTRATIVE_HOST_CSV","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_CSV"],"publication_status":"ACKNOWLEDGED","readback_status":"VERIFIED","observed_generation":"CANDIDATE","previous_verification":"VERIFIED","preservation_outcome":"CHANGED_VERIFIED","previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_freshness":"STALE","baseline":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"previous_evidence_ref":"ILLUSTRATIVE/previous/VERIFIED","readback_evidence":{"expected":{"basis":"LOGICAL_ROWS_SHA256","value":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"observed":{"basis":"LOGICAL_ROWS_SHA256","value":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"evidence_ref":"ILLUSTRATIVE/candidate-readback/csv","scope":"FULL_REQUIRED_TARGET"},"mutation_at":"2026-10-04T09:00:00Z","readback_at":"2026-10-04T09:05:00Z","errors":[]},{"target_id":"ILLUSTRATIVE_HOST_XLSX","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_XLSX"],"publication_status":"UNKNOWN","readback_status":"UNKNOWN","observed_generation":"UNKNOWN","previous_verification":"VERIFIED","preservation_outcome":"UNKNOWN","previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_freshness":"STALE","baseline":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"previous_evidence_ref":"ILLUSTRATIVE/previous/VERIFIED","readback_evidence":null,"mutation_at":"2026-10-04T09:00:00Z","readback_at":null,"errors":[{"category":"TIMEOUT","code":"MUTATING_TIMEOUT","stage":"export","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted MUTATING_TIMEOUT","run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","instrument":null,"interval":24,"page":null,"file":null,"artifact_id":null,"target_id":"ILLUSTRATIVE_HOST_XLSX","endpoint":null,"attempt":1,"exception_type":"TimeoutError","null_reasons":{"instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}]},{"target_id":"ILLUSTRATIVE_SHEETS","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_GOOGLE_SHEETS"],"publication_status":"NOT_ATTEMPTED","readback_status":"NOT_ATTEMPTED","observed_generation":"UNKNOWN","previous_verification":"VERIFIED","preservation_outcome":"NOT_TOUCHED","previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_freshness":"STALE","baseline":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"previous_evidence_ref":"ILLUSTRATIVE/previous/VERIFIED","readback_evidence":null,"mutation_at":null,"readback_at":null,"errors":[]}],"candidate_manifest_ref":"ILLUSTRATIVE/candidate-manifest","current_manifest_ref":"ILLUSTRATIVE/previous-manifest","previous_manifest_ref":"ILLUSTRATIVE/previous-manifest","published_as_of":"2026-10-03T09:00:00Z","exit_status":1,"errors":[{"category":"TIMEOUT","code":"MUTATING_TIMEOUT","stage":"export","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted MUTATING_TIMEOUT","run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","instrument":null,"interval":24,"page":null,"file":null,"artifact_id":null,"target_id":"ILLUSTRATIVE_HOST_XLSX","endpoint":null,"attempt":1,"exception_type":"TimeoutError","null_reasons":{"instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}]}}
  ]
}
```

<!-- EXAMPLE E11 06_PREFLIGHT_VS_ACTUAL -->
### E11 — Identical preflight PASS can precede actual NOT_RUN or FAILED preparation.

```json
{
  "example_id": "E11",
  "family": "06_PREFLIGHT_VS_ACTUAL",
  "illustrative": true,
  "description": "Identical preflight PASS can precede actual NOT_RUN or FAILED preparation.",
  "expect": {"valid":true,"invariants":["capacity assessment PASS is independent","NOT_RUN/FAILED have null measured output","no publication/readback success inferred"]},
  "objects": [
    {"name":"not_run","type":"export_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_XLSX","target_id":"ILLUSTRATIVE_HOST_XLSX","format":"xlsx","required":true,"preflight_status":"PASS","export_status":"NOT_RUN","capacity_assessment":{"format":"xlsx","status":"PASS","capabilities_ref":"ILLUSTRATIVE/caps/xlsx","source_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":4,"required_columns":10,"required_cells":40,"row_limit":1048576,"column_limit":16384,"cell_limit":null,"workbook_cells_before":null,"workbook_cells_after":null,"limit_evidence_ref":"ILLUSTRATIVE/limits/xlsx","grid_evidence_ref":null,"errors":[],"null_reasons":{"workbook_cells_before":"NOT_APPLICABLE_TO_SELECTED_FORMAT","cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT","workbook_cells_after":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}},"measured_shape":null,"export_verification_ref":null,"errors":[]}},
    {"name":"failed","type":"export_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_XLSX","target_id":"ILLUSTRATIVE_HOST_XLSX","format":"xlsx","required":true,"preflight_status":"PASS","export_status":"FAILED","capacity_assessment":{"format":"xlsx","status":"PASS","capabilities_ref":"ILLUSTRATIVE/caps/xlsx","source_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":4,"required_columns":10,"required_cells":40,"row_limit":1048576,"column_limit":16384,"cell_limit":null,"workbook_cells_before":null,"workbook_cells_after":null,"limit_evidence_ref":"ILLUSTRATIVE/limits/xlsx","grid_evidence_ref":null,"errors":[],"null_reasons":{"workbook_cells_before":"NOT_APPLICABLE_TO_SELECTED_FORMAT","cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT","workbook_cells_after":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}},"measured_shape":null,"export_verification_ref":null,"errors":[{"category":"DISK_FULL","code":"CANDIDATE_WRITE_DISK_FULL","stage":"export","retryable":false,"final":true,"message":"ILLUSTRATIVE insufficient candidate storage","run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","instrument":null,"interval":24,"page":null,"file":null,"artifact_id":null,"target_id":"ILLUSTRATIVE_HOST_XLSX","endpoint":null,"attempt":1,"exception_type":"OSError","null_reasons":{"instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}]}}
  ]
}
```

<!-- EXAMPLE E12 07_CAPABILITIES_NOT_VERIFIED -->
### E12 — Documented provider number is not effective-target confirmation; missing numeric/grid/evidence blocks.

```json
{
  "example_id": "E12",
  "family": "07_CAPABILITIES_NOT_VERIFIED",
  "illustrative": true,
  "description": "Documented provider number is not effective-target confirmation; missing numeric/grid/evidence blocks.",
  "expect": {"valid":true,"invariants":["no assumed production 20m","NOT_VERIFIED not PASS nor proven overflow","actual export NOT_RUN"]},
  "objects": [
    {"name":"caps","type":"target_capabilities","value":{"capabilities_ref":"ILLUSTRATIVE/caps/google_sheets","target_id":"ILLUSTRATIVE_SHEETS","format":"google_sheets","verification":"NOT_VERIFIED","row_limit":null,"column_limit":null,"cell_limit":null,"grid":null,"limit_evidence_ref":null,"grid_evidence_ref":null,"verified_at":null,"verification_scope":"UNVERIFIED","null_reasons":{"cell_limit":"NOT_VERIFIED_OR_NOT_MEASURED","verified_at":"NOT_VERIFIED_OR_NOT_MEASURED","limit_evidence_ref":"NOT_VERIFIED_OR_NOT_MEASURED","row_limit":"NOT_VERIFIED_OR_NOT_MEASURED","column_limit":"NOT_VERIFIED_OR_NOT_MEASURED","grid":"NOT_VERIFIED_OR_NOT_MEASURED","grid_evidence_ref":"NOT_VERIFIED_OR_NOT_MEASURED"}}},
    {"name":"assessment","type":"capacity_assessment","value":{"format":"google_sheets","status":"NOT_VERIFIED","capabilities_ref":"ILLUSTRATIVE/caps/google_sheets","source_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":4,"required_columns":10,"required_cells":40,"row_limit":null,"column_limit":null,"cell_limit":null,"workbook_cells_before":null,"workbook_cells_after":null,"limit_evidence_ref":null,"grid_evidence_ref":null,"errors":[{"category":"CAPACITY_NOT_VERIFIED","code":"EFFECTIVE_CAPABILITY_NOT_VERIFIED","stage":"preflight","retryable":false,"final":true,"message":"ILLUSTRATIVE effective limit/grid/evidence unavailable","run_id":null,"snapshot_id":null,"dataset_id":null,"instrument":null,"interval":null,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"run_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","snapshot_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","dataset_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","interval":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE","attempt":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}],"null_reasons":{"workbook_cells_before":"NOT_VERIFIED_OR_NOT_MEASURED","cell_limit":"NOT_VERIFIED_OR_NOT_MEASURED","limit_evidence_ref":"NOT_VERIFIED_OR_NOT_MEASURED","workbook_cells_after":"NOT_VERIFIED_OR_NOT_MEASURED","row_limit":"NOT_VERIFIED_OR_NOT_MEASURED","column_limit":"NOT_VERIFIED_OR_NOT_MEASURED","grid_evidence_ref":"NOT_VERIFIED_OR_NOT_MEASURED"}}},
    {"name":"export","type":"export_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_GOOGLE_SHEETS","target_id":"ILLUSTRATIVE_SHEETS","format":"google_sheets","required":true,"preflight_status":"NOT_VERIFIED","export_status":"NOT_RUN","capacity_assessment":{"format":"google_sheets","status":"NOT_VERIFIED","capabilities_ref":"ILLUSTRATIVE/caps/google_sheets","source_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":4,"required_columns":10,"required_cells":40,"row_limit":null,"column_limit":null,"cell_limit":null,"workbook_cells_before":null,"workbook_cells_after":null,"limit_evidence_ref":null,"grid_evidence_ref":null,"errors":[{"category":"CAPACITY_NOT_VERIFIED","code":"EFFECTIVE_CAPABILITY_NOT_VERIFIED","stage":"preflight","retryable":false,"final":true,"message":"ILLUSTRATIVE effective limit/grid/evidence unavailable","run_id":null,"snapshot_id":null,"dataset_id":null,"instrument":null,"interval":null,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"run_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","snapshot_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","dataset_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","interval":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE","attempt":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}],"null_reasons":{"workbook_cells_before":"NOT_VERIFIED_OR_NOT_MEASURED","cell_limit":"NOT_VERIFIED_OR_NOT_MEASURED","limit_evidence_ref":"NOT_VERIFIED_OR_NOT_MEASURED","workbook_cells_after":"NOT_VERIFIED_OR_NOT_MEASURED","row_limit":"NOT_VERIFIED_OR_NOT_MEASURED","column_limit":"NOT_VERIFIED_OR_NOT_MEASURED","grid_evidence_ref":"NOT_VERIFIED_OR_NOT_MEASURED"}},"measured_shape":null,"export_verification_ref":null,"errors":[{"category":"CAPACITY_NOT_VERIFIED","code":"EFFECTIVE_CAPABILITY_NOT_VERIFIED","stage":"preflight","retryable":false,"final":true,"message":"ILLUSTRATIVE effective limit/grid/evidence unavailable","run_id":null,"snapshot_id":null,"dataset_id":null,"instrument":null,"interval":null,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"run_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","snapshot_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","dataset_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","interval":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE","attempt":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}]}}
  ]
}
```

<!-- EXAMPLE E13 08_TWO_RUNS_PURE_CORRELATION -->
### E13 — Same spec/shape/caps produce equal pure assessments; explicit callers bind distinct run/snapshot IDs.

```json
{
  "example_id": "E13",
  "family": "08_TWO_RUNS_PURE_CORRELATION",
  "illustrative": true,
  "description": "Same spec/shape/caps produce equal pure assessments; explicit callers bind distinct run/snapshot IDs.",
  "expect": {"valid":true,"invariants":["assessment_A equals assessment_B","pure assessments have no run/snapshot/artifact/target fields","run_B results carry run_B/snapshot_B","cross-snapshot artifact rejected"]},
  "negative_case": {"input_contract":"artifact_intent","input":{"dataset_id":"10years_data_1d_interval","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_CSV","format":"csv","candidate_relative_path":"10years_data_1d_interval.csv"},"consumer_context":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_B","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_B","as_of":"2026-10-04T09:00:00Z","producer_sha":"ILLUSTRATIVE_SHA","environment":"ILLUSTRATIVE_OFFLINE","package":"ILLUSTRATIVE_F0","roots":{"data":"ILLUSTRATIVE/data","log":"ILLUSTRATIVE/log","temp":"ILLUSTRATIVE/temp","evidence":"ILLUSTRATIVE/evidence"},"required_targets":["ILLUSTRATIVE_HOST_CSV","ILLUSTRATIVE_HOST_XLSX","ILLUSTRATIVE_SHEETS"]},"expected_error":"SNAPSHOT_ID_MISMATCH"},
  "objects": [
    {"name":"spec","type":"spec","value":{"contract_version":1,"dataset_id":"10years_data_1d_interval","stem":"10years_data_1d_interval","horizon":"10years","interval":24,"schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14","columns":[{"name":"open","logical_type":"number","nullable":false,"unit":null,"unit_status":"UNKNOWN","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"close","logical_type":"number","nullable":false,"unit":null,"unit_status":"UNKNOWN","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"high","logical_type":"number","nullable":false,"unit":null,"unit_status":"UNKNOWN","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"low","logical_type":"number","nullable":false,"unit":null,"unit_status":"UNKNOWN","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"value","logical_type":"number","nullable":false,"unit":null,"unit_status":"UNKNOWN","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"volume","logical_type":"number","nullable":false,"unit":null,"unit_status":"UNKNOWN","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"begin","logical_type":"exchange_timestamp","nullable":false,"unit":null,"unit_status":"NOT_APPLICABLE","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"end","logical_type":"exchange_timestamp","nullable":false,"unit":null,"unit_status":"NOT_APPLICABLE","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"ticker","logical_type":"string","nullable":false,"unit":null,"unit_status":"NOT_APPLICABLE","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"},{"name":"RSI14","logical_type":"number","nullable":true,"unit":null,"unit_status":"UNKNOWN","evidence_ref":"ILLUSTRATIVE/mini-schema","null_reason":"ILLUSTRATIVE miniature descriptor; production units/missing semantics not asserted"}],"key_fields":["engine","market","source_scope","board","raw_instrument_id","interval","begin"],"sort_fields":["engine","market","source_scope","board","raw_instrument_id","interval","begin"],"filenames":{"csv":"10years_data_1d_interval.csv","xlsx":"10years_data_1d_interval.xlsx"},"required_exports":[{"export_id":"ILLUSTRATIVE_REQUIRED_CSV","target_id":"ILLUSTRATIVE_HOST_CSV","format":"csv","required":true,"source_dataset_id":"10years_data_1d_interval","source_artifact":"10years_data_1d_interval.csv","interface":{"columns":["open","close","high","low","value","volume","begin","end","ticker","RSI14"],"header_rows":1,"index_columns":0,"anchor":null,"nan_token":null,"configuration_ref":null}},{"export_id":"ILLUSTRATIVE_REQUIRED_XLSX","target_id":"ILLUSTRATIVE_HOST_XLSX","format":"xlsx","required":true,"source_dataset_id":"10years_data_1d_interval","source_artifact":"10years_data_1d_interval.xlsx","interface":{"columns":["open","close","high","low","value","volume","begin","end","ticker","RSI14"],"header_rows":1,"index_columns":0,"anchor":null,"nan_token":null,"configuration_ref":null}},{"export_id":"ILLUSTRATIVE_REQUIRED_GOOGLE_SHEETS","target_id":"ILLUSTRATIVE_SHEETS","format":"google_sheets","required":true,"source_dataset_id":"10years_data_1d_interval","source_artifact":"ILLUSTRATIVE_CANONICAL_GOOGLE_PROJECTION","interface":{"columns":["open","close","high","low","value","volume","begin","end","ticker","RSI14"],"header_rows":1,"index_columns":0,"anchor":"A1","nan_token":"NaN","configuration_ref":"ILLUSTRATIVE/retained-production-sheet-alias"}}],"empty_policy":"REQUIRE_NONEMPTY","notes":"ILLUSTRATIVE reduced raw9+RSI14 schema, not a replacement for full production enriched schema."}},
    {"name":"shape","type":"shape","value":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0}},
    {"name":"caps","type":"target_capabilities","value":{"capabilities_ref":"ILLUSTRATIVE/caps/csv","target_id":"ILLUSTRATIVE_HOST_CSV","format":"csv","verification":"NOT_APPLICABLE","row_limit":null,"column_limit":null,"cell_limit":null,"grid":null,"limit_evidence_ref":null,"grid_evidence_ref":null,"verified_at":null,"verification_scope":"NOT_APPLICABLE","null_reasons":{"cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","verified_at":"NOT_APPLICABLE_TO_SELECTED_FORMAT","limit_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT","row_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","column_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}}},
    {"name":"run_A","type":"run_context","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","as_of":"2026-10-04T09:00:00Z","producer_sha":"ILLUSTRATIVE_SHA","environment":"ILLUSTRATIVE_OFFLINE","package":"ILLUSTRATIVE_F0","roots":{"data":"ILLUSTRATIVE/data","log":"ILLUSTRATIVE/log","temp":"ILLUSTRATIVE/temp","evidence":"ILLUSTRATIVE/evidence"},"required_targets":["ILLUSTRATIVE_HOST_CSV","ILLUSTRATIVE_HOST_XLSX","ILLUSTRATIVE_SHEETS"]}},
    {"name":"assessment_A","type":"capacity_assessment","value":{"format":"csv","status":"PASS","capabilities_ref":"ILLUSTRATIVE/caps/csv","source_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":4,"required_columns":10,"required_cells":40,"row_limit":null,"column_limit":null,"cell_limit":null,"workbook_cells_before":null,"workbook_cells_after":null,"limit_evidence_ref":null,"grid_evidence_ref":null,"errors":[],"null_reasons":{"workbook_cells_before":"NOT_APPLICABLE_TO_SELECTED_FORMAT","cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","limit_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT","workbook_cells_after":"NOT_APPLICABLE_TO_SELECTED_FORMAT","row_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","column_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}}},
    {"name":"export_A","type":"export_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_CSV","target_id":"ILLUSTRATIVE_HOST_CSV","format":"csv","required":true,"preflight_status":"PASS","export_status":"SUCCEEDED","capacity_assessment":{"format":"csv","status":"PASS","capabilities_ref":"ILLUSTRATIVE/caps/csv","source_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":4,"required_columns":10,"required_cells":40,"row_limit":null,"column_limit":null,"cell_limit":null,"workbook_cells_before":null,"workbook_cells_after":null,"limit_evidence_ref":null,"grid_evidence_ref":null,"errors":[],"null_reasons":{"workbook_cells_before":"NOT_APPLICABLE_TO_SELECTED_FORMAT","cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","limit_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT","workbook_cells_after":"NOT_APPLICABLE_TO_SELECTED_FORMAT","row_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","column_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}},"measured_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"export_verification_ref":"ILLUSTRATIVE/prepared/csv","errors":[]}},
    {"name":"run_B","type":"run_context","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_B","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_B","as_of":"2026-10-04T09:00:00Z","producer_sha":"ILLUSTRATIVE_SHA","environment":"ILLUSTRATIVE_OFFLINE","package":"ILLUSTRATIVE_F0","roots":{"data":"ILLUSTRATIVE/data","log":"ILLUSTRATIVE/log","temp":"ILLUSTRATIVE/temp","evidence":"ILLUSTRATIVE/evidence"},"required_targets":["ILLUSTRATIVE_HOST_CSV","ILLUSTRATIVE_HOST_XLSX","ILLUSTRATIVE_SHEETS"]}},
    {"name":"assessment_B","type":"capacity_assessment","value":{"format":"csv","status":"PASS","capabilities_ref":"ILLUSTRATIVE/caps/csv","source_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":4,"required_columns":10,"required_cells":40,"row_limit":null,"column_limit":null,"cell_limit":null,"workbook_cells_before":null,"workbook_cells_after":null,"limit_evidence_ref":null,"grid_evidence_ref":null,"errors":[],"null_reasons":{"workbook_cells_before":"NOT_APPLICABLE_TO_SELECTED_FORMAT","cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","limit_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT","workbook_cells_after":"NOT_APPLICABLE_TO_SELECTED_FORMAT","row_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","column_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}}},
    {"name":"intent_B","type":"export_intent","value":{"dataset_id":"10years_data_1d_interval","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_B","artifact_id":"ILLUSTRATIVE_SNAPSHOT_B_CSV","target_id":"ILLUSTRATIVE_HOST_CSV","format":"csv","required":true,"capabilities_ref":"ILLUSTRATIVE/caps/csv"}},
    {"name":"export_B","type":"export_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_B","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_B","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","artifact_id":"ILLUSTRATIVE_SNAPSHOT_B_CSV","target_id":"ILLUSTRATIVE_HOST_CSV","format":"csv","required":true,"preflight_status":"PASS","export_status":"SUCCEEDED","capacity_assessment":{"format":"csv","status":"PASS","capabilities_ref":"ILLUSTRATIVE/caps/csv","source_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":4,"required_columns":10,"required_cells":40,"row_limit":null,"column_limit":null,"cell_limit":null,"workbook_cells_before":null,"workbook_cells_after":null,"limit_evidence_ref":null,"grid_evidence_ref":null,"errors":[],"null_reasons":{"workbook_cells_before":"NOT_APPLICABLE_TO_SELECTED_FORMAT","cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","limit_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT","workbook_cells_after":"NOT_APPLICABLE_TO_SELECTED_FORMAT","row_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","column_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}},"measured_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"export_verification_ref":"ILLUSTRATIVE/prepared/csv","errors":[]}}
  ]
}
```

<!-- EXAMPLE E14 09_ONE_FORMAT_ONE_ARTIFACT -->
### E14 — CSV prepared from shared snapshot; mandatory XLSX failure prevents coherent release and all current mutations.

```json
{
  "example_id": "E14",
  "family": "09_ONE_FORMAT_ONE_ARTIFACT",
  "illustrative": true,
  "description": "CSV prepared from shared snapshot; mandatory XLSX failure prevents coherent release and all current mutations.",
  "expect": {"valid":true,"invariants":["csv/xlsx IDs and paths differ","snapshot identical","CSV success != overall success","mandatory XLSX failure blocks, nonzero"]},
  "objects": [
    {"name":"csv_intent","type":"artifact_intent","value":{"dataset_id":"10years_data_1d_interval","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_CSV","format":"csv","candidate_relative_path":"10years_data_1d_interval.csv"}},
    {"name":"csv_artifact","type":"artifact_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_CSV","format":"csv","status":"VERIFIED","expected_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"expected_schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14","expected_columns":["open","close","high","low","value","volume","begin","end","ticker","RSI14"],"relative_path":"10years_data_1d_interval.csv","actual_rows":3,"bytes":1234,"sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","canonical_fingerprint":{"basis":"LOGICAL_ROWS_SHA256","value":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"verification_evidence_ref":"ILLUSTRATIVE/roundtrip/csv","errors":[]}},
    {"name":"csv_export","type":"export_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_CSV","target_id":"ILLUSTRATIVE_HOST_CSV","format":"csv","required":true,"preflight_status":"PASS","export_status":"SUCCEEDED","capacity_assessment":{"format":"csv","status":"PASS","capabilities_ref":"ILLUSTRATIVE/caps/csv","source_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":4,"required_columns":10,"required_cells":40,"row_limit":null,"column_limit":null,"cell_limit":null,"workbook_cells_before":null,"workbook_cells_after":null,"limit_evidence_ref":null,"grid_evidence_ref":null,"errors":[],"null_reasons":{"workbook_cells_before":"NOT_APPLICABLE_TO_SELECTED_FORMAT","cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","limit_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT","workbook_cells_after":"NOT_APPLICABLE_TO_SELECTED_FORMAT","row_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","column_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}},"measured_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"export_verification_ref":"ILLUSTRATIVE/prepared/csv","errors":[]}},
    {"name":"xlsx_intent","type":"artifact_intent","value":{"dataset_id":"10years_data_1d_interval","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_XLSX","format":"xlsx","candidate_relative_path":"10years_data_1d_interval.xlsx"}},
    {"name":"xlsx_artifact","type":"artifact_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_XLSX","format":"xlsx","status":"FAILED","expected_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"expected_schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14","expected_columns":["open","close","high","low","value","volume","begin","end","ticker","RSI14"],"relative_path":"10years_data_1d_interval.xlsx","actual_rows":null,"bytes":null,"sha256":null,"canonical_fingerprint":null,"verification_evidence_ref":null,"errors":[{"category":"DISK_FULL","code":"CANDIDATE_WRITE_DISK_FULL","stage":"export","retryable":false,"final":true,"message":"ILLUSTRATIVE insufficient candidate storage","run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","instrument":null,"interval":24,"page":null,"file":null,"artifact_id":null,"target_id":"ILLUSTRATIVE_HOST_XLSX","endpoint":null,"attempt":1,"exception_type":"OSError","null_reasons":{"instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}]}},
    {"name":"xlsx_export","type":"export_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_XLSX","target_id":"ILLUSTRATIVE_HOST_XLSX","format":"xlsx","required":true,"preflight_status":"PASS","export_status":"FAILED","capacity_assessment":{"format":"xlsx","status":"PASS","capabilities_ref":"ILLUSTRATIVE/caps/xlsx","source_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":4,"required_columns":10,"required_cells":40,"row_limit":1048576,"column_limit":16384,"cell_limit":null,"workbook_cells_before":null,"workbook_cells_after":null,"limit_evidence_ref":"ILLUSTRATIVE/limits/xlsx","grid_evidence_ref":null,"errors":[],"null_reasons":{"workbook_cells_before":"NOT_APPLICABLE_TO_SELECTED_FORMAT","cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT","workbook_cells_after":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}},"measured_shape":null,"export_verification_ref":null,"errors":[{"category":"DISK_FULL","code":"CANDIDATE_WRITE_DISK_FULL","stage":"export","retryable":false,"final":true,"message":"ILLUSTRATIVE insufficient candidate storage","run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","instrument":null,"interval":24,"page":null,"file":null,"artifact_id":null,"target_id":"ILLUSTRATIVE_HOST_XLSX","endpoint":null,"attempt":1,"exception_type":"OSError","null_reasons":{"instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}]}},
    {"name":"google_export","type":"export_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","as_of":"2026-10-04T09:00:00Z","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_GOOGLE_SHEETS","target_id":"ILLUSTRATIVE_SHEETS","format":"google_sheets","required":true,"preflight_status":"PASS","export_status":"NOT_RUN","capacity_assessment":{"format":"google_sheets","status":"PASS","capabilities_ref":"ILLUSTRATIVE/caps/google_sheets","source_shape":{"data_rows":3,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":4,"required_columns":10,"required_cells":40,"row_limit":null,"column_limit":null,"cell_limit":100,"workbook_cells_before":40,"workbook_cells_after":40,"limit_evidence_ref":"ILLUSTRATIVE/limits/google_sheets","grid_evidence_ref":"ILLUSTRATIVE/complete-workbook-grid","errors":[],"null_reasons":{"row_limit":"NOT_APPLICABLE_TO_ILLUSTRATIVE_CELL_ONLY_ORACLE","column_limit":"NOT_APPLICABLE_TO_ILLUSTRATIVE_CELL_ONLY_ORACLE"}},"measured_shape":null,"export_verification_ref":null,"errors":[]}},
    {"name":"release","type":"release_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","as_of":"2026-10-04T09:00:00Z","candidate_release_id":"ILLUSTRATIVE_CANDIDATE_ILLUSTRATIVE_RUN_A","status":"BLOCKED","stage_outcomes":{"collection":"COMPLETE","validation":"PASS","artifact":"FAILED","preflight":"PASS","export":"FAILED","publication":"NOT_ATTEMPTED","readback":"NOT_ATTEMPTED"},"required_export_ids":["ILLUSTRATIVE_REQUIRED_CSV","ILLUSTRATIVE_REQUIRED_XLSX","ILLUSTRATIVE_REQUIRED_GOOGLE_SHEETS"],"failed_required_exports":["ILLUSTRATIVE_REQUIRED_CSV","ILLUSTRATIVE_REQUIRED_XLSX","ILLUSTRATIVE_REQUIRED_GOOGLE_SHEETS"],"per_target_outcomes":[{"target_id":"ILLUSTRATIVE_HOST_CSV","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_CSV"],"publication_status":"NOT_ATTEMPTED","readback_status":"NOT_ATTEMPTED","observed_generation":"UNKNOWN","previous_verification":"VERIFIED","preservation_outcome":"NOT_TOUCHED","previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_freshness":"STALE","baseline":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"previous_evidence_ref":"ILLUSTRATIVE/previous/VERIFIED","readback_evidence":null,"mutation_at":null,"readback_at":null,"errors":[]},{"target_id":"ILLUSTRATIVE_HOST_XLSX","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_XLSX"],"publication_status":"NOT_ATTEMPTED","readback_status":"NOT_ATTEMPTED","observed_generation":"UNKNOWN","previous_verification":"VERIFIED","preservation_outcome":"NOT_TOUCHED","previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_freshness":"STALE","baseline":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"previous_evidence_ref":"ILLUSTRATIVE/previous/VERIFIED","readback_evidence":null,"mutation_at":null,"readback_at":null,"errors":[]},{"target_id":"ILLUSTRATIVE_SHEETS","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_GOOGLE_SHEETS"],"publication_status":"NOT_ATTEMPTED","readback_status":"NOT_ATTEMPTED","observed_generation":"UNKNOWN","previous_verification":"VERIFIED","preservation_outcome":"NOT_TOUCHED","previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_freshness":"STALE","baseline":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"previous_evidence_ref":"ILLUSTRATIVE/previous/VERIFIED","readback_evidence":null,"mutation_at":null,"readback_at":null,"errors":[]}],"candidate_manifest_ref":"ILLUSTRATIVE/candidate-manifest","current_manifest_ref":"ILLUSTRATIVE/previous-manifest","previous_manifest_ref":"ILLUSTRATIVE/previous-manifest","published_as_of":"2026-10-03T09:00:00Z","exit_status":1,"errors":[{"category":"DISK_FULL","code":"CANDIDATE_WRITE_DISK_FULL","stage":"export","retryable":false,"final":true,"message":"ILLUSTRATIVE insufficient candidate storage","run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","instrument":null,"interval":24,"page":null,"file":null,"artifact_id":null,"target_id":"ILLUSTRATIVE_HOST_XLSX","endpoint":null,"attempt":1,"exception_type":"OSError","null_reasons":{"instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}]}}
  ]
}
```

<!-- EXAMPLE E15 10_UNVERIFIED_BASELINE_TIMEOUT_READBACK -->
### E15 — Matching known previous baseline resolves content after mutating TIMEOUT without upgrading previous quality/freshness.

```json
{
  "example_id": "E15",
  "family": "10_UNVERIFIED_BASELINE_TIMEOUT_READBACK",
  "illustrative": true,
  "description": "Matching known previous baseline resolves content after mutating TIMEOUT without upgrading previous quality/freshness.",
  "expect": {"valid":true,"invariants":["previous_verification UNVERIFIED","readback VERIFIED, observed PREVIOUS, preservation PRESERVED_VERIFIED","previous ID/as_of/freshness unchanged","TIMEOUT remains","no candidate current; terminal required failure nonzero"]},
  "objects": [
    {"name":"target","type":"per_target_outcome","value":{"target_id":"ILLUSTRATIVE_HOST_CSV","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_CSV"],"publication_status":"UNKNOWN","readback_status":"VERIFIED","observed_generation":"PREVIOUS","previous_verification":"UNVERIFIED","preservation_outcome":"PRESERVED_VERIFIED","previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_freshness":"STALE","baseline":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"previous_evidence_ref":"ILLUSTRATIVE/previous/UNVERIFIED","readback_evidence":{"expected":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"observed":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"evidence_ref":"ILLUSTRATIVE/matching-previous-readback/csv","scope":"FULL_REQUIRED_TARGET"},"mutation_at":"2026-10-04T09:00:00Z","readback_at":"2026-10-04T09:05:00Z","errors":[{"category":"TIMEOUT","code":"MUTATING_TIMEOUT","stage":"export","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted MUTATING_TIMEOUT","run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","instrument":null,"interval":24,"page":null,"file":null,"artifact_id":null,"target_id":"ILLUSTRATIVE_HOST_CSV","endpoint":null,"attempt":1,"exception_type":"TimeoutError","null_reasons":{"instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}]}},
    {"name":"release","type":"release_result","value":{"contract_version":1,"run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","as_of":"2026-10-04T09:00:00Z","candidate_release_id":"ILLUSTRATIVE_CANDIDATE_ILLUSTRATIVE_RUN_A","status":"BLOCKED","stage_outcomes":{"collection":"COMPLETE","validation":"PASS","artifact":"VERIFIED","preflight":"PASS","export":"SUCCEEDED","publication":"UNKNOWN","readback":"NOT_ATTEMPTED"},"required_export_ids":["ILLUSTRATIVE_REQUIRED_CSV","ILLUSTRATIVE_REQUIRED_XLSX","ILLUSTRATIVE_REQUIRED_GOOGLE_SHEETS"],"failed_required_exports":["ILLUSTRATIVE_REQUIRED_CSV","ILLUSTRATIVE_REQUIRED_XLSX","ILLUSTRATIVE_REQUIRED_GOOGLE_SHEETS"],"per_target_outcomes":[{"target_id":"ILLUSTRATIVE_HOST_CSV","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_CSV"],"publication_status":"UNKNOWN","readback_status":"VERIFIED","observed_generation":"PREVIOUS","previous_verification":"UNVERIFIED","preservation_outcome":"PRESERVED_VERIFIED","previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_freshness":"STALE","baseline":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"previous_evidence_ref":"ILLUSTRATIVE/previous/UNVERIFIED","readback_evidence":{"expected":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"observed":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"evidence_ref":"ILLUSTRATIVE/matching-previous-readback/csv","scope":"FULL_REQUIRED_TARGET"},"mutation_at":"2026-10-04T09:00:00Z","readback_at":"2026-10-04T09:05:00Z","errors":[{"category":"TIMEOUT","code":"MUTATING_TIMEOUT","stage":"export","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted MUTATING_TIMEOUT","run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","instrument":null,"interval":24,"page":null,"file":null,"artifact_id":null,"target_id":"ILLUSTRATIVE_HOST_CSV","endpoint":null,"attempt":1,"exception_type":"TimeoutError","null_reasons":{"instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}]},{"target_id":"ILLUSTRATIVE_HOST_XLSX","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_XLSX"],"publication_status":"NOT_ATTEMPTED","readback_status":"NOT_ATTEMPTED","observed_generation":"UNKNOWN","previous_verification":"UNVERIFIED","preservation_outcome":"NOT_TOUCHED","previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_freshness":"STALE","baseline":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"previous_evidence_ref":"ILLUSTRATIVE/previous/UNVERIFIED","readback_evidence":null,"mutation_at":null,"readback_at":null,"errors":[]},{"target_id":"ILLUSTRATIVE_SHEETS","required":true,"planned_artifact_ids":["ILLUSTRATIVE_SNAPSHOT_A_GOOGLE_SHEETS"],"publication_status":"NOT_ATTEMPTED","readback_status":"NOT_ATTEMPTED","observed_generation":"UNKNOWN","previous_verification":"UNVERIFIED","preservation_outcome":"NOT_TOUCHED","previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_freshness":"STALE","baseline":{"basis":"LOGICAL_ROWS_SHA256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","schema_version":"ILLUSTRATIVE_MINI_RAW9_RSI14"},"previous_evidence_ref":"ILLUSTRATIVE/previous/UNVERIFIED","readback_evidence":null,"mutation_at":null,"readback_at":null,"errors":[]}],"candidate_manifest_ref":"ILLUSTRATIVE/candidate-manifest","current_manifest_ref":"ILLUSTRATIVE/previous-manifest","previous_manifest_ref":"ILLUSTRATIVE/previous-manifest","published_as_of":"2026-10-03T09:00:00Z","exit_status":1,"errors":[{"category":"TIMEOUT","code":"MUTATING_TIMEOUT","stage":"export","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted MUTATING_TIMEOUT","run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","instrument":null,"interval":24,"page":null,"file":null,"artifact_id":null,"target_id":"ILLUSTRATIVE_HOST_CSV","endpoint":null,"attempt":1,"exception_type":"TimeoutError","null_reasons":{"instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}]}}
  ]
}
```

<!-- EXAMPLE E16 11_ORDINARY_ERROR_AND_STARTUP -->
### E16 — Ordinary ERROR and startup failure use general envelope/full same error; startup has no invented IDs/artifact/root dependency.

```json
{
  "example_id": "E16",
  "family": "11_ORDINARY_ERROR_AND_STARTUP",
  "illustrative": true,
  "description": "Ordinary ERROR and startup failure use general envelope/full same error; startup has no invented IDs/artifact/root dependency.",
  "expect": {"valid":true,"invariants":["event.error matches identical common error schema","counts0 at FAILED not VALID_EMPTY","all unavailable startup nullable context explicitly explained","no artifact required to report original error"]},
  "objects": [
    {"name":"ordinary_error","type":"event","value":{"timestamp_utc":"2026-10-04T09:05:00Z","level":"ERROR","event":"source_request_failed","stage":"candles","message":"ILLUSTRATIVE redacted original error","run_id":"ILLUSTRATIVE_RUN_A","producer_sha":"ILLUSTRATIVE_SHA","process":"main","worker":"collector","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","release_id":null,"dataset_id":"10years_data_1d_interval","instrument":"ILLUSTRATIVE_SECID","interval":24,"page":2,"file":null,"artifact_id":null,"target_id":null,"outcome":"FAILED","elapsed_ms":3200,"duration_ms":1100,"counts":{"rows_received":0,"rows_accepted":null,"rows_quarantined":null,"pages":2,"instruments":1,"null_reasons":{"rows_accepted":"NOT_MEASURED_AFTER_FAILED_COLLECTION","rows_quarantined":"NOT_MEASURED_AFTER_FAILED_COLLECTION"}},"retries":{"attempt":3,"limit":3},"error":{"category":"SOURCE","code":"RETRY_EXHAUSTED","stage":"candles","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted RETRY_EXHAUSTED","run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","instrument":"ILLUSTRATIVE_SECID","interval":24,"page":2,"file":null,"artifact_id":null,"target_id":null,"endpoint":"https://iss.moex.com/iss/engines/stock/markets/shares/securities/ILLUSTRATIVE_SECID/candles.csv","attempt":3,"exception_type":null,"null_reasons":{"file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}},"fields":{},"null_reasons":{"release_id":"NO_RELEASE_SELECTED","file":"NO_FILE_SELECTED","artifact_id":"NO_ARTIFACT_REQUIRED_FOR_SOURCE_EVENT","target_id":"NO_PUBLICATION_TARGET"}}},
    {"name":"startup_error","type":"event","value":{"timestamp_utc":"2026-10-04T09:05:00Z","level":"ERROR","event":"startup_failed","stage":"startup","message":"ILLUSTRATIVE redacted original error","run_id":null,"producer_sha":null,"process":null,"worker":null,"snapshot_id":null,"release_id":null,"dataset_id":null,"instrument":null,"interval":null,"page":null,"file":null,"artifact_id":null,"target_id":null,"outcome":"FAILED","elapsed_ms":null,"duration_ms":null,"counts":null,"retries":null,"error":{"category":"CONFIG","code":"OUTPUT_ROOT_UNAVAILABLE","stage":"startup","retryable":false,"final":true,"message":"ILLUSTRATIVE output root unavailable; original error remains reportable","run_id":null,"snapshot_id":null,"dataset_id":null,"instrument":null,"interval":null,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"run_id":"STARTUP_CONTEXT_UNAVAILABLE","snapshot_id":"STARTUP_CONTEXT_UNAVAILABLE","dataset_id":"STARTUP_CONTEXT_UNAVAILABLE","instrument":"STARTUP_CONTEXT_UNAVAILABLE","interval":"STARTUP_CONTEXT_UNAVAILABLE","page":"STARTUP_CONTEXT_UNAVAILABLE","file":"STARTUP_CONTEXT_UNAVAILABLE","artifact_id":"STARTUP_CONTEXT_UNAVAILABLE","target_id":"STARTUP_CONTEXT_UNAVAILABLE","endpoint":"STARTUP_CONTEXT_UNAVAILABLE","attempt":"STARTUP_CONTEXT_UNAVAILABLE","exception_type":"STARTUP_CONTEXT_UNAVAILABLE"}},"fields":{},"null_reasons":{"run_id":"STARTUP_CONTEXT_UNAVAILABLE","producer_sha":"STARTUP_CONTEXT_UNAVAILABLE","process":"STARTUP_CONTEXT_UNAVAILABLE","worker":"STARTUP_CONTEXT_UNAVAILABLE","snapshot_id":"STARTUP_CONTEXT_UNAVAILABLE","release_id":"STARTUP_CONTEXT_UNAVAILABLE","dataset_id":"STARTUP_CONTEXT_UNAVAILABLE","instrument":"STARTUP_CONTEXT_UNAVAILABLE","interval":"STARTUP_CONTEXT_UNAVAILABLE","page":"STARTUP_CONTEXT_UNAVAILABLE","file":"STARTUP_CONTEXT_UNAVAILABLE","artifact_id":"STARTUP_CONTEXT_UNAVAILABLE","target_id":"STARTUP_CONTEXT_UNAVAILABLE","elapsed_ms":"STARTUP_CONTEXT_UNAVAILABLE","duration_ms":"STARTUP_CONTEXT_UNAVAILABLE","counts":"STARTUP_CONTEXT_UNAVAILABLE","retries":"STARTUP_CONTEXT_UNAVAILABLE"}}}
  ]
}
```

<!-- EXAMPLE E17 03_OVERFLOW_PREVIOUS_STATES -->
### E17 — Independent exact-boundary and boundary+1 shape oracles; no boundary PASS declares release PASS.

```json
{
  "example_id": "E17",
  "family": "03_OVERFLOW_PREVIOUS_STATES",
  "illustrative": true,
  "description": "Independent exact-boundary and boundary+1 shape oracles; no boundary PASS declares release PASS.",
  "expect": {"valid":true,"invariants":["xlsx1048575+header PASS;1048576+header FAIL","injected Sheets total101 PASS;102 FAIL atlimit101","entire retained workbook counted","capacity PASS independent from full release"]},
  "objects": [
    {"name":"xlsx_boundary_caps","type":"target_capabilities","value":{"capabilities_ref":"ILLUSTRATIVE/caps/xlsx","target_id":"ILLUSTRATIVE_HOST_XLSX","format":"xlsx","verification":"VERIFIED","row_limit":1048576,"column_limit":16384,"cell_limit":null,"grid":null,"limit_evidence_ref":"ILLUSTRATIVE/limits/xlsx","grid_evidence_ref":null,"verified_at":"2026-10-04T09:00:00Z","verification_scope":"DOCUMENTED_FORMAT","null_reasons":{"grid":"NOT_APPLICABLE_TO_SELECTED_FORMAT","cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}}},
    {"name":"xlsx_boundary_pass","type":"capacity_assessment","value":{"format":"xlsx","status":"PASS","capabilities_ref":"ILLUSTRATIVE/caps/xlsx","source_shape":{"data_rows":1048575,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":1048576,"required_columns":10,"required_cells":10485760,"row_limit":1048576,"column_limit":16384,"cell_limit":null,"workbook_cells_before":null,"workbook_cells_after":null,"limit_evidence_ref":"ILLUSTRATIVE/limits/xlsx","grid_evidence_ref":null,"errors":[],"null_reasons":{"workbook_cells_before":"NOT_APPLICABLE_TO_SELECTED_FORMAT","cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT","workbook_cells_after":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}}},
    {"name":"xlsx_boundary_fail","type":"capacity_assessment","value":{"format":"xlsx","status":"FAIL","capabilities_ref":"ILLUSTRATIVE/caps/xlsx","source_shape":{"data_rows":1048576,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":1048577,"required_columns":10,"required_cells":10485770,"row_limit":1048576,"column_limit":16384,"cell_limit":null,"workbook_cells_before":null,"workbook_cells_after":null,"limit_evidence_ref":"ILLUSTRATIVE/limits/xlsx","grid_evidence_ref":null,"errors":[{"category":"CAPACITY","code":"REQUIRED_EXPORT_OVERFLOW","stage":"preflight","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted REQUIRED_EXPORT_OVERFLOW","run_id":null,"snapshot_id":null,"dataset_id":null,"instrument":null,"interval":null,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"run_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","snapshot_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","dataset_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","interval":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE","attempt":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}],"null_reasons":{"workbook_cells_before":"NOT_APPLICABLE_TO_SELECTED_FORMAT","cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT","workbook_cells_after":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}}},
    {"name":"sheets_boundary_caps","type":"target_capabilities","value":{"capabilities_ref":"ILLUSTRATIVE/caps/google_sheets/boundary","target_id":"ILLUSTRATIVE_SHEETS","format":"google_sheets","verification":"VERIFIED","row_limit":null,"column_limit":null,"cell_limit":101,"grid":[{"is_target":true,"rows":10,"columns":10},{"is_target":false,"rows":1,"columns":1}],"limit_evidence_ref":"ILLUSTRATIVE/limits/google_sheets","grid_evidence_ref":"ILLUSTRATIVE/complete-workbook-grid","verified_at":"2026-10-04T09:00:00Z","verification_scope":"INJECTED","null_reasons":{"row_limit":"NOT_APPLICABLE_TO_ILLUSTRATIVE_CELL_ONLY_ORACLE","column_limit":"NOT_APPLICABLE_TO_ILLUSTRATIVE_CELL_ONLY_ORACLE"}}},
    {"name":"sheets_boundary_pass","type":"capacity_assessment","value":{"format":"google_sheets","status":"PASS","capabilities_ref":"ILLUSTRATIVE/caps/google_sheets/boundary","source_shape":{"data_rows":9,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":10,"required_columns":10,"required_cells":100,"row_limit":null,"column_limit":null,"cell_limit":101,"workbook_cells_before":101,"workbook_cells_after":101,"limit_evidence_ref":"ILLUSTRATIVE/limits/google_sheets","grid_evidence_ref":"ILLUSTRATIVE/complete-workbook-grid","errors":[],"null_reasons":{"row_limit":"NOT_APPLICABLE_TO_ILLUSTRATIVE_CELL_ONLY_ORACLE","column_limit":"NOT_APPLICABLE_TO_ILLUSTRATIVE_CELL_ONLY_ORACLE"}}},
    {"name":"sheets_plus_one_caps","type":"target_capabilities","value":{"capabilities_ref":"ILLUSTRATIVE/caps/google_sheets/plus-one","target_id":"ILLUSTRATIVE_SHEETS","format":"google_sheets","verification":"VERIFIED","row_limit":null,"column_limit":null,"cell_limit":101,"grid":[{"is_target":true,"rows":10,"columns":10},{"is_target":false,"rows":1,"columns":2}],"limit_evidence_ref":"ILLUSTRATIVE/limits/google_sheets","grid_evidence_ref":"ILLUSTRATIVE/complete-workbook-grid","verified_at":"2026-10-04T09:00:00Z","verification_scope":"INJECTED","null_reasons":{"row_limit":"NOT_APPLICABLE_TO_ILLUSTRATIVE_CELL_ONLY_ORACLE","column_limit":"NOT_APPLICABLE_TO_ILLUSTRATIVE_CELL_ONLY_ORACLE"}}},
    {"name":"sheets_boundary_fail","type":"capacity_assessment","value":{"format":"google_sheets","status":"FAIL","capabilities_ref":"ILLUSTRATIVE/caps/google_sheets/plus-one","source_shape":{"data_rows":9,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":10,"required_columns":10,"required_cells":100,"row_limit":null,"column_limit":null,"cell_limit":101,"workbook_cells_before":102,"workbook_cells_after":102,"limit_evidence_ref":"ILLUSTRATIVE/limits/google_sheets","grid_evidence_ref":"ILLUSTRATIVE/complete-workbook-grid","errors":[{"category":"CAPACITY","code":"REQUIRED_EXPORT_OVERFLOW","stage":"preflight","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted REQUIRED_EXPORT_OVERFLOW","run_id":null,"snapshot_id":null,"dataset_id":null,"instrument":null,"interval":null,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"run_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","snapshot_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","dataset_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","interval":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE","attempt":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}],"null_reasons":{"row_limit":"NOT_VERIFIED_OR_NOT_MEASURED","column_limit":"NOT_VERIFIED_OR_NOT_MEASURED"}}}
  ]
}
```

<!-- EXAMPLE E18 11_ORDINARY_ERROR_AND_STARTUP -->
### E18 — Capacity ERROR adds assessed geometry, previous verification/preservation, separate outcomes and nonzero summary to full envelope.

```json
{
  "example_id": "E18",
  "family": "11_ORDINARY_ERROR_AND_STARTUP",
  "illustrative": true,
  "description": "Capacity ERROR adds assessed geometry, previous verification/preservation, separate outcomes and nonzero summary to full envelope.",
  "expect": {"valid":true,"invariants":["capacity is additive envelope","applicable run/snapshot/dataset/artifact/target/interval present","known previous UNVERIFIED does not become verified","error/summary agree on required failure"]},
  "objects": [
    {"name":"capacity_event","type":"event","value":{"timestamp_utc":"2026-10-04T09:05:00Z","level":"ERROR","event":"required_export_capacity_failed","stage":"preflight","message":"ILLUSTRATIVE redacted original error","run_id":"ILLUSTRATIVE_RUN_A","producer_sha":"ILLUSTRATIVE_SHA","process":"main","worker":"collector","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","release_id":null,"dataset_id":"10years_data_1d_interval","instrument":null,"interval":24,"page":null,"file":"10years_data_1d_interval.xlsx","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_XLSX","target_id":"ILLUSTRATIVE_HOST_XLSX","outcome":"FAILED","elapsed_ms":3200,"duration_ms":1100,"counts":null,"retries":null,"error":{"category":"CAPACITY","code":"REQUIRED_EXPORT_OVERFLOW","stage":"preflight","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted REQUIRED_EXPORT_OVERFLOW","run_id":"ILLUSTRATIVE_RUN_A","snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","dataset_id":"10years_data_1d_interval","instrument":null,"interval":24,"page":null,"file":"10years_data_1d_interval.xlsx","artifact_id":"ILLUSTRATIVE_SNAPSHOT_A_XLSX","target_id":"ILLUSTRATIVE_HOST_XLSX","endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"instrument":"NOT_APPLICABLE","page":"NOT_APPLICABLE","endpoint":"NOT_APPLICABLE","attempt":"NOT_APPLICABLE","exception_type":"NOT_APPLICABLE"}},"fields":{"capacity":{"assessment":{"format":"xlsx","status":"FAIL","capabilities_ref":"ILLUSTRATIVE/caps/xlsx","source_shape":{"data_rows":1048576,"data_columns":10,"header_rows":1,"index_columns":0},"required_rows":1048577,"required_columns":10,"required_cells":10485770,"row_limit":1048576,"column_limit":16384,"cell_limit":null,"workbook_cells_before":null,"workbook_cells_after":null,"limit_evidence_ref":"ILLUSTRATIVE/limits/xlsx","grid_evidence_ref":null,"errors":[{"category":"CAPACITY","code":"REQUIRED_EXPORT_OVERFLOW","stage":"preflight","retryable":false,"final":true,"message":"ILLUSTRATIVE redacted REQUIRED_EXPORT_OVERFLOW","run_id":null,"snapshot_id":null,"dataset_id":null,"instrument":null,"interval":null,"page":null,"file":null,"artifact_id":null,"target_id":null,"endpoint":null,"attempt":null,"exception_type":null,"null_reasons":{"run_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","snapshot_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","dataset_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","instrument":"NOT_AVAILABLE_OR_NOT_APPLICABLE","interval":"NOT_AVAILABLE_OR_NOT_APPLICABLE","page":"NOT_AVAILABLE_OR_NOT_APPLICABLE","file":"NOT_AVAILABLE_OR_NOT_APPLICABLE","artifact_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","target_id":"NOT_AVAILABLE_OR_NOT_APPLICABLE","endpoint":"NOT_AVAILABLE_OR_NOT_APPLICABLE","attempt":"NOT_AVAILABLE_OR_NOT_APPLICABLE","exception_type":"NOT_AVAILABLE_OR_NOT_APPLICABLE"}}],"null_reasons":{"workbook_cells_before":"NOT_APPLICABLE_TO_SELECTED_FORMAT","cell_limit":"NOT_APPLICABLE_TO_SELECTED_FORMAT","grid_evidence_ref":"NOT_APPLICABLE_TO_SELECTED_FORMAT","workbook_cells_after":"NOT_APPLICABLE_TO_SELECTED_FORMAT"}},"horizon":"10years","candidate_snapshot_id":"ILLUSTRATIVE_SNAPSHOT_A","saved_candidate_artifact_ids":[],"saved_candidate_rows":null,"previous_release_id":"ILLUSTRATIVE_PREVIOUS_RELEASE","previous_as_of":"2026-10-03T09:00:00Z","previous_verification":"UNVERIFIED","previous_freshness":"STALE","preservation_outcome":"NOT_TOUCHED","preservation_evidence_ref":null,"export_status":"NOT_RUN","publication_status":"NOT_ATTEMPTED","readback_status":"NOT_ATTEMPTED","observed_generation":"UNKNOWN","failed_required_exports":["ILLUSTRATIVE_REQUIRED_CSV","ILLUSTRATIVE_REQUIRED_XLSX","ILLUSTRATIVE_REQUIRED_GOOGLE_SHEETS"],"release_status":"BLOCKED","exit_status":1}},"null_reasons":{"release_id":"NOT_APPLICABLE_OR_NOT_MEASURED","instrument":"NOT_APPLICABLE_OR_NOT_MEASURED","page":"NOT_APPLICABLE_OR_NOT_MEASURED","counts":"NOT_APPLICABLE_OR_NOT_MEASURED","retries":"NOT_APPLICABLE_OR_NOT_MEASURED"}}}
  ]
}
```
