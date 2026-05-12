# AGENTS.md

Project instructions for Codex and other AI coding agents working on `moex-dataset`.

## Project context

This project builds datasets with Moscow Exchange trading data through the MOEX ISS API.
It downloads ticker lists, candle data, dividends, technical indicators, and prepares files for publishing.

Main areas:

- `data_gathering.py` — MOEX ticker lists, candle downloads, full reloads, incremental updates.
- `tech.py` — technical indicator calculations.
- `dividends.py` and related files — dividends data.
- `upload.py` — upload/publish flow.
- `settings/` — dataset and runtime configuration.
- `README.md` and project notes — project documentation and roadmap.

## Core rule

Do not rewrite the project from scratch.
Work in small, reviewable PRs that preserve the author's coding style.

Prefer minimal, safe, incremental changes over large architecture rewrites.

## Coding style

Keep the current project style unless the task explicitly asks otherwise:

- keep simple functions instead of introducing heavy abstractions;
- do not convert the project to classes without a separate task;
- keep existing public function names and output column names;
- keep the Jupytext-style `# %%` sections in Python files;
- keep Russian comments, prints, and user-facing messages when editing existing code;
- use clear intermediate variables;
- avoid clever one-liners if they make the code harder to read;
- prefer local fixes over broad refactoring;
- do not silently change dataset schemas, file names, or column order.

When adding new English-only project instructions or technical docs, use concise English to reduce token usage.

## Safety rules

- Do not run a full production data reload unless the task explicitly asks for it.
- Do not call the real MOEX API from tests; use mocks or fixtures.
- Do not commit secrets, FTP credentials, service account files, API tokens, or local paths.
- Do not commit large generated datasets unless the task is specifically about dataset files.
- Do not remove existing behavior only because it looks old or duplicated.
- If a change can alter published CSV/XLSX outputs, call it out clearly in the PR summary.

## Preferred workflow

Use small PRs. One PR should cover one topic.

Current project workflow:

1. Add and maintain `AGENTS.md`.
2. Analyze the author's code style and run a project audit.
3. Go through Plan Mode using the audit and the author's roadmap.
4. Fix obvious bugs and add the first tests.
5. Complete the missing tests.
6. Speed up `data_gathering.py` and `tech.py`.
7. Add logging and improve exception handling.
8. Clean up documentation.
9. Repeat audit, bug fixing, and test improvement.

## Plan Mode expectations

Before editing code, prepare a short plan that includes:

- what was found;
- which files will be touched;
- which behavior should stay unchanged;
- which tests will be added or updated;
- risks and rollback notes.

Do not start a large refactor from Plan Mode.
Break the plan into small PR-sized tasks.

## Audit expectations

When auditing the project:

- do not change files during the audit;
- point to concrete files and functions;
- separate bugs, performance issues, missing tests, documentation gaps, and architecture risks;
- mark quick fixes separately from larger work;
- prefer practical fixes over theoretical rewrites.

## Testing expectations

Use `pytest` for new tests unless the project already defines another explicit test runner.

Prioritize tests for:

- `data_gathering.py`;
- `tech.py`;
- dividends logic;
- update/reload behavior;
- config handling;
- error handling and retry behavior.

Tests that cover MOEX API calls must use mocked responses or local fixtures.

## Performance priorities

For `data_gathering.py`:

- avoid repeated downloads when data can be reused;
- make sleep/rate-limit behavior configurable;
- consider reusing `requests.Session`;
- avoid unnecessary XLSX writes when CSV is enough;
- check whether 10-year datasets can be sliced from 30-year datasets.

For `tech.py`:

- avoid repeated `pd.concat` inside loops;
- calculate by ticker safely without mixing rows between tickers;
- preserve existing indicator names and output columns;
- add tests before changing calculations.

## Logging and exceptions

Improve API error handling gradually:

- log failed ticker, interval, date range, and endpoint;
- preserve an exception list/report for failed downloads;
- retry carefully with configurable limits;
- do not create infinite retry loops;
- keep console output readable.

## Documentation

Documentation cleanup should be done after behavior is covered by tests.

Keep docs practical:

- how to run data loading;
- how to run tests;
- what each major file does;
- dataset formats;
- known limitations;
- project roadmap.

## PR summary format

Every PR should explain:

- changed files;
- what changed;
- what behavior stayed unchanged;
- tests run;
- risks or follow-up tasks.
