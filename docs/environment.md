# F1-01 reproducible Windows environment

This project uses a documented **scripts workflow**. Run scripts from the checkout;
`pip install .` is not the supported installation command. F1 installs dependencies
and checks them in isolation. It does not make the current pipeline safe to run.
Run the PowerShell commands below from the checkout root.

## Supported profile and dependency files

The current verification target is Windows x86-64, ordinary GIL CPython **3.14.8**.
Linux and macOS are **NOT VERIFIED**; compatibility tags or Windows multiprocessing
checks do not establish support on another operating system.

| Profile | Direct requirements | Full Windows CPython 3.14 hash lock | Purpose |
|---|---|---|---|
| runtime | `requirements/runtime.in` | `requirements/windows-cp314-runtime.lock` | Collection, indicators and configured CSV/XLSX/Google backends |
| test | `requirements/test.in` | `requirements/windows-cp314-test.lock` | Runtime plus the isolated pytest checks |
| dev | `requirements/dev.in` | `requirements/windows-cp314-dev.lock` | Test tools plus explicit notebook conversion and development tools |

Each lock contains every resolved package, including transitive dependencies, with
an exact version and the SHA-256 of its selected wheel. These are platform locks,
not universal locks. Test and dev resolution use a **version-only constraint file**
derived from the runtime lock so the three profiles cannot silently select different
runtime versions. Their final locks are complete; verification does not combine
multiple hash locks or install unpinned extras.

Jupytext stays **1.19.2**, matching the existing pre-commit hook. `nbconvert` belongs
to dev, not runtime. Python 3.14.6 and the package versions in the September audit
are historical evidence, not this environment's lock.

## Local interpreter and pip bootstrap

Use the official full PythonCore ZIP, not the embeddable or free-threaded build:

- [Python 3.14.8 full Windows x64 ZIP](https://www.python.org/ftp/python/3.14.8/python-3.14.8-amd64.zip)
- [Official Windows release manifest](https://www.python.org/ftp/python/3.14.8/windows-3.14.8.json)
- ZIP SHA-256: `4873947a8afc037846b180312b83c744a4146a851cfd316a75c3125a4d8299da`

Verify the downloaded archive before extracting it into ignored `.f1/python`.
Use `.f1/python/python.exe` directly. Do not install globally or change PATH,
registry, launcher settings or file associations. Each worktree has its own
interpreter, environments, wheelhouse and evidence under ignored `.f1/`.

The verified full ZIP includes **pip 26.2.1**. Create each environment with this
interpreter's `venv`/`ensurepip`, verify that the resulting pip is **26.2.1**, and
record its version separately. Its integrity is bound to the verified official ZIP
hash. Do not upgrade pip or include it in the application locks. No separate pip
wheel download or unverified source build is needed.

Bootstrap a fresh worktree-local interpreter with this block. It downloads only
when the named archive is absent, verifies any retained archive again, and refuses
an existing `.f1/python` destination instead of overwriting or reusing it.

```powershell
if (Test-Path -LiteralPath .f1/python) { throw "Use a fresh .f1/python destination" }
New-Item -ItemType Directory -Force -Path @('.f1/downloads', '.f1/evidence', '.f1/wheelhouse', '.f1/tmp/pip') | Out-Null
$taskArchive = '.f1/downloads/python-3.14.8-amd64.zip'
$taskExpectedHash = '4873947a8afc037846b180312b83c744a4146a851cfd316a75c3125a4d8299da'
if (-not (Test-Path -LiteralPath $taskArchive)) {
    Invoke-WebRequest -Uri 'https://www.python.org/ftp/python/3.14.8/python-3.14.8-amd64.zip' -OutFile $taskArchive -UseBasicParsing -ErrorAction Stop
}
$taskArchiveHash = (Get-FileHash -LiteralPath $taskArchive -Algorithm SHA256 -ErrorAction Stop).Hash.ToLowerInvariant()
if ($taskArchiveHash -ne $taskExpectedHash) { throw "Official Python ZIP hash mismatch" }
Expand-Archive -LiteralPath $taskArchive -DestinationPath .f1/python -ErrorAction Stop
$taskPython = (Resolve-Path .f1/python/python.exe).Path
& $taskPython -I -S -c 'import struct, sys, sysconfig; assert sys.version_info[:3] == (3, 14, 8); assert struct.calcsize("P") * 8 == 64; assert sysconfig.get_config_var("Py_GIL_DISABLED") == 0; assert sys._is_gil_enabled(); print("CPython 3.14.8 x64 GIL: PASS")'
if ($LASTEXITCODE) { throw "Interpreter identity check failed" }
$taskPipVersion = & $taskPython -I -m pip --version
if ($LASTEXITCODE -or $taskPipVersion -notmatch '^pip 26\.2\.1 ') { throw "Unexpected bundled pip" }
```

## Install committed locks or regenerate them

For ordinary installation, bootstrap the interpreter, skip the report-conversion
recipe and resolve loop, then use the verify loop below. Its download command
fetches only the committed, hash-checked wheels before the offline installation.
Regenerate locks only for an intentional dependency change.

Use public PyPI explicitly. Preserve the process's existing `PIP_CONFIG_FILE`, set
it to `nul` for the operation, pass `--isolated`, and restore the original value in
`finally`. Also redirect process-only `TEMP` and `TMP` to ignored `.f1/tmp/pip`,
restore their previous values, and disable pip's download cache. Do not edit
persistent pip configuration or expose credential-bearing index URLs in evidence.

Copy the following standard-library-only recipe into ignored `.f1/lock_report.py`.
It converts pip's report to a complete, sorted hash lock and can also write the
runtime version constraints. Before writing, it verifies pip 26.2.1 and the report's
Windows x64 CPython 3.14.8 environment. It also rejects unsupported report versions,
empty reports, duplicate canonical names, missing SHA-256, non-wheel artifacts,
yanked packages, direct URLs, VCS/local inputs and non-public download hosts.
No new tracked helper is required.

```python
import argparse
import json
import re
from pathlib import Path
from urllib.parse import unquote, urlsplit


def report_records(report_path):
    report = json.loads(Path(report_path).read_text(encoding="utf-8"))
    if report.get("version") != "1":
        raise ValueError("Unsupported pip report version")
    if report.get("pip_version") != "26.2.1":
        raise ValueError("Unexpected pip version")
    expected_environment = {
        "implementation_name": "cpython",
        "platform_python_implementation": "CPython",
        "sys_platform": "win32",
        "platform_system": "Windows",
        "platform_machine": "AMD64",
        "python_full_version": "3.14.8",
    }
    environment = report.get("environment")
    if not isinstance(environment, dict):
        raise ValueError("Missing report environment")
    for field, expected in expected_environment.items():
        if environment.get(field) != expected:
            raise ValueError("Unexpected report environment field: " + field)
    installs = report.get("install")
    if not isinstance(installs, list) or not installs:
        raise ValueError("Report has no install records")
    records = {}
    for item in installs:
        if item.get("is_direct") is not False:
            raise ValueError("Direct URL or unclassified requirement")
        if item.get("is_yanked") is not False:
            raise ValueError("Yanked or unclassified package")
        download = item.get("download_info", {})
        if "vcs_info" in download or "dir_info" in download:
            raise ValueError("VCS or local input")
        url = urlsplit(download.get("url", ""))
        if (url.scheme != "https" or url.hostname != "files.pythonhosted.org"
                or url.username or url.password or url.query or url.fragment
                or not unquote(url.path).endswith(".whl")):
            raise ValueError("Expected a public PyPI HTTPS wheel")
        metadata = item.get("metadata", {})
        name = metadata.get("name", "")
        version = metadata.get("version", "")
        if not isinstance(name, str) or not re.fullmatch(
                r"[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?", name):
            raise ValueError("Invalid distribution name")
        if not isinstance(version, str) or not re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9.!+_-]*", version):
            raise ValueError("Invalid one-line distribution version")
        canonical = re.sub(r"[-_.]+", "-", name).lower()
        if canonical in records:
            raise ValueError("Duplicate canonical distribution: " + canonical)
        digest = download.get("archive_info", {}).get("hashes", {}).get("sha256")
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("Missing or invalid wheel SHA-256: " + canonical)
        records[canonical] = (version, digest)
    return sorted(records.items())


parser = argparse.ArgumentParser()
parser.add_argument("report")
parser.add_argument("lock")
parser.add_argument("--constraints")
args = parser.parse_args()
records = report_records(args.report)
lock_text = "# Generated from a clean pip report; Windows x64 CPython 3.14.\n"
for name, (version, digest) in records:
    lock_text += f"{name}=={version} --hash=sha256:{digest}\n"
Path(args.lock).write_text(lock_text, encoding="utf-8", newline="\n")
if args.constraints:
    constraint_lines = [
        line.split(" --hash=sha256:", 1)[0]
        for line in Path(args.lock).read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    ]
    constraints = "\n".join(constraint_lines) + "\n"
    Path(args.constraints).write_text(constraints, encoding="utf-8", newline="\n")
print(f"Converted {len(records)} wheel records")
```

Resolve and install each profile in a new `.f1/venv-<profile>-resolve` environment
containing only its verified bootstrap pip. Do runtime first and pass its generated
constraints to test and dev. Retain the real installation report; the independent
second environment proves installation from the generated lock and wheelhouse.

```powershell
$taskPreviousPipConfig = [Environment]::GetEnvironmentVariable('PIP_CONFIG_FILE', 'Process')
$taskPreviousTemp = [Environment]::GetEnvironmentVariable('TEMP', 'Process')
$taskPreviousTmp = [Environment]::GetEnvironmentVariable('TMP', 'Process')
New-Item -ItemType Directory -Force -Path .f1/tmp/pip | Out-Null
$taskPipTemp = (Resolve-Path .f1/tmp/pip).Path
$taskPython = (Resolve-Path .f1/python/python.exe).Path
try {
    $env:PIP_CONFIG_FILE = 'nul'
    $env:TEMP = $taskPipTemp
    $env:TMP = $taskPipTemp
    foreach ($taskProfile in @('runtime', 'test', 'dev')) {
        $taskResolveRoot = ".f1/venv-$taskProfile-resolve"
        if (Test-Path -LiteralPath $taskResolveRoot) { throw "Use a new resolve environment" }
        & $taskPython -I -m venv $taskResolveRoot
        if ($LASTEXITCODE) { throw "venv creation failed" }
        $taskResolvePython = "$taskResolveRoot/Scripts/python.exe"
        $taskPipVersion = & $taskResolvePython -I -m pip --version
        if ($LASTEXITCODE -or $taskPipVersion -notmatch '^pip 26\.2\.1 ') { throw "Unexpected bootstrap pip" }
        $taskConstraintArgs = @()
        if ($taskProfile -ne 'runtime') { $taskConstraintArgs = @('-c', '.f1/runtime.constraints.txt') }
        & $taskResolvePython -I -m pip --isolated --no-cache-dir install --only-binary=:all: --index-url https://pypi.org/simple --report ".f1/evidence/$taskProfile-resolve.json" -r "requirements/$taskProfile.in" @taskConstraintArgs
        if ($LASTEXITCODE) { throw "profile resolution failed" }
        $taskConversionArgs = @(".f1/evidence/$taskProfile-resolve.json", "requirements/windows-cp314-$taskProfile.lock")
        if ($taskProfile -eq 'runtime') { $taskConversionArgs += @('--constraints', '.f1/runtime.constraints.txt') }
        & $taskPython -I .f1/lock_report.py @taskConversionArgs
        if ($LASTEXITCODE) { throw "lock conversion failed" }
        & $taskResolvePython -I -m pip --isolated --no-cache-dir download --only-binary=:all: --index-url https://pypi.org/simple --require-hashes --dest .f1/wheelhouse -r "requirements/windows-cp314-$taskProfile.lock"
        if ($LASTEXITCODE) { throw "wheelhouse download failed" }
    }
}
finally {
    [Environment]::SetEnvironmentVariable('PIP_CONFIG_FILE', $taskPreviousPipConfig, 'Process')
    [Environment]::SetEnvironmentVariable('TEMP', $taskPreviousTemp, 'Process')
    [Environment]::SetEnvironmentVariable('TMP', $taskPreviousTmp, 'Process')
}
```

Bootstrap creates ignored `.f1/evidence` and `.f1/wheelhouse` directories. Retain
resolver reports, wheel URLs/hashes, logs and interpreter/pip provenance there. Regenerate
and review locks only as an intentional dependency change; ordinary installation
uses the committed lock without resolving again.

Create a **second clean environment** `.f1/venv-<profile>-verify` using the same
verified interpreter, check its bootstrap pip version, then install the selected
committed lock offline. The download step supports a fresh checkout; omit that
step for an entirely offline replay using a retained, verified wheelhouse:

```powershell
$taskPreviousPipConfig = [Environment]::GetEnvironmentVariable('PIP_CONFIG_FILE', 'Process')
$taskPreviousTemp = [Environment]::GetEnvironmentVariable('TEMP', 'Process')
$taskPreviousTmp = [Environment]::GetEnvironmentVariable('TMP', 'Process')
New-Item -ItemType Directory -Force -Path .f1/tmp/pip | Out-Null
$taskPipTemp = (Resolve-Path .f1/tmp/pip).Path
$taskPython = (Resolve-Path .f1/python/python.exe).Path
try {
    $env:PIP_CONFIG_FILE = 'nul'
    $env:TEMP = $taskPipTemp
    $env:TMP = $taskPipTemp
    foreach ($taskProfile in @('runtime', 'test', 'dev')) {
        $taskVerifyRoot = ".f1/venv-$taskProfile-verify"
        if (Test-Path -LiteralPath $taskVerifyRoot) { throw "Use a new verify environment" }
        & $taskPython -I -m venv $taskVerifyRoot
        if ($LASTEXITCODE) { throw "venv creation failed" }
        $taskVerifyPython = "$taskVerifyRoot/Scripts/python.exe"
        $taskPipVersion = & $taskVerifyPython -I -m pip --version
        if ($LASTEXITCODE -or $taskPipVersion -notmatch '^pip 26\.2\.1 ') { throw "Unexpected bootstrap pip" }
        & $taskVerifyPython -I -m pip --isolated --no-cache-dir download --only-binary=:all: --index-url https://pypi.org/simple --require-hashes --dest .f1/wheelhouse -r "requirements/windows-cp314-$taskProfile.lock"
        if ($LASTEXITCODE) { throw "locked wheelhouse download failed" }
        & $taskVerifyPython -I -m pip --isolated --no-cache-dir install --no-index --find-links .f1/wheelhouse --only-binary=:all: --require-hashes --report ".f1/evidence/$taskProfile-verify.json" -r "requirements/windows-cp314-$taskProfile.lock"
        if ($LASTEXITCODE) { throw "offline profile installation failed" }
        & $taskVerifyPython -I -m pip --isolated check
        if ($LASTEXITCODE) { throw "pip check failed" }
        & $taskVerifyPython -I -m pip --isolated list --format=json
        if ($LASTEXITCODE) { throw "installed inventory failed" }
        & $taskVerifyPython -I tests/environment/test_environment.py --profile $taskProfile --output-root ".f1/evidence/$taskProfile"
        if ($LASTEXITCODE) { throw "profile smoke check failed" }
    }
}
finally {
    [Environment]::SetEnvironmentVariable('PIP_CONFIG_FILE', $taskPreviousPipConfig, 'Process')
    [Environment]::SetEnvironmentVariable('TEMP', $taskPreviousTemp, 'Process')
    [Environment]::SetEnvironmentVariable('TMP', $taskPreviousTmp, 'Process')
}
```

Retain the actual installed inventory, `pip check`, native TA-Lib computations and
backend checks independently of the resolution report. Check every exit code; a
later successful command does not recover an earlier failure.

The smoke CLI accepts `--profile runtime|test|dev` (default `runtime`) and requires
an explicit ignored `--output-root`. It installs guards before third-party imports
and uses synthetic data. It does not import project pipeline modules.

For the test profile, run the scoped suite only:

```powershell
$taskPreviousPluginAutoload = [Environment]::GetEnvironmentVariable('PYTEST_DISABLE_PLUGIN_AUTOLOAD', 'Process')
$taskPreviousPytestAddopts = [Environment]::GetEnvironmentVariable('PYTEST_ADDOPTS', 'Process')
$taskPreviousPytestPlugins = [Environment]::GetEnvironmentVariable('PYTEST_PLUGINS', 'Process')
try {
    $env:PYTEST_DISABLE_PLUGIN_AUTOLOAD = '1'
    [Environment]::SetEnvironmentVariable('PYTEST_ADDOPTS', $null, 'Process')
    [Environment]::SetEnvironmentVariable('PYTEST_PLUGINS', $null, 'Process')
    & .f1/venv-test-verify/Scripts/python.exe -I -B -m pytest -c pyproject.toml --confcutdir tests/environment tests/environment -q --basetemp .f1/tmp/pytest -o cache_dir=.f1/cache/pytest
    if ($LASTEXITCODE) { throw "scoped environment tests failed" }
}
finally {
    [Environment]::SetEnvironmentVariable('PYTEST_DISABLE_PLUGIN_AUTOLOAD', $taskPreviousPluginAutoload, 'Process')
    [Environment]::SetEnvironmentVariable('PYTEST_ADDOPTS', $taskPreviousPytestAddopts, 'Process')
    [Environment]::SetEnvironmentVariable('PYTEST_PLUGINS', $taskPreviousPytestPlugins, 'Process')
}
```

Pytest defaults to the test profile. A process-only `MDS_ENVIRONMENT_PROFILE=dev`
selects dev; `MDS_ENVIRONMENT_OUTPUT_ROOT` selects the ignored evidence root
(default `.f1/evidence/pytest`). Restore any pre-existing values after using them.
`-B` prevents bytecode writes in the tracked test tree. The collection guard requires
explicit `tests/environment` targets and ignored `.f1/` cache/temp roots.
Disable automatic third-party pytest plugins and unset inherited `PYTEST_ADDOPTS`
and `PYTEST_PLUGINS` before Python starts, then restore all three values as above.

## Safety and acceptance evidence

Current `main.py` still converts notebooks at import and starts the collection and
publication pipeline when run. Safe entrypoint, paths and conversion handling are
**F2** work. Do not import `main.py`, run production entrypoints, perform a full
reload, publish Google/FTP data or run broad discovery against legacy
`tests.py`/`main_tests.py`. Do not execute notebooks as an environment check.

The pandas **2.3.3** pin and fixture-backend checks do not fix the dohod.ru HTML
parser; issue [#31](https://github.com/foykes/moex-dataset/issues/31) remains C work.
Dataset schemas, calculations, public filenames, notebook pairs and publication
targets remain unchanged. Credentials are neither needed nor printed. The project
budget stays zero; no billing, paid services or quota increases are part of F1.

| Criterion | F1 evidence / boundary |
|---|---|
| [#43](https://github.com/foykes/moex-dataset/issues/43): Владелец выбрал package либо документированный scripts workflow; установка соответствует этому выбору. | Scripts workflow and profile installation documented; package installation is not promised |
| #43: Прямые runtime и обязательные backends/tool dependencies объявлены, версии/lock проверяемы. | Three `.in` manifests, complete per-platform hash locks and clean resolution/installation provenance |
| #43: Clean-env3.14GIL install, pipcheck и nativeTA вычисления проходят; supported platforms объявляются только с runner evidence. | Actual Windows profile results recorded below; pipeline success remains unproved |
| [#67](https://github.com/foykes/moex-dataset/issues/67): Сохранены OS/architecture/Python, dependency provenance и фактические start methods. | Record Windows evidence; real Linux/macOS runners are **NOT VERIFIED** |
| #67: Реальные subprocess проверяют Linux forkserver/fork и macOS spawn, CWD/Unicode/пробелы/encoding/child failure; writers ограничены fixtures/staging. | Deferred to R1/#67; Windows checks cannot satisfy these criteria |
| #67: Матрица содержит PASS/FAIL/NOT VERIFIED и логи. Общий CI и packaging остаются отдельными задачами. | Keep explicit result matrix; general offline CI remains F3 |

The final Windows verification used CPython **3.14.8 x86-64 GIL**, `Py_GIL_DISABLED=0`,
GIL enabled, and pip **26.2.1** from the SHA-256-verified official ZIP. The reported
OS version was **10.0**, build **26200**. The actual multiprocessing start method
and the only available method were **spawn**.

| Profile | Locked wheels | Installed distributions, including bootstrap pip | Fresh resolve/install | Offline hash replay, pip check and smoke CLI | Scoped pytest |
|---|---:|---:|---|---|---|
| runtime | 43 | 44 | PASS, exit 0 | PASS, exit 0 | NOT APPLICABLE |
| test | 47 | 48 | PASS, exit 0 | PASS, exit 0 | 11 passed, 2.76 s, exit 0 |
| dev | 84 | 85 | PASS, exit 0 | PASS, exit 0 | 11 passed, 2.89 s, exit 0 |

All three profiles passed native TA-Lib checks with Python wrapper **0.6.8** and
native library **0.6.4**: SMA, RSI and MACD were compared with independent synthetic
calculations, including Series input. Thirteen prohibited-operation guard probes
passed in each profile. UTF-8 raw-byte HTML parsing and the configured fallback passed. Synthetic
CSV/XLSX round trips passed for ten data columns, three rows, the default service
index, NaN, dtypes and values. These fixtures do not establish production dataset
schema conformance or published-format parity.

The documented report converter reproduced all three locks byte-for-byte.
Twelve negative cases covering report, pip/platform identity and wheel provenance
were rejected without creating an output lock.

Dev includes Jupytext **1.19.2**, pre-commit **4.3.0** and nbconvert **7.16.6**; no
notebook conversion was performed. A fresh dev verification environment was also
checked before installation: interpreter and guards passed, while missing imports
and the absent installed dependency graph failed with the expected exit 1.
Installation and the independent offline replay then produced the green results
above. Reports, inventories, fixture outputs and test outcomes remain in ignored
`.f1/evidence`.

Linux/macOS runners, the application pipeline, production publication and general
CI remain **NOT VERIFIED**. These results do not declare product readiness or
satisfy all acceptance criteria of #67.

Rollback: revert the F1 manifests, locks, configuration and documentation commit.
Ignored `.f1/` environments and evidence are local; preserve needed evidence before
removing them. No dataset or remote target is changed by this slice.
