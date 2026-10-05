"""Offline F1 checks; also a third-party-only smoke CLI without pytest."""

from pathlib import Path
import sys

sys.dont_write_bytecode = True

# python -I не оставляет каталог скрипта в sys.path. Добавляем только эту зону.
test_directory = str(Path(__file__).resolve().parent)
if test_directory not in sys.path:
    sys.path.insert(0, test_directory)

from _safety import OfflineViolation, install_guards, isolated_path, validate_pytest_basetemp, validate_pytest_cache

install_guards()

import argparse
from datetime import datetime, timezone
import hashlib
import importlib
from importlib import metadata
import json
import multiprocessing
import os
import re
import socket
import struct
import subprocess
import sysconfig
import time


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROFILES = ("runtime", "test", "dev")
EXPECTED_PYTHON = (3, 14, 8)
EXPECTED_PIP = "26.2.1"


def check_interpreter():
    assert sys.implementation.name == "cpython", "CPython is required"
    assert sys.version_info[:3] == EXPECTED_PYTHON, "Expected CPython 3.14.8"
    assert sys.platform == "win32", "This evidence profile covers Windows only"
    assert struct.calcsize("P") * 8 == 64, "Expected a 64-bit interpreter"
    # platform.machine() на Windows может запускать системный subprocess.
    assert sysconfig.get_platform() == "win-amd64", "Expected Windows x64"
    assert not sysconfig.get_config_var("Py_GIL_DISABLED"), "Free-threaded builds are outside this profile"
    assert sys._is_gil_enabled(), "The GIL must be enabled"
    assert metadata.version("pip") == EXPECTED_PIP, "Expected seed pip 26.2.1"
    assert sys.prefix != sys.base_prefix, "Run checks in a dedicated venv"
    assert Path(sys.base_prefix).resolve().is_relative_to(PROJECT_ROOT / ".f1" / "python"), "Expected the project-local F1 interpreter"
    venv_config = (Path(sys.prefix) / "pyvenv.cfg").read_text(encoding="utf-8")
    settings = dict(line.split("=", 1) for line in venv_config.splitlines() if "=" in line)
    settings = {key.strip().lower(): value.strip().lower() for key, value in settings.items()}
    assert settings.get("include-system-site-packages") == "false", "System site packages must remain excluded"
    windows_version = sys.getwindowsversion()
    start_method = multiprocessing.get_start_method()
    available_methods = multiprocessing.get_all_start_methods()
    assert start_method == "spawn" and available_methods == ["spawn"], "Expected the Windows spawn start method"
    return {"python": "3.14.8", "implementation": "CPython", "os": "Windows", "os_version": {"major": windows_version.major, "minor": windows_version.minor, "build": windows_version.build}, "architecture": "x64", "gil": "enabled", "pip": EXPECTED_PIP, "venv": "isolated", "include_system_site_packages": False, "multiprocessing_start_method": start_method, "multiprocessing_available_methods": available_methods}


def check_imports():
    names = ("numpy", "pandas", "requests", "bs4", "talib", "pygsheets", "lxml.etree", "html5lib", "openpyxl")
    for name in names:
        importlib.import_module(name)
    return {"third_party_imports": list(names), "project_imports": []}


def check_native_indicators():
    import numpy as np
    import pandas as pd
    import talib

    assert metadata.version("TA-Lib") == "0.6.8", "Expected Python TA-Lib 0.6.8"
    native_version = talib.__ta_version__
    if isinstance(native_version, bytes):
        native_version = native_version.decode("ascii")
    assert native_version.split()[0] == "0.6.4", "Expected native TA-Lib 0.6.4"

    # Линейный ряд даёт независимый аналитический oracle, без production-кода.
    values = np.arange(1, 81, dtype=np.float64)
    sma = talib.SMA(values, timeperiod=5)
    rsi = talib.RSI(values, timeperiod=14)
    macd, signal, histogram = talib.MACD(values, fastperiod=12, slowperiod=26, signalperiod=9)
    for result, warmup, expected in (
        (sma, 4, values[4:] - 2), (rsi, 14, np.full(66, 100.0)),
        (macd, 33, np.full(47, 7.0)), (signal, 33, np.full(47, 7.0)),
        (histogram, 33, np.zeros(47)),
    ):
        assert result.shape == values.shape, "Indicator row count changed"
        assert np.isnan(result[:warmup]).all(), "Warm-up must remain NaN"
        assert np.isfinite(result[warmup:]).all(), "Finite indicator values are missing"
        # numpy.testing импортирует platform.machine(), который запускает WMIC.
        assert np.allclose(result[warmup:], expected, rtol=1e-12, atol=1e-12), "Native numeric values differ from the independent oracle"

    index = pd.Index(np.arange(100, 260, 2), name="row")
    series = pd.Series(values, index=index, name="close")
    series_macd = talib.MACD(series, fastperiod=12, slowperiod=26, signalperiod=9)
    for result, expected in (
        (talib.SMA(series, timeperiod=5), sma), (talib.RSI(series, timeperiod=14), rsi),
        (series_macd[0], macd), (series_macd[1], signal), (series_macd[2], histogram),
    ):
        assert isinstance(result, pd.Series), "TA-Lib must preserve the pandas bridge"
        assert result.index.equals(index), "The non-default index changed"
        assert np.allclose(result.to_numpy(), expected, rtol=1e-12, atol=1e-12, equal_nan=True), "The pandas bridge changed numeric values"
    return {"python_talib": "0.6.8", "native_talib": "0.6.4", "rows": 80, "warmup": {"SMA5": 4, "RSI14": 14, "MACD12_26_9": 33}, "rtol": 1e-12, "atol": 1e-12, "pandas_nondefault_index": "PASS"}


def check_html():
    import pandas as pd
    from bs4 import BeautifulSoup

    html = '<html><head><meta charset="utf-8"></head><body><table><tr><th>Тикер</th><th>Цена</th></tr><tr><td>ТЕСТ</td><td>12.5</td></tr></table></body></html>'
    raw = html.encode("utf-8")
    expected = pd.DataFrame({"Тикер": ["ТЕСТ"], "Цена": [12.5]})
    # Проверяем именно bytes из Response.content, как в dohodru_data.py.
    for flavor in (None, "bs4"):
        tables = pd.read_html(raw, flavor=flavor)
        assert len(tables) == 1, "Unexpected HTML table count"
        pd.testing.assert_frame_equal(tables[0], expected)
    soup = BeautifulSoup(html, "lxml")
    assert soup.find("td").get_text() == "ТЕСТ", "lxml changed Cyrillic text"
    return {"input": "UTF-8 bytes", "rows": 1, "default_parser": "PASS", "bs4_html5lib_fallback": "PASS", "beautifulsoup_lxml": "PASS"}


def check_file_backends(output_root):
    import pandas as pd

    output_root = isolated_path(output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    expected = pd.DataFrame({
        "open": [1.25, 2.5, 3.75], "close": [1.5, 2.75, 4.0],
        "high": [1.75, 3.0, 4.25], "low": [1.0, 2.25, 3.5],
        "value": [150.25, 0.0, 1200.5], "volume": [100, 0, 300],
        "begin": ["2026-01-01 10:00:00", "2026-01-01 10:00:00", "2026-01-02 10:00:00"],
        "end": ["2026-01-01 10:59:59", "2026-01-01 10:59:59", "2026-01-02 10:59:59"],
        "ticker": ["ТЕСТ", "SBER", "ТЕСТ"], "RSI14": [float("nan"), float("nan"), 65.25],
    })
    columns = ["open", "close", "high", "low", "value", "volume", "begin", "end", "ticker", "RSI14"]
    assert expected.columns.tolist() == columns, "The approved ten-column projection changed"
    csv_path = output_root / "fixture.csv"
    xlsx_path = output_root / "fixture.xlsx"
    # Служебный индекс текущих файлов сохраняется; он не часть десяти колонок.
    expected.to_csv(csv_path, encoding="utf-8")
    with pd.ExcelWriter(xlsx_path) as writer:
        assert writer.engine == "openpyxl", "Default XLSX writer must be openpyxl"
        expected.to_excel(writer)
    physical_csv = pd.read_csv(csv_path)
    actual_csv = pd.read_csv(csv_path, index_col=0)
    with pd.ExcelFile(xlsx_path) as reader:
        assert reader.engine == "openpyxl", "Default XLSX reader must be openpyxl"
        physical_xlsx = pd.read_excel(reader)
        actual_xlsx = pd.read_excel(reader, index_col=0)
    for physical in (physical_csv, physical_xlsx):
        assert physical.columns.tolist() == ["Unnamed: 0"] + columns, "The physical service index changed"
        assert physical.iloc[:, 0].tolist() == [0, 1, 2], "The physical service index values changed"
        assert str(physical.dtypes.iloc[0]) == "int64", "The service index type changed"
    for actual in (actual_csv, actual_xlsx):
        assert actual.columns.tolist() == columns, "Output column order changed"
        assert actual.index.equals(pd.RangeIndex(3)), "The recovered service index changed"
        assert not actual.duplicated(["ticker", "begin"]).any(), "Fixture keys changed"
        for ticker, rows in actual.groupby("ticker"):
            assert rows["begin"].is_monotonic_increasing, "Ticker chronology changed"
        assert actual["RSI14"].isna().tolist() == [True, True, False], "Warm-up NaNs changed"
        pd.testing.assert_frame_equal(actual, expected, check_dtype=True, check_exact=True)
    return {"rows": 3, "columns": columns, "keys": ["ticker", "begin"], "dtypes": {name: str(dtype) for name, dtype in expected.dtypes.items()}, "service_index_written": True, "rsi_nan_rows": 2, "ticker_chronology": "PASS", "default_xlsx_engine": "openpyxl", "csv": "PASS", "xlsx": "PASS"}


def _normalize_name(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def check_installed_graph(profile):
    assert profile in PROFILES, "Unknown environment profile"
    lock_path = PROJECT_ROOT / "requirements" / ("windows-cp314-" + profile + ".lock")
    lock_bytes = lock_path.read_bytes()
    pinned = {}
    hashes = {}
    current_name = None
    for line in lock_bytes.decode("utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = re.match(r"^([A-Za-z0-9][A-Za-z0-9._-]*)==([^\s;\\]+)(?:\s|$)", line)
        if match:
            current_name = _normalize_name(match.group(1))
            assert current_name not in pinned, "Duplicate package in the profile lock"
            pinned[current_name] = match.group(2)
            hashes[current_name] = []
            remainder = line[match.end():].strip()
        else:
            assert current_name and line.startswith("--hash="), "Profile lock must contain exact package pins and hashes only"
            remainder = line
        wheel_hashes = re.findall(r"--hash=sha256:([0-9a-f]{64})(?=\s|$)", remainder)
        hashes[current_name].extend(wheel_hashes)
        remainder = re.sub(r"--hash=sha256:[0-9a-f]{64}(?=\s|$)", "", remainder).strip()
        assert remainder in {"", "\\"}, "Unsupported or malformed profile lock entry"
    assert pinned, "Profile lock is empty"
    assert all(hashes.values()), "Every locked package needs a SHA-256 wheel hash"
    assert "pip" not in pinned, "Seed pip must remain outside the profile lock"
    expected = {**pinned, "pip": EXPECTED_PIP}
    installed = {}
    for distribution in metadata.distributions():
        name = _normalize_name(distribution.metadata["Name"])
        assert name not in installed, "Duplicate installed distribution"
        installed[name] = distribution.version
    missing = sorted(set(expected) - set(installed))
    extra = sorted(set(installed) - set(expected))
    changed = sorted(name for name in set(installed) & set(expected) if installed[name] != expected[name])
    assert not missing, "Missing packages: " + ", ".join(missing)
    assert not extra, "Undeclared packages: " + ", ".join(extra)
    assert not changed, "Wrong package versions: " + ", ".join(changed)
    return {"profile": profile, "lock": lock_path.relative_to(PROJECT_ROOT).as_posix(), "lock_sha256": hashlib.sha256(lock_bytes).hexdigest(), "wheel_hash_count": sum(len(value) for value in hashes.values()), "seed_pip": EXPECTED_PIP, "distribution_count": len(installed), "installed": dict(sorted(installed.items()))}


def check_guards():
    assert isinstance(subprocess.Popen, type), "Popen must remain a class for asyncio imports"
    attempts = [
        lambda: socket.getaddrinfo("localhost", 80),
        lambda: socket.create_connection(("localhost", 80)),
        lambda: subprocess.run(["F1-MUST-NOT-START"]),
        lambda: subprocess.Popen(["F1-MUST-NOT-START"]),
        lambda: os.system("F1-MUST-NOT-START"),
        lambda: sys.audit("subprocess.Popen", "F1-MUST-NOT-START", [], None, None),
        lambda: multiprocessing.Process(target=lambda: None).start(),
        lambda: sys.audit("_winapi.CreateProcess", None),
        lambda: sys.audit("os.fork"),
        lambda: sys.audit("os.forkpty"),
    ]
    if sys.platform == "win32":
        attempts.append(lambda: importlib.import_module("_winapi").CreateProcess())
    for attempt in attempts:
        try:
            attempt()
        except OfflineViolation:
            pass
        else:
            raise AssertionError("An offline guard did not block an operation")
    with socket.socket() as connection:
        for method in (connection.connect, connection.connect_ex):
            try:
                method(("localhost", 80))
            except OfflineViolation:
                pass
            else:
                raise AssertionError("A socket connection escaped the guard")
    install_guards()
    return {"dns": "BLOCKED", "connections": "BLOCKED", "child_processes": "BLOCKED", "multiprocessing": "BLOCKED", "audit_hook": "BLOCKED", "attempts": len(attempts) + 2}


def test_interpreter():
    check_interpreter()


def test_third_party_imports():
    check_imports()


def test_native_indicators():
    check_native_indicators()


def test_html_backends():
    check_html()


def test_csv_xlsx_backends(tmp_path):
    check_file_backends(tmp_path)


def test_installed_profile_graph():
    check_installed_graph(os.environ.get("MDS_ENVIRONMENT_PROFILE", "test"))


def test_offline_guards():
    check_guards()


def test_output_path_isolation():
    try:
        isolated_path(PROJECT_ROOT / "datasets")
    except ValueError:
        pass
    else:
        raise AssertionError("Output escaped the F1 directory")


def test_pytest_basetemp_protection():
    for path in (".f1/tmp/pytest", ".f1/tmp/pytest-runtime", ".f1/tmp/pytest-test", ".f1/tmp/pytest-dev"):
        assert validate_pytest_basetemp(path) == PROJECT_ROOT / path
    # Никакой cleanup не запускается: проверяем отказ до collection.
    for path in (
        ".f1", ".f1/tmp", ".f1/python", ".f1/venv-runtime-verify",
        ".f1/wheelhouse", ".f1/evidence", "datasets", ".f1/tmp/other",
        ".f1/tmp/pytest-unknown", ".f1/tmp/pytest-test/nested",
    ):
        try:
            validate_pytest_basetemp(path)
        except ValueError:
            pass
        else:
            raise AssertionError("Pytest could recursively delete a protected directory")


def test_pytest_basetemp_symlink_escape(monkeypatch):
    candidate = PROJECT_ROOT / ".f1" / "tmp" / "pytest-test"
    original_resolve = Path.resolve
    # Моделируем разрешение alias без создания Windows symlink и привилегий.
    for target in (PROJECT_ROOT / ".f1" / "python", PROJECT_ROOT / ".f1" / "tmp" / "pytest-dev"):
        def resolve_alias(path, *args, **kwargs):
            if path == candidate:
                return target
            return original_resolve(path, *args, **kwargs)

        monkeypatch.setattr(Path, "resolve", resolve_alias)
        try:
            validate_pytest_basetemp(candidate)
        except ValueError:
            pass
        else:
            raise AssertionError("A basetemp symlink alias escaped validation")


def test_pytest_cache_protection():
    for path in (".f1/cache", ".f1/cache/pytest", ".f1/pytest-cache-runtime", ".f1/pytest-cache-test", ".f1/pytest-cache-dev"):
        assert validate_pytest_cache(path) == PROJECT_ROOT / path
    for path in (".f1", ".f1/python", ".f1/venv-test-verify", ".f1/wheelhouse", ".f1/evidence", "datasets"):
        try:
            validate_pytest_cache(path)
        except ValueError:
            pass
        else:
            raise AssertionError("Pytest could write cache into a protected directory")


def _redacted_error(error):
    message = str(error)
    message = re.sub(r"https?://[^\s'\"]+", "<url>", message)
    return re.sub(r"(?:[A-Za-z]:[\\/]|/)[^\s'\"]+", "<path>", message)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", required=True, help="Disposable evidence directory inside .f1")
    parser.add_argument("--profile", choices=PROFILES, default="runtime")
    args = parser.parse_args(argv)
    try:
        output_root = isolated_path(args.output_root)
    except ValueError as error:
        parser.error(str(error))
    output_root.mkdir(parents=True, exist_ok=True)
    checks = (
        ("offline_guards", check_guards), ("interpreter", check_interpreter),
        ("third_party_imports", check_imports), ("native_indicators", check_native_indicators),
        ("html_backends", check_html), ("csv_xlsx_backends", lambda: check_file_backends(output_root)),
        ("installed_profile_graph", lambda: check_installed_graph(args.profile)),
    )
    started = time.perf_counter()
    report = {"profile": args.profile, "started_at_utc": datetime.now(timezone.utc).isoformat(), "network": "forbidden", "child_processes": "forbidden", "checks": {}}
    for name, check in checks:
        check_started = time.perf_counter()
        try:
            report["checks"][name] = {"status": "PASS", "evidence": check()}
        except Exception as error:
            report["checks"][name] = {"status": "FAIL", "error_type": type(error).__name__, "error": _redacted_error(error)}
        report["checks"][name]["elapsed_seconds"] = round(time.perf_counter() - check_started, 6)
        print(name + ": " + report["checks"][name]["status"])
    report["status"] = "PASS" if all(item["status"] == "PASS" for item in report["checks"].values()) else "FAIL"
    report["elapsed_seconds"] = round(time.perf_counter() - started, 6)
    (output_root / "environment.json").write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print("Evidence: environment.json; overall " + report["status"])
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
