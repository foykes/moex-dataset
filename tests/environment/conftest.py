"""Guard the dedicated F1 collection; never collect the legacy live scripts."""

from pathlib import Path
import sys

sys.dont_write_bytecode = True

# В isolated mode корень проекта не добавляем: доступны только наши helpers.
test_directory = str(Path(__file__).resolve().parent)
if test_directory not in sys.path:
    sys.path.insert(0, test_directory)

from _safety import install_guards, isolated_path, validate_pytest_basetemp, validate_pytest_cache

install_guards()

from datetime import datetime, timezone
import json
import os


_reports = []


def pytest_configure(config):
    import pytest

    try:
        if config.option.basetemp is None:
            raise ValueError("Pass --basetemp .f1/tmp/pytest for the F1 suite")
        validate_pytest_basetemp(config.option.basetemp)
        validate_pytest_cache(config.getini("cache_dir"))
        config._f1_output_root = isolated_path(os.environ.get("MDS_ENVIRONMENT_OUTPUT_ROOT", ".f1/evidence/pytest"))
        # Проверяем аргументы до collection: соседние live-модули недопустимы.
        allowed = Path(__file__).resolve().parent
        for argument in config.args:
            path = Path(argument.split("::", 1)[0]).resolve()
            if not path.is_relative_to(allowed):
                raise ValueError("Collect only tests/environment for the F1 suite")
    except ValueError as error:
        raise pytest.UsageError(str(error)) from error


def pytest_runtest_logreport(report):
    _reports.append({"test": report.nodeid, "phase": report.when, "outcome": report.outcome, "elapsed_seconds": round(report.duration, 6)})


def pytest_sessionfinish(session, exitstatus):
    output_root = getattr(session.config, "_f1_output_root", None)
    if output_root is None:
        return
    output_root.mkdir(parents=True, exist_ok=True)
    evidence = {"finished_at_utc": datetime.now(timezone.utc).isoformat(), "profile": os.environ.get("MDS_ENVIRONMENT_PROFILE", "test"), "exit_code": int(exitstatus), "network": "forbidden", "child_processes": "forbidden", "reports": _reports}
    (output_root / "pytest.json").write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
