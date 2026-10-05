"""Offline orchestration oracles for the guarded legacy entrypoints."""

import ast
import builtins
import inspect
from io import StringIO
import os
from pathlib import Path
import runpy
import sys
import time
import types

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _forbidden_conversion(*args, **kwargs):
    raise AssertionError("Entrypoint contract forbids notebook conversion")


def _load_script(stem, monkeypatch, **dependencies):
    path = PROJECT_ROOT / (stem + ".py")
    tree = ast.parse(path.read_text(encoding="utf-8"))
    # На red-before сначала проверяем структуру, не исполняя старые live-блоки.
    assert any(isinstance(node, ast.FunctionDef) and node.name == "main" for node in tree.body), "A guarded runtime wrapper is required"
    assert not any(isinstance(node, (ast.With, ast.For)) for node in tree.body), "Working blocks must not run during import"
    for name, module in dependencies.items():
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.setattr(os, "system", _forbidden_conversion)
    return runpy.run_path(str(path), run_name="f2_contract_" + stem)


def _dependency(**functions):
    module = types.ModuleType("f2_fixture_dependency")
    for name, function in functions.items():
        setattr(module, name, function)
    return module


def test_main_preserves_stage_order_and_arguments(monkeypatch, capsys):
    calls = []
    dependencies = {
        name: _dependency(main=lambda *args, stage=name: calls.append((stage, args)))
        for name in ("data_gathering", "dividends", "tech", "dohodru_data", "upload")
    }
    module = _load_script("main", monkeypatch, **dependencies)
    assert capsys.readouterr().out == ""
    assert module["main"]() is None
    root = str(PROJECT_ROOT)
    google_url = "https://docs.google.com/spreadsheets/d/1HXXoxcDVqIrWN6QEg5ij88AxcNAKT-G-xm2UTUfQe1Q/edit?usp=sharing"
    assert calls == [
        ("data_gathering", (root,)), ("dividends", ()), ("tech", (root,)),
        ("dohodru_data", ("https://www.dohod.ru/ik/analytics/dividend",)),
        ("upload", (root, google_url)),
    ]


def test_1year_preserves_four_reload_calls(monkeypatch):
    calls = []
    tickers = pd.DataFrame({"TRADE_CODE": ["FIXTURE"], "SUPERTYPE": ["Акции"]}, index=[8])
    dates = pd.DataFrame({"TRADE_CODE": ["FIXTURE"], "issue_date": ["2024-01-01"], "stopped_date": [None]})
    dependency = _dependency(
        moex_tickerlists=lambda root: calls.append(("tickers", root)) or tickers,
        build_tickers_dates=lambda frame, root: calls.append(("dates", frame, root)) or dates,
        full_reload=lambda *args: calls.append(("reload", args)),
    )
    module = _load_script("1year", monkeypatch, data_gathering=dependency)
    assert module["main"]() is None
    root = module["current_path"]
    assert calls[0] == ("tickers", root)
    assert calls[1][0] == "dates" and calls[1][1] is tickers and calls[1][2] == root
    assert tickers.index.tolist() == [0]
    expected = [
        (24, "2years_data_1d_interval", "часа"), (60, "2years_data_1h_interval", "минут"),
        (10, "2years_data_10m_interval", "минут"), (1, "2years_data_1m_interval", "минута"),
    ]
    assert len(calls) == 6
    for call, (interval, filename, word) in zip(calls[2:], expected):
        args = call[1]
        assert call[0] == "reload" and args[0] is tickers and args[6] is dates
        assert args[1:6] == (interval, 2, filename, word, root)


def test_all_preserves_existing_query_and_export_sequence(monkeypatch):
    tickers = pd.DataFrame({"TRADE_CODE": ["T" + str(i) for i in range(10)], "SUPERTYPE": ["Акции"] * 10})
    dates = pd.DataFrame({"TRADE_CODE": tickers["TRADE_CODE"], "issue_date": [pd.Timestamp("2001-01-01")] * 10, "stopped_date": [pd.Timestamp("2025-01-01")] * 10})
    row = {"open": 1.0, "close": 2.0, "high": 3.0, "low": 0.5, "value": 10.0, "volume": 5, "begin": "2025-01-01 00:00:00", "end": "2025-01-01 23:59:59", "ticker": "FIXTURE"}
    queries = []
    exports = []
    stages = []

    def query(*args):
        queries.append(args)
        return pd.DataFrame([row])

    dependency = _dependency(
        moex_tickerlists=lambda root: stages.append("tickers") or tickers,
        build_tickers_dates=lambda frame, root: stages.append("dates") or dates,
        moex_query=query,
    )
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    for method, extension in (("to_excel", "xlsx"), ("to_csv", "csv")):
        monkeypatch.setattr(pd.DataFrame, method, lambda frame, path, index=False, ext=extension: exports.append((ext, Path(path).name, index, frame.copy())))
    module = _load_script("all", monkeypatch, data_gathering=dependency)
    assert module["main"]() is None
    assert stages == ["tickers", "dates"]
    expected_queries = [("T9", "Акции", "2001-01-01", "2025-01-01", 24)]
    expected_queries += [("T" + str(i), "Акции", "2001-01-01", "2025-01-01", interval) for interval in (24, 60, 10, 1) for i in range(10)]
    assert queries == expected_queries
    names = ["all_data_1d_interval", "all_data_1h_interval", "all_data_10m_interval", "all_data_1m_interval"]
    assert [(ext, filename, index) for ext, filename, index, frame in exports] == [(ext, name + "." + ext, False) for name in names for ext in ("xlsx", "csv")]
    # F2 только переносит entrypoint: прежнее накопление df_full не исправляем здесь.
    assert [len(frame) for ext, filename, index, frame in exports] == [10, 10, 20, 20, 30, 30, 40, 40]
    for ext, filename, index, frame in exports:
        assert frame.columns.tolist() == list(row)
        assert frame.to_dict("records") == [row] * len(frame)


def test_ticker_dates_keeps_signature_lazy_header_and_schema(monkeypatch):
    module = _load_script("tests", monkeypatch, data_gathering=_dependency())
    function = module["get_ticker_dates"]
    assert list(inspect.signature(function).parameters) == ["moex_df"]
    reads = []
    requests = []

    def config_open(path, mode="r", encoding=None):
        assert path == "settings/user_agents.json" and mode == "r" and encoding == "utf-8"
        reads.append(path)
        return StringIO('{"chrome": ["OFFLINE_FIXTURE_AGENT"]}')

    def request(url, headers):
        requests.append((url, dict(headers)))
        return types.SimpleNamespace(status_code=200, json=lambda: {"dates": {"data": [["2001-01-01", "2025-01-01"]]}})

    monkeypatch.setattr(builtins, "open", config_open)
    monkeypatch.setattr(function.__globals__["requests"], "get", request)
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    frame = pd.DataFrame({"SUPERTYPE": ["Акции", "Облигации"], "TRADE_CODE": ["FIXTURE_SHARE", "FIXTURE_BOND"]})
    result, errors = function(frame)
    expected = pd.DataFrame({"ticker": ["FIXTURE_SHARE", "FIXTURE_BOND"], "date_from": ["2001-01-01"] * 2, "date_till": ["2025-01-01"] * 2})
    pd.testing.assert_frame_equal(result, expected)
    assert errors == [] and reads == ["settings/user_agents.json"]
    assert requests == [
        ("http://iss.moex.com/iss/history/engines/stock/markets/shares/securities/FIXTURE_SHARE/dates.json", {"User-Agent": "OFFLINE_FIXTURE_AGENT"}),
        ("http://iss.moex.com/iss/history/engines/stock/markets/bonds/securities/FIXTURE_BOND/dates.json", {"User-Agent": "OFFLINE_FIXTURE_AGENT"}),
    ]
    function(frame.iloc[:0])
    assert len(reads) == 1


def test_tests_wrapper_preserves_filter_schema_and_demo_call(monkeypatch):
    calls = []
    tickers = pd.DataFrame({"SUPERTYPE": ["Акции", "Облигации", "Акции"], "TRADE_CODE": ["FIXTURE_SHARE", "FIXTURE_BOND", ""]})
    expected_input = tickers.iloc[:2].copy()
    dates = pd.DataFrame({"ticker": ["FIXTURE_SHARE", "FIXTURE_BOND"], "date_from": ["2001-01-01"] * 2, "date_till": ["2025-01-01"] * 2})
    dependency = _dependency(
        moex_tickerlists=lambda root: calls.append(("tickers", root)) or tickers,
        moex_query=lambda *args: calls.append(("query", args)) or pd.DataFrame(),
    )
    module = _load_script("tests", monkeypatch, data_gathering=dependency)

    def fixture_dates(frame):
        pd.testing.assert_frame_equal(frame, expected_input)
        calls.append(("dates",))
        return dates, []

    monkeypatch.setitem(module["main"].__globals__, "get_ticker_dates", fixture_dates)
    exported = []
    monkeypatch.setattr(pd.DataFrame, "to_excel", lambda frame, path: exported.append((path, frame.copy())))
    assert module["main"]() is None
    assert calls == [
        ("tickers", module["current_path"]), ("dates",),
        ("query", ("SBER", "Акции", "2011-11-21", "2025-03-24", 24)),
    ]
    assert len(exported) == 1
    assert exported[0][0] == "{}/datasets/ticker_lists/ticker_dates.xlsx".format(module["current_path"])
    pd.testing.assert_frame_equal(exported[0][1], dates)


def test_count_check_preserves_counts(monkeypatch):
    frame = pd.DataFrame({"SUPERTYPE": ["Акции", "Депозитарные расписки", "Инвестиционные паи", "Ипотечные сертификаты участия", "Облигации", "Еврооблигации", "Другое"]})
    reads = []
    monkeypatch.setattr(pd, "read_excel", lambda path: reads.append(path) or frame)
    module = _load_script("count_check", monkeypatch)
    messages = []
    monkeypatch.setattr(builtins, "print", lambda value: messages.append(value))
    assert module["main"]() is None
    assert reads == ["{}/datasets/ticker_lists/moex_full.xlsx".format(module["current_path"])]
    assert messages == [7, 4, 2, 6]


def test_main_tests_keeps_classes_and_moves_live_example(monkeypatch):
    calls = []
    dependency = _dependency(moex=lambda *args: calls.append(("moex", args)) or pd.DataFrame())
    module = _load_script("main_tests", monkeypatch, data_gathering=dependency)
    assert list(inspect.signature(module["data_gathering___moex_query"].tests_moex_query).parameters) == ["self"]
    assert list(inspect.signature(module["data_gathering___moex"].tests_moex_).parameters) == ["self"]
    assert calls == []
    monkeypatch.setattr(module["unittest"], "main", lambda: calls.append(("unittest", ())))
    assert module["main"]() is None
    assert calls == [("moex", ("SBER", "Акции", 1, 24)), ("unittest", ())]
