"""Frozen dividend representation evidence; no live-source or quality-policy claims."""

import copy
from collections import Counter
import csv
from decimal import Decimal
from fractions import Fraction
import hashlib
import importlib.util
from io import BytesIO, StringIO
import json
import math
from pathlib import Path
import platform
import re
import sys
import xml.etree.ElementTree as ET
from zipfile import ZipFile

import numpy as np
import openpyxl
from openpyxl.utils.exceptions import IllegalCharacterError
import pandas as pd
import pytest
import mds_offline_guard as guard


ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / 'tests/fixtures/c2_quality'
BASELINE = {
    'sha': 'b423a7817e9a508fc167cdd47704d0b788252a8c',
    'python_blob': '7e63237528dbf43d2fe9dd3fba7ac277f477dc35',
    'notebook_blob': '2b297e85944ad7445889ec85c5ce4931271adc3f',
}
OUTPUT_COLUMNS = ['ISIN', 'TRADE_CODE', 'dt', 'value', 'currency']
PROFILE = {'platform': 'win32', 'implementation': 'CPython', 'python': '3.14.8',
           'pandas': '2.3.3', 'numpy': '2.3.4', 'openpyxl': '3.1.5', 'pytest': '8.4.2'}
XML_NS = {'m': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
NUMBER = re.compile(r'-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\Z')
SCALAR = re.compile(r'(?:-Infinity|Infinity|NaN|true|false|null|'
                    r'-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)')


def _reject_json_constant(token):
    raise ValueError('C2_03_NONSTANDARD_OUTER_JSON')


def _fixture_json(filename):
    return json.loads((FIXTURES / filename).read_text(encoding='utf-8'),
                      parse_constant=_reject_json_constant)


def _source_cases():
    return _fixture_json('source_cases.json')


def _assert_profile():
    actual = {'platform': sys.platform, 'implementation': platform.python_implementation(),
              'python': platform.python_version(), 'pandas': pd.__version__,
              'numpy': np.__version__, 'openpyxl': openpyxl.__version__,
              'pytest': pytest.__version__}
    assert actual == PROFILE, 'C2_03_PROFILE_MISMATCH'


def _typed(value):
    """Keep actual scalar types, including numpy cells and float bit patterns."""
    result = {'type': type(value).__module__ + '.' + type(value).__qualname__}
    if value is None:
        result['kind'] = 'NULL'
    elif isinstance(value, (bool, np.bool_)):
        result.update(kind='BOOLEAN', value=bool(value))
    elif isinstance(value, (int, np.integer)):
        result.update(kind='INTEGER', decimal=str(int(value)))
    elif isinstance(value, (float, np.floating)):
        number = float(value)
        if math.isnan(number):
            result.update(kind='FLOAT', representation='NAN')
        elif math.isinf(number):
            result.update(kind='FLOAT', representation='POSITIVE_INFINITY' if number > 0 else 'NEGATIVE_INFINITY')
        else:
            result.update(kind='FLOAT', representation='FINITE', hex=number.hex())
    elif isinstance(value, str):
        result.update(kind='STRING', text=value)
    elif type(value) is list:
        result.update(kind='LIST', items=[_typed(item) for item in value])
    elif type(value) is dict:
        result.update(kind='DICT', entries=[[_typed(key), _typed(item)] for key, item in value.items()])
    else:
        raise AssertionError('C2_03_UNSUPPORTED_CAPTURE_TYPE')
    return result


def _rows(rows):
    return [[_typed(cell) for cell in row] for row in rows]


def _missing(value):
    if type(value) in (list, dict):
        return False
    missing = pd.isna(value)
    assert isinstance(missing, (bool, np.bool_)), 'C2_03_NONSCALAR_NULL_MASK'
    return bool(missing)


def _frame(frame):
    values = [[frame.iat[row, column] for column in range(len(frame.columns))]
              for row in range(len(frame))]
    return {'columns': frame.columns.tolist(), 'dtypes': [str(dtype) for dtype in frame.dtypes],
            'cells': _rows(values), 'null_mask': [[_missing(value) for value in row] for row in values]}, values


def _exception(error):
    # Guard, assertion and I/O failures are infrastructure failures, not semantic evidence.
    if isinstance(error, (guard.OfflineViolation, AssertionError)):
        raise error
    if type(error) not in (ValueError, KeyError, TypeError, OverflowError,
                           json.JSONDecodeError, UnicodeDecodeError, IllegalCharacterError):
        raise error
    code = 'NON_DOMAIN_EXCEPTION'
    if type(error) is json.JSONDecodeError:
        code = 'JSON_DECODE_FAILED'
    elif type(error) is UnicodeDecodeError:
        code = 'UTF8_DECODE_FAILED'
    elif type(error) is IllegalCharacterError:
        code = 'XLSX_ILLEGAL_CHARACTER'
    elif type(error) is KeyError and error.args == ('dividends',):
        code = 'DIVIDEND_BLOCK_MISSING'
    elif error.args and type(error.args[0]) is str:
        candidate = error.args[0].split(':', 1)[0]
        if candidate in ('DIVIDEND_SOURCE_SCHEMA_INVALID', 'DIVIDEND_EMPTY_RELEASE_BLOCKED',
                         'DIVIDEND_SCOPE_EMPTY_UNVERIFIED'):
            code = candidate
    return {'type': type(error).__module__ + '.' + type(error).__qualname__, 'safe_code': code}


def _failed(stages, stage, error):
    stages[stage] = {'status': 'RAISED', 'exception': _exception(error)}


def _literal_node(raw, position=0):
    """Locate literal token spans without turning numeric source tokens into floats."""
    while position < len(raw) and raw[position].isspace():
        position += 1
    start = position
    if raw[position] in '{[':
        opening = raw[position]
        closing = '}' if opening == '{' else ']'
        children = {} if opening == '{' else []
        position += 1
        while True:
            while position < len(raw) and raw[position].isspace():
                position += 1
            if raw[position] == closing:
                return (raw[start:position + 1], children), position + 1
            key = None
            if opening == '{':
                key, position = json.JSONDecoder().raw_decode(raw, position)
                assert type(key) is str, 'C2_03_FIXTURE_OBJECT_KEY'
                while raw[position].isspace():
                    position += 1
                assert raw[position] == ':', 'C2_03_FIXTURE_COLON'
                position += 1
            node, position = _literal_node(raw, position)
            if opening == '{':
                assert key not in children, 'C2_03_FIXTURE_DUPLICATE_KEY'
                children[key] = node
            else:
                children.append(node)
            while raw[position].isspace():
                position += 1
            if raw[position] == closing:
                return (raw[start:position + 1], children), position + 1
            assert raw[position] == ',', 'C2_03_FIXTURE_COMMA'
            position += 1
    if raw[position] == '"':
        _, end = json.JSONDecoder().raw_decode(raw, position)
    else:
        match = SCALAR.match(raw, position)
        assert match is not None, 'C2_03_FIXTURE_SCALAR'
        end = match.end()
    return (raw[start:end], None), end


def _validate_source_literals(case):
    assert case['input_kind'] in ('STANDARD_JSON', 'NONSTANDARD_JSON_EXTENSION', 'PYTHON_RUNTIME')
    assert case['provenance'] in ('SYNTHETIC', 'HISTORICAL_AUDIT_RECONSTRUCTION')
    if case['input_kind'] == 'PYTHON_RUNTIME':
        assert case['provenance'] == 'SYNTHETIC'
        assert case['raw_json_utf8'] is None and type(case['runtime_rows']) is list
        assert all(item['raw_token'] is None and item['decimal_reference'] is None
                   and item['comparison_reason'] == 'NOT_APPLICABLE'
                   for item in case['value_inputs'])
        return
    assert case['runtime_rows'] is None and type(case['raw_json_utf8']) is str
    if case['input_kind'] == 'STANDARD_JSON':
        # Decimal accepts syntactically valid 1e309 without a binary-float overflow.
        json.loads(case['raw_json_utf8'], parse_float=Decimal, parse_constant=_reject_json_constant)
    else:
        assert case['provenance'] == 'SYNTHETIC'
        constants = []
        def explicit_extension(token):
            assert token in ('NaN', 'Infinity', '-Infinity'), 'C2_03_UNKNOWN_JSON_EXTENSION'
            constants.append(token)
            return token
        json.loads(case['raw_json_utf8'], parse_float=Decimal, parse_constant=explicit_extension)
        assert constants, 'C2_03_EXTENSION_TOKEN_REQUIRED'
        assert all(item['raw_token'] in ('NaN', 'Infinity', '-Infinity')
                   and item['decimal_reference'] is None
                   and item['comparison_reason'] == 'NONSTANDARD_NONFINITE_TOKEN'
                   for item in case['value_inputs'])
        assert Counter(constants) == Counter(item['raw_token'] for item in case['value_inputs'])
    node, end = _literal_node(case['raw_json_utf8'])
    assert not case['raw_json_utf8'][end:].strip(), 'C2_03_FIXTURE_TRAILING_TOKEN'
    table = node[1]['dividends'][1]
    columns = [json.loads(column[0]) for column in table['columns'][1]]
    value_column = columns.index('value')
    source_rows = table['data'][1]
    for item in case['value_inputs']:
        literal = source_rows[item['row_ordinal']][1][value_column][0]
        assert item['raw_token'] == literal, 'C2_03_LITERAL_TOKEN_MISMATCH'
        if item['decimal_reference'] is not None:
            assert NUMBER.fullmatch(literal), 'C2_03_NONNUMERIC_REFERENCE'
            assert Decimal(literal) == Decimal(item['decimal_reference']), 'C2_03_REFERENCE_MISMATCH'
            assert Decimal(item['decimal_reference']).is_finite()
            assert item['comparison_reason'] is None
        else:
            assert item['comparison_reason'] is not None


def _runtime_value(value):
    if type(value) is dict and set(value) == {'runtime_float'}:
        assert value['runtime_float'] in ('nan', 'inf', '-inf'), 'C2_03_RUNTIME_DESCRIPTOR'
        return {'nan': float('nan'), 'inf': float('inf'), '-inf': float('-inf')}[value['runtime_float']]
    if type(value) is list:
        return [_runtime_value(item) for item in value]
    if type(value) is dict:
        return {key: _runtime_value(item) for key, item in value.items()}
    return value


def _fraction(value):
    return {'numerator': str(value.numerator), 'denominator': str(value.denominator)}


def _numeric_fraction(value):
    if isinstance(value, (bool, np.bool_)):
        return None, 'OBSERVED_BOOLEAN'
    if isinstance(value, (int, np.integer)):
        return Fraction(int(value)), None
    if isinstance(value, (float, np.floating)):
        if not math.isfinite(float(value)):
            return None, 'OBSERVED_NONFINITE'
        return Fraction.from_float(float(value)), None
    return None, 'OBSERVED_NULL' if value is None else 'OBSERVED_NONNUMERIC'


def _comparison(reference, value=None, *, literal=None, reason=None):
    if reason is None:
        if literal is not None:
            if NUMBER.fullmatch(literal):
                observed, reason = Fraction(Decimal(literal)), None
            else:
                observed, reason = None, 'SERIALIZED_NONNUMERIC'
        else:
            observed, reason = _numeric_fraction(value)
    else:
        observed = None
    return {'observed_fraction': _fraction(observed) if observed is not None else None,
            'delta': _fraction(observed - reference) if observed is not None else None,
            'reason': reason}


def _xlsx_xml(path):
    with ZipFile(path) as archive:
        document = ET.fromstring(archive.read('xl/worksheets/sheet1.xml'))
    cells = []
    for cell in document.findall('.//m:sheetData/m:row/m:c', XML_NS):
        literal = cell.find('m:v', XML_NS)
        inline = cell.find('m:is', XML_NS)
        cells.append({'coordinate': cell.attrib['r'], 'data_type': cell.attrib.get('t'),
                      'literal_v': literal.text if literal is not None else None,
                      'inline_string': ''.join(text.text or '' for text in inline.findall('.//m:t', XML_NS))
                          if inline is not None else None})
    return cells


def _xlsx_readback(path, rows):
    workbook = openpyxl.load_workbook(path, data_only=False)
    try:
        sheet = workbook.active
        cells = []
        values = []
        for row in sheet.iter_rows(min_row=1, max_row=rows + 1, min_col=1, max_col=5):
            values.append([cell.value for cell in row])
            cells.extend({'coordinate': cell.coordinate, 'data_type': cell.data_type,
                          'value': _typed(cell.value)} for cell in row)
        return cells, values
    finally:
        workbook.close()


def _fresh_dividends():
    specification = importlib.util.spec_from_file_location('c2_03_fresh_dividends', ROOT / 'dividends.py')
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    assert module.divs_all == [], 'C2_03_FRESH_IMPORT_REQUIRED'
    return module


def characterize(case, monkeypatch, tmp_path):
    """Return JSON-safe observations; callers must supply an issued fixture root."""
    _assert_profile()
    _validate_source_literals(case)
    module = _fresh_dividends()
    directory = tmp_path / 'datasets/dividends'
    directory.mkdir(parents=True)
    module.current_path = str(tmp_path)
    paths = {suffix: directory / ('all.' + suffix) for suffix in ('xlsx', 'csv')}
    source_path = tmp_path / 'datasets/ticker_lists/moex_full.xlsx'
    search_url = 'https://iss.moex.com/iss/securities.json?' + module.urllib.parse.urlencode(
        {'q': case['isin'], 'iss.meta': 'off'})
    detail_url = 'https://iss.moex.com/iss/securities/{}/dividends.json'.format(
        module.urllib.parse.quote(case['ticker'], safe=''))
    search_raw = json.dumps({'securities': {'columns': ['secid', 'isin'],
                            'data': [[case['ticker'], case['isin']]]}}).encode('utf-8')
    detail_raw = case['raw_json_utf8'].encode('utf-8') if case['raw_json_utf8'] is not None else None
    stages = {name: {'status': 'NOT_REACHED'} for name in
              ('decode', 'parse', 'loader', 'accumulator', 'frame', 'xlsx', 'csv', 'main')}
    captured = {'decoded_rows': None, 'prepared_rows': None, 'frame_values': None}
    requests, reads, writer_calls = [], [], []
    active_url = None
    original_decode = module._decode_json
    original_parse = module._parse_dividend_rows
    original_loader = module._div_loader
    original_request = module._request_json
    original_excel = pd.DataFrame.to_excel
    original_csv = pd.DataFrame.to_csv

    def opened(url, *, timeout=None):
        nonlocal active_url
        assert url in (search_url, detail_url), 'C2_03_UNEXPECTED_REQUEST'
        assert isinstance(timeout, (float, int)) and 0 < timeout <= 10
        requests.append({'operation': 'OPEN', 'endpoint': 'SEARCH' if url == search_url else 'DETAIL'})
        active_url = url
        assert url != detail_url or detail_raw is not None, 'C2_03_RUNTIME_RAW_REQUEST'
        return BytesIO(search_raw if url == search_url else detail_raw)

    def decode(raw):
        if active_url != detail_url:
            return original_decode(raw)
        assert raw == detail_raw, 'C2_03_RAW_BYTES_CHANGED'
        try:
            result = original_decode(raw)
        except Exception as error:
            _failed(stages, 'decode', error)
            raise
        stages['decode'] = {'status': 'RETURNED'}
        captured['decoded_rows'] = copy.deepcopy(result['dividends']['data'])
        return result

    def request(url, deadline, logging_context, instrument, code, state):
        if case['input_kind'] != 'PYTHON_RUNTIME' or url != detail_url:
            return original_request(url, deadline, logging_context, instrument, code, state)
        requests.append({'operation': 'ALREADY_DECODED_RUNTIME', 'endpoint': 'DETAIL'})
        stages['decode'] = {'status': 'NOT_REACHED', 'reason': 'ALREADY_DECODED_RUNTIME'}
        state.clear()
        state.update(started=0.0, category='SOURCE', reason='', attempt=1, phase='')
        rows = _runtime_value(copy.deepcopy(case['runtime_rows']))
        captured['decoded_rows'] = copy.deepcopy(rows)
        return {'dividends': {'columns': ['registryclosedate', 'value', 'currencyid'], 'data': rows}}

    def parse(data, isin, ticker):
        try:
            result = original_parse(data, isin, ticker)
        except Exception as error:
            _failed(stages, 'parse', error)
            raise
        stages['parse'] = {'status': 'RETURNED'}
        captured['prepared_rows'] = copy.deepcopy(result)
        return result

    def loader(*args, **kwargs):
        try:
            result = original_loader(*args, **kwargs)
        except Exception as error:
            _failed(stages, 'loader', error)
            raise
        stages['loader'] = {'status': 'RETURNED'}
        stages['accumulator'] = {'status': 'RETURNED'}
        return result

    def read_excel(path, *args, **kwargs):
        assert Path(path) == source_path and args == () and kwargs == {}, 'C2_03_UNEXPECTED_SCOPE_READ'
        reads.append('datasets/ticker_lists/moex_full.xlsx')
        return pd.DataFrame([[case['ticker'], case['isin']]], columns=['TRADE_CODE', 'ISIN'])

    def write(frame, path, args, kwargs, suffix, serializer):
        assert Path(path) == paths[suffix] and args == () and kwargs == {'index': False}, 'C2_03_WRITER_CONTRACT'
        assert len(writer_calls) == (0 if suffix == 'xlsx' else 1), 'C2_03_WRITER_ORDER'
        snapshot, values = _frame(frame)
        assert snapshot['columns'] == OUTPUT_COLUMNS
        if captured['frame_values'] is None:
            captured['frame'] = snapshot
            captured['frame_values'] = values
            stages['frame'] = {'status': 'RETURNED'}
        else:
            assert snapshot == captured['frame'], 'C2_03_FRAME_CHANGED_BETWEEN_WRITERS'
        writer_calls.append({'format': suffix, 'destination': 'datasets/dividends/all.' + suffix,
                             'kwargs': {'index': False}})
        try:
            result = serializer(frame, path, **kwargs)
        except Exception as error:
            _failed(stages, suffix, error)
            raise
        stages[suffix] = {'status': 'RETURNED'}
        return result

    def to_excel(frame, path, *args, **kwargs):
        return write(frame, path, args, kwargs, 'xlsx', original_excel)

    def to_csv(frame, path, *args, **kwargs):
        return write(frame, path, args, kwargs, 'csv', original_csv)

    def forbidden_wait(delay):
        raise AssertionError('C2_03_UNEXPECTED_RETRY_WAIT')

    with monkeypatch.context() as patches:
        patches.setattr(module, '_network_clock', lambda: 0.0)
        patches.setattr(module, '_network_wall_clock', lambda: 0.0)
        patches.setattr(module, '_retry_wait', forbidden_wait)
        patches.setattr(module.urllib.request, 'urlopen', opened)
        patches.setattr(module, '_decode_json', decode)
        patches.setattr(module, '_request_json', request)
        patches.setattr(module, '_parse_dividend_rows', parse)
        patches.setattr(module, '_div_loader', loader)
        patches.setattr(pd, 'read_excel', read_excel)
        patches.setattr(pd.DataFrame, 'to_excel', to_excel)
        patches.setattr(pd.DataFrame, 'to_csv', to_csv)
        try:
            assert module.main() is None, 'C2_03_MAIN_RETURN_CHANGED'
        except Exception as error:
            _failed(stages, 'main', error)
        else:
            stages['main'] = {'status': 'RETURNED'}

    result = {'case_id': case['case_id'], 'input_kind': case['input_kind'], 'provenance': case['provenance'],
              'raw_input': {'raw_json_utf8': case['raw_json_utf8'],
                            'value_inputs': copy.deepcopy(case['value_inputs'])},
              'stages': stages, 'requests': requests, 'reads': reads, 'writer_calls': writer_calls,
              'decoded_rows': _rows(captured['decoded_rows']) if captured['decoded_rows'] is not None else None,
              'prepared_rows': _rows(captured['prepared_rows']) if captured['prepared_rows'] is not None else None,
              'accumulator_rows': _rows(module.divs_all), 'frame': captured.get('frame'),
              'artifact_presence': {suffix: path.exists() for suffix, path in paths.items()},
              'csv': None, 'xlsx_xml': None, 'xlsx_readback': None}
    csv_fields = None
    readback_values = None
    if stages['csv']['status'] == 'RETURNED':
        literal_csv = paths['csv'].read_bytes().decode('utf-8')
        csv_fields = list(csv.reader(StringIO(literal_csv, newline='')))
        result['csv'] = {'literal_utf8': literal_csv, 'fields': csv_fields}
    if stages['xlsx']['status'] == 'RETURNED':
        result['xlsx_xml'] = _xlsx_xml(paths['xlsx'])
        result['xlsx_readback'], readback_values = _xlsx_readback(paths['xlsx'], len(module.divs_all))
    result['deltas'] = []
    xml_cells = {cell['coordinate']: cell for cell in result['xlsx_xml'] or []}
    for item in case['value_inputs']:
        ordinal = item['row_ordinal']
        reference = Fraction(Decimal(item['decimal_reference'])) if item['decimal_reference'] is not None else None
        delta = {'row_ordinal': ordinal, 'source_fraction': _fraction(reference) if reference is not None else None,
                 'comparisons': {}}
        for name, rows, column in [('decoded', captured['decoded_rows'], 1),
                                    ('accumulator', module.divs_all, 3),
                                    ('frame', captured['frame_values'], 3)]:
            reason = item['comparison_reason'] if reference is None else None
            if reason is None and (rows is None or ordinal >= len(rows)):
                reason = 'STAGE_NOT_REACHED'
            delta['comparisons'][name] = _comparison(reference, rows[ordinal][column] if reason is None else None,
                                                    reason=reason)
        reason = item['comparison_reason'] if reference is None else ('STAGE_NOT_REACHED' if csv_fields is None else None)
        delta['comparisons']['csv'] = _comparison(reference, literal=csv_fields[ordinal + 1][3] if reason is None else None,
                                                reason=reason)
        cell = xml_cells.get('D' + str(ordinal + 2))
        reason = item['comparison_reason'] if reference is None else ('STAGE_NOT_REACHED' if result['xlsx_xml'] is None else None)
        if reason is None and (cell is None or cell['data_type'] not in (None, 'n') or cell['literal_v'] is None):
            reason = 'XLSX_CELL_NONNUMERIC_OR_ABSENT'
        delta['comparisons']['xlsx_xml'] = _comparison(reference, literal=cell['literal_v'] if reason is None else None,
                                                     reason=reason)
        reason = item['comparison_reason'] if reference is None else ('STAGE_NOT_REACHED' if readback_values is None else None)
        delta['comparisons']['xlsx_readback'] = _comparison(reference,
            readback_values[ordinal + 1][3] if reason is None else None, reason=reason)
        result['deltas'].append(delta)
    # Reject nonstandard NaN/Infinity in the evidence itself.
    json.dumps(result, allow_nan=False)
    return result


def test_fixture_and_source_identity():
    _assert_profile()
    source = _source_cases()
    assert source['fixture_version'] == 1 and source['baseline'] == BASELINE
    assert source['columns'] == ['registryclosedate', 'value', 'currencyid']
    ids = [case['case_id'] for case in source['cases']]
    assert len(ids) == len(set(ids)) == 41
    for filename, field in [('dividends.py', 'python_blob'), ('dividends.ipynb', 'notebook_blob')]:
        # Windows checkout CRLF is normalized by Git for these tracked text files.
        raw = (ROOT / filename).read_bytes().replace(b'\r\n', b'\n')
        blob = hashlib.sha1(b'blob ' + str(len(raw)).encode('ascii') + b'\0' + raw).hexdigest()
        assert blob == BASELINE[field], 'C2_03_SOURCE_DRIFT'
    oracles = _fixture_json('export_oracles.json')
    assert oracles['fixture_version'] == 1 and oracles['baseline'] == BASELINE
    assert oracles['runtime_profile'] == PROFILE
    assert set(oracles['cases']) == set(ids), 'C2_03_ORACLE_INVENTORY_MISMATCH'


@pytest.mark.parametrize('case', _source_cases()['cases'], ids=lambda case: case['case_id'])
def test_frozen_export_characterization(case, monkeypatch, tmp_path):
    actual = characterize(case, monkeypatch, tmp_path)
    oracles = _fixture_json('export_oracles.json')
    assert actual == oracles['cases'][case['case_id']]
    failure_stage = case['expected_failure_stage']
    assert actual['stages']['main']['status'] == ('RAISED' if failure_stage else 'RETURNED')
    if failure_stage:
        assert actual['stages'][failure_stage]['status'] == 'RAISED'
        assert actual['stages']['csv']['status'] == 'NOT_REACHED'
    if actual['stages']['parse']['status'] == 'RETURNED':
        decoded = actual['decoded_rows']
        expected = [[_typed(case['isin']), _typed(case['ticker']), *row] for row in decoded]
        assert actual['prepared_rows'] == actual['accumulator_rows'] == expected
    else:
        assert actual['prepared_rows'] is None and actual['accumulator_rows'] == []


def test_long_literal_exposes_exact_binary_and_serialized_loss(monkeypatch, tmp_path):
    case = next(case for case in _source_cases()['cases'] if case['case_id'] == 'long_decimal')
    actual = characterize(case, monkeypatch, tmp_path)
    assert case['value_inputs'][0]['raw_token'] == '0.10000000000000001'
    reference = Fraction(10000000000000001, 100000000000000000)
    binary_delta = Fraction(3602879701896397, 36028797018963968) - reference
    serialized_delta = Fraction(1, 10) - reference
    assert binary_delta != 0 and serialized_delta != 0
    observed = actual['deltas'][0]
    assert observed['source_fraction'] == _fraction(reference)
    for stage in ('decoded', 'accumulator', 'frame', 'xlsx_readback'):
        assert observed['comparisons'][stage]['delta'] == _fraction(binary_delta)
        assert observed['comparisons'][stage]['reason'] is None
    for stage in ('csv', 'xlsx_xml'):
        assert observed['comparisons'][stage]['delta'] == _fraction(serialized_delta)
        assert observed['comparisons'][stage]['reason'] is None


def test_public_loader_returns_none_and_preserves_decoded_row(monkeypatch):
    _assert_profile()
    case = next(case for case in _source_cases()['cases'] if case['case_id'] == 'healthy')
    module = _fresh_dividends()
    search_url = 'https://iss.moex.com/iss/securities.json?q=RU0009029540&iss.meta=off'
    detail_url = 'https://iss.moex.com/iss/securities/SBER/dividends.json'
    search = {'securities': {'columns': ['secid', 'isin'], 'data': [[case['ticker'], case['isin']]]}}
    responses = {search_url: json.dumps(search).encode('utf-8'),
                 detail_url: case['raw_json_utf8'].encode('utf-8')}
    calls = []
    def opened(url, *, timeout=None):
        assert url in responses and 0 < timeout <= 10, 'C2_03_UNEXPECTED_REQUEST'
        calls.append(url)
        return BytesIO(responses[url])
    def forbidden_wait(delay):
        raise AssertionError('C2_03_UNEXPECTED_RETRY_WAIT')
    monkeypatch.setattr(module.urllib.request, 'urlopen', opened)
    monkeypatch.setattr(module, '_network_clock', lambda: 0.0)
    monkeypatch.setattr(module, '_network_wall_clock', lambda: 0.0)
    monkeypatch.setattr(module, '_retry_wait', forbidden_wait)
    assert module.div_loader(case['isin'], case['ticker']) is None
    assert calls == [search_url, detail_url]
    expected = [[case['isin'], case['ticker'], '2024-07-11', 17.35, 'RUB']]
    assert _rows(module.divs_all) == _rows(expected)
