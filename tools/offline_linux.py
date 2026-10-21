"""Separate closed LNX-03 and D2 Linux consumers; no general launcher API.

Only the verified helper creates its short-lived runner ticket. Local portable
tests exercise parsers/refusals; native acceptance requires the isolated Linux
worker and actual process/F-LOG observations.
"""

import hashlib
import importlib
import json
import os
from pathlib import Path
import re
import stat
import sys


SOURCE_PATHS = (
    '.github/lnx02/verify_env.py', '.github/workflows/d2-linux-offline.yml',
    'tools/offline_linux.py', 'tools/offline_guard_linux.py',
    'tests/linux_bootstrap/conftest.py', 'tests/linux_bootstrap/native_worker.py',
    'tests/linux_bootstrap/test_environment_contract.py',
    'tests/linux_bootstrap/test_admission.py', 'tests/linux_bootstrap/test_spawn_contract.py',
    'docs/d2_linux_ci.md', 'docs/testing.md', 'docs/coordination.md',
    'run_logging.py', 'requirements/linux-cp314-runtime.lock',
    'requirements/linux-cp314-test.lock', 'requirements/linux-cp314-dev.lock',
)
TESTS = (
    'tests/linux_bootstrap/test_environment_contract.py',
    'tests/linux_bootstrap/test_admission.py',
    'tests/linux_bootstrap/test_spawn_contract.py',
)
CASES = (
    'success', 'child_error', 'exit23', 'hang',
    'bootstrap_replay_failure', 'bootstrap_import_failure',
    'bootstrap_unpickle_failure', 'bootstrap_pipe_failure', 'foreign_fd',
    'listener_failure', 'ack_failure', 'auxiliary_settlement',
    'canary_positive_control',
)
GRANT_KEYS = {
    'approved', 'consumer', 'repository', 'sha', 'base', 'event', 'ref',
    'actor', 'triggering_actor', 'workflow', 'job', 'run_number', 'attempt', 'ticket_id', 'run_id',
}


class Refusal(RuntimeError):
    """Only fixed codes, never exception payloads, leave this entrypoint."""


def require(condition, code):
    if not condition:
        raise Refusal(code)


def json_bytes(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'),
                       ensure_ascii=True, allow_nan=False) + '\n').encode('ascii')


def no_duplicates(items):
    result = {}
    for key, value in items:
        require(key not in result, 'lnx03-runner-json-duplicate')
        result[key] = value
    return result


def real_path(value, directory=False, private=False):
    require(type(value) is str and Path(value).is_absolute(), 'lnx03-runner-path-invalid')
    path = Path(os.path.abspath(value))
    # Inspect every component before accepting resolve(): symlink aliases must
    # not become authority merely by resolving to a legitimate owned directory.
    for component in (path, *path.parents):
        info = component.lstat()
        require(not stat.S_ISLNK(info.st_mode), 'lnx03-runner-path-alias')
    info = path.lstat()
    require(path.resolve() == path, 'lnx03-runner-path-alias')
    require(stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode),
            'lnx03-runner-path-kind')
    if not directory:
        require(info.st_nlink == 1, 'lnx03-runner-hardlink-refused')
    if hasattr(os, 'getuid'):
        require(info.st_uid == os.getuid(), 'lnx03-runner-path-owner')
        if private:
            require(stat.S_IMODE(info.st_mode) == (0o700 if directory else 0o600),
                    'lnx03-runner-path-permission')
    return path


def read_record(path):
    path = real_path(str(path), private=True)
    require(path.stat().st_size <= 65536, 'lnx03-runner-ticket-bound')
    flags = os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0)
    descriptor = os.open(path, flags)
    with os.fdopen(descriptor, 'rb') as handle:
        info = os.fstat(handle.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size <= 65536,
                'lnx03-runner-ticket-bound')
        data = handle.read(65537)
    require(len(data) <= 65536, 'lnx03-runner-ticket-bound')
    return data, json.loads(data, object_pairs_hook=no_duplicates)


def validate_authority(value, sha):
    require(type(value) is dict and set(value) == GRANT_KEYS | {'run_id'},
            'lnx03-runner-authority-schema')
    require(value['approved'] is True and value['consumer'] == 'linux-bootstrap'
            and value['repository'] == 'foykes/moex-dataset'
            and value['actor'] == value['triggering_actor'] == 'foykes'
            and value['workflow'] == '.github/workflows/d2-linux-offline.yml'
            and value['job'] == 'linux-bootstrap' and value['sha'] == sha,
            'lnx03-runner-authority-identity')
    for key, width in (('sha', 40), ('base', 40), ('ticket_id', 64)):
        require(type(value[key]) is str and re.fullmatch('[0-9a-f]{' + str(width) + '}', value[key]),
                'lnx03-runner-authority-identity')
    require(type(value['attempt']) is int and value['attempt'] == 2
            and type(value['run_number']) is int and value['run_number'] > 0
            and type(value['run_id']) is str and re.fullmatch('[1-9][0-9]*', value['run_id']),
            'lnx03-runner-authority-identity')
    require((value['event'] == 'push' and value['ref'] == 'refs/heads/codex/lnx-03-d2-bootstrap')
            or (value['event'] == 'workflow_dispatch' and value['ref'] == 'refs/heads/main'
                and value['base'] == value['sha']), 'lnx03-runner-authority-identity')


def validate_ticket(env, repo):
    """Pure file/identity admission, before pytest or a native process can run."""
    repo = real_path(str(repo), directory=True)
    value = env.get('MDS_LNX03_RUN_TICKET')
    digest = env.get('MDS_LNX03_RUN_TICKET_SHA256')
    require(type(digest) is str and re.fullmatch('[0-9a-f]{64}', digest),
            'lnx03-runner-ticket-missing')
    ticket_path = real_path(value, private=True)
    data, ticket = read_record(ticket_path)
    require(hashlib.sha256(data).hexdigest() == digest, 'lnx03-runner-ticket-digest')
    require(type(ticket) is dict and set(ticket) == {
        'schema_version', 'root', 'nonce', 'repo', 'parent_pid', 'source_sha',
        'source_files_sha256', 'authority', 'result', 'native_root'},
        'lnx03-runner-ticket-schema')
    require(ticket['schema_version'] == 'lnx03-runner-ticket-v1', 'lnx03-runner-ticket-schema')
    require(type(ticket['parent_pid']) is int and ticket['parent_pid'] == os.getppid(),
            'lnx03-runner-parent-mismatch')
    require(type(ticket['nonce']) is str and re.fullmatch('[0-9a-f]{32}', ticket['nonce']),
            'lnx03-runner-owner-invalid')
    root = real_path(ticket['root'], directory=True, private=True)
    require(root.name.startswith('lnx03-') and ticket_path == root / 'jobs' / 'application-ticket.json',
            'lnx03-runner-ticket-foreign')
    _, marker = read_record(root / '.lnx02-owner.json')
    require(marker == {'nonce': ticket['nonce'], 'purpose': 'LNX-03'}, 'lnx03-runner-owner-invalid')
    require(ticket['repo'] == str(repo) and type(ticket['source_sha']) is str
            and re.fullmatch('[0-9a-f]{40}', ticket['source_sha']), 'lnx03-runner-source-invalid')
    validate_authority(ticket['authority'], ticket['source_sha'])
    source = ticket['source_files_sha256']
    require(type(source) is dict and set(source) == set(SOURCE_PATHS), 'lnx03-runner-source-invalid')
    for name in SOURCE_PATHS:
        path = real_path(str(repo / name))
        require(type(source[name]) is str and re.fullmatch('[0-9a-f]{64}', source[name])
                and hashlib.sha256(path.read_bytes()).hexdigest() == source[name],
                'lnx03-runner-source-invalid')
    native = real_path(ticket['native_root'], directory=True, private=True)
    require(native == root / 'fixtures' / 'native', 'lnx03-runner-native-root-invalid')
    require(ticket['result'] == str(root / 'jobs' / 'application-tests.json'),
            'lnx03-runner-result-path-invalid')
    require(not Path(ticket['result']).exists(), 'lnx03-runner-result-exists')
    real_path(str(root / 'jobs'), directory=True, private=True)
    return ticket


def claim_ticket(ticket):
    """A runner ticket cannot be reused, including after a failed test run."""
    path = Path(ticket['root']) / 'jobs' / 'application-ticket.claimed'
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0)
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError:
        raise Refusal('lnx03-runner-ticket-replayed') from None
    with os.fdopen(descriptor, 'wb') as handle:
        handle.write(json_bytes({'ticket_id': ticket['authority']['ticket_id'],
                                 'parent_pid': ticket['parent_pid']}))


class Counts:
    def __init__(self):
        self.collected = 0
        self.deselected = 0
        self.passed_ids = set()
        self.failed_ids = set()

    def pytest_collection_finish(self, session):
        self.collected = len(session.items)

    def pytest_deselected(self, items):
        self.deselected += len(items)

    def pytest_runtest_logreport(self, report):
        if report.failed:
            self.failed_ids.add(report.nodeid)
        if report.when == 'call' and report.passed:
            self.passed_ids.add(report.nodeid)

    def pytest_collectreport(self, report):
        if report.failed:
            self.failed_ids.add(report.nodeid)


def run_tests(ticket, repo):
    # -I ignores PYTHON* options. Set this explicit flag before imports, and do
    # not mutate the disposable environment or tracked source with __pycache__.
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(repo))
    os.environ['LNX03_SOURCE_SHA'] = ticket['source_sha']
    os.environ['LNX03_NATIVE_ROOT'] = ticket['native_root']
    os.environ['PYTEST_DISABLE_PLUGIN_AUTOLOAD'] = '1'
    os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
    guard_module = importlib.import_module('tools.offline_guard_linux')
    guard = guard_module.install_process_guard()
    guard_module.attest_runtime()
    # -S prevents .pth/sitecustomize execution before admission. Add only this
    # owned venv's actual package directories; never execute its .pth files.
    import sysconfig
    prefix = real_path(str(Path(sys.prefix).absolute()), directory=True)
    require(prefix == Path(ticket['root']) / 'dev-replay', 'lnx03-runner-environment-foreign')
    paths = sysconfig.get_paths()
    for key in ('purelib', 'platlib'):
        site = real_path(paths[key], directory=True)
        require(site.is_relative_to(prefix), 'lnx03-runner-site-foreign')
        if str(site) not in sys.path:
            sys.path.append(str(site))
    require(not os.environ.get('PYTEST_ADDOPTS') and not os.environ.get('PYTEST_PLUGINS'),
            'lnx03-runner-pytest-override')
    import pytest
    counts = Counts()
    base = Path(ticket['root']) / 'fixtures' / 'pytest'
    require(not base.exists(), 'lnx03-runner-fixture-exists')
    args = [str(repo / name) for name in TESTS] + [
        '-c', '/dev/null', '--rootdir', str(repo),
        '--confcutdir', str(repo / 'tests/linux_bootstrap'),
        '--import-mode=importlib', '--strict-markers',
        '-q', '--tb=no', '-p', 'no:cacheprovider', '--basetemp', str(base),
    ]
    exit_code = int(pytest.main(args, plugins=[counts]))
    cases = []
    expected_conftest = repo / 'tests/linux_bootstrap/conftest.py'
    for module in tuple(sys.modules.values()):
        filename = getattr(module, '__file__', None)
        if filename and Path(filename).absolute() == expected_conftest:
            cases = getattr(module, 'NATIVE_RESULTS', [])
            break
    passed = len(counts.passed_ids - counts.failed_ids)
    failed = len(counts.failed_ids)
    # The independent pytest oracles must run, not merely a native helper's
    # status bit. Missing cases, collection errors, deselections and denials all
    # prevent successful application admission.
    complete = (exit_code == 0 and failed == 0 and counts.collected > len(CASES)
                and passed == counts.collected and counts.deselected == 0
                and type(cases) is list and len(cases) == len(CASES)
                and {item.get('case') for item in cases} == set(CASES)
                and all(item.get('status') == 'PASS' for item in cases)
                and not guard.denials)
    return {
        'schema_version': 'lnx03-runner-result-v1', 'status': 'PASS' if complete else 'FAIL',
        'source_sha': ticket['source_sha'], 'collected': counts.collected,
        'passed': passed, 'failed': failed, 'deselected': counts.deselected,
        'exit_code': 0 if complete else (exit_code or 1), 'native_cases': cases,
        'failure_code': None if complete else 'lnx03-application-oracle-failed',
    }


def admit_export_bytes(data):
    """An actual fixed-canary/URL export gate, also tested by a native fault."""
    require(type(data) is bytes and len(data) <= 1024 * 1024, 'lnx03-runner-result-bound')
    require(b'LNX03_NATIVE_PRIVATE_CANARY_7fe94d31' not in data
            and re.search(rb'(?i)(https?://|file://|Traceback [(]most recent)', data) is None,
            'lnx03-runner-export-refused')
    return data


def write_result(ticket, result):
    data = admit_export_bytes(json_bytes(result))
    # Full serialization precedes create; diagnostics are never silently cut.
    require(len(data) <= 1024 * 1024, 'lnx03-runner-result-bound')
    descriptor = os.open(ticket['result'], os.O_WRONLY | os.O_CREAT | os.O_EXCL
                         | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    with os.fdopen(descriptor, 'wb') as handle:
        handle.write(data)


def main():
    if sys.argv[1:] == ['--lane', 'd2-linux']:
        return d2_main()
    ticket = None
    try:
        require(sys.argv[1:] == ['--lane', 'linux-bootstrap'], 'lnx03-runner-cli-refused')
        require(sys.platform == 'linux' and sys.implementation.name == 'cpython'
                and sys.version_info[:3] == (3, 14, 8)
                and sys.flags.isolated and sys.flags.no_site and sys.flags.dont_write_bytecode,
                'lnx03-runner-platform-unsupported')
        repo = Path(__file__).absolute().parents[1]
        ticket = validate_ticket(os.environ, repo)
        claim_ticket(ticket)
        import ctypes
        import signal
        library = ctypes.CDLL(None, use_errno=True)
        library.prctl.argtypes = (ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong)
        library.prctl.restype = ctypes.c_int
        require(library.prctl(1, signal.SIGKILL, 0, 0, 0) == 0 and os.getppid() == ticket['parent_pid'],
                'lnx03-runner-parent-death-bind-failed')
        result = run_tests(ticket, repo)
        write_result(ticket, result)
        return result['exit_code']
    except BaseException:
        # Native output is DEVNULL from process birth; the local refusal surface
        # also contains no arguments, paths, exception text or tracebacks.
        if ticket is not None:
            try:
                write_result(ticket, {
                    'schema_version': 'lnx03-runner-result-v1', 'status': 'INCOMPLETE',
                    'source_sha': ticket['source_sha'], 'collected': 0, 'passed': 0,
                    'failed': 0, 'deselected': 0, 'exit_code': 1, 'native_cases': [],
                    'failure_code': 'lnx03-runner-incomplete',
                })
            except BaseException:
                pass
        print('LNX03_RUNNER_REFUSED')
        return 1


D2_PATHS = (
    'upload.py', 'upload.ipynb', 'main.py', '.github/lnx02/verify_env.py',
    '.github/workflows/d2-linux-offline.yml', 'tools/offline_linux.py',
    'tools/offline_guard_linux.py', 'tests/d2/conftest.py', 'tests/d2/d2_worker.py',
    'tests/d2/test_supervision.py', 'tests/d2/test_targets.py',
    'tests/d2/test_logging_contract.py', 'tests/d2/test_guard_admission.py',
    'tests/fixtures/d2/expected.json', 'docs/d2_supervision.md',
    'docs/testing.md', 'docs/coordination.md',
)
D2_TESTS = ('tests/d2/test_supervision.py', 'tests/d2/test_targets.py',
            'tests/d2/test_logging_contract.py', 'tests/d2/test_guard_admission.py')
D2_SOURCE_PATHS = tuple(dict.fromkeys(SOURCE_PATHS + D2_PATHS))
D2_DEVELOPMENT_BASE = '25dee502a68128aea752f24ff67dfcb16b615a36'
D2_NODE_IDS_SHA256 = '4072cf243d4fc1a556a68b8ad923d23d8b4168bb10aa57d56649b42aefa67a3f'


def d2_validate_authority(value, sha):
    require(type(value) is dict and set(value) == GRANT_KEYS,
            'd2-linux-runner-authority-fields')
    require(value['approved'] is True and value['consumer'] == value['job'] == 'd2-linux'
            and value['repository'] == 'foykes/moex-dataset'
            and value['actor'] == value['triggering_actor'] == 'foykes'
            and value['workflow'] == '.github/workflows/d2-linux-offline.yml'
            and value['sha'] == sha, 'd2-linux-runner-authority-identity')
    for key, width in (('sha', 40), ('base', 40), ('ticket_id', 64)):
        require(type(value[key]) is str and re.fullmatch('[0-9a-f]{' + str(width) + '}', value[key]),
                'd2-linux-runner-authority-identity')
    require(type(value['attempt']) is int and value['attempt'] == 2
            and type(value['run_number']) is int and value['run_number'] > 0
            and type(value['run_id']) is str and re.fullmatch('[1-9][0-9]*', value['run_id']),
            'd2-linux-runner-authority-identity')
    require((value['event'] == 'push' and value['ref'] == 'refs/heads/codex/mds-d2-linux-stage-b'
             and value['base'] == D2_DEVELOPMENT_BASE)
            or (value['event'] == 'workflow_dispatch' and value['ref'] == 'refs/heads/main'
                and value['base'] == value['sha']), 'd2-linux-runner-authority-ref')


def d2_validate_ticket(env, repo):
    repo = real_path(str(repo), directory=True)
    digest = env.get('MDS_D2_LINUX_RUN_TICKET_SHA256')
    require(type(digest) is str and re.fullmatch('[0-9a-f]{64}', digest), 'd2-linux-runner-ticket-missing')
    path = real_path(env.get('MDS_D2_LINUX_RUN_TICKET'), private=True)
    data, ticket = read_record(path)
    require(hashlib.sha256(data).hexdigest() == digest, 'd2-linux-runner-ticket-digest')
    keys = {'schema_version', 'root', 'nonce', 'repo', 'parent_pid', 'source_sha',
            'source_files_sha256', 'authority', 'result', 'native_root', 'phase'}
    require(type(ticket) is dict and set(ticket) == keys
            and ticket['schema_version'] == 'd2-linux-runner-ticket-v1'
            and ticket['phase'] in {'foundation', 'application'}, 'd2-linux-runner-ticket-fields')
    require(type(ticket['parent_pid']) is int and ticket['parent_pid'] == os.getppid(),
            'd2-linux-runner-parent')
    root = real_path(ticket['root'], directory=True, private=True)
    require(root.name.startswith('d2-linux-')
            and path == root / 'jobs' / ('application-' + ticket['phase'] + '-ticket.json')
            and type(ticket['nonce']) is str and re.fullmatch('[0-9a-f]{32}', ticket['nonce']),
            'd2-linux-runner-owner')
    _, marker = read_record(root / '.lnx02-owner.json')
    require(marker == {'nonce': ticket['nonce'], 'purpose': 'D2-LINUX'}, 'd2-linux-runner-owner')
    require(ticket['repo'] == str(repo), 'd2-linux-runner-source')
    d2_validate_authority(ticket['authority'], ticket['source_sha'])
    raw_authority = env.get('MDS_D2_LINUX_ADMISSION_GRANT', '')
    require(type(raw_authority) is str and 0 < len(raw_authority) <= 16384
            and json.loads(raw_authority, object_pairs_hook=no_duplicates) == ticket['authority'],
            'd2-linux-runner-authority-binding')
    native_names = {'repository': 'GITHUB_REPOSITORY', 'sha': 'GITHUB_SHA',
        'event': 'GITHUB_EVENT_NAME', 'ref': 'GITHUB_REF', 'actor': 'GITHUB_ACTOR',
        'triggering_actor': 'GITHUB_TRIGGERING_ACTOR', 'job': 'GITHUB_JOB',
        'run_id': 'GITHUB_RUN_ID', 'run_number': 'GITHUB_RUN_NUMBER', 'attempt': 'GITHUB_RUN_ATTEMPT'}
    require(all(env.get(name) == str(ticket['authority'][key]) for key, name in native_names.items())
            and env.get('GITHUB_ACTIONS') == 'true'
            and env.get('RUNNER_ENVIRONMENT') == env.get('TASK_RUNNER_ENVIRONMENT') == 'github-hosted'
            and env.get('ImageOS') == 'ubuntu24', 'd2-linux-runner-authority-runtime')
    require(type(ticket['source_files_sha256']) is dict
            and set(ticket['source_files_sha256']) == set(D2_SOURCE_PATHS), 'd2-linux-runner-source')
    for name, digest in ticket['source_files_sha256'].items():
        require(type(digest) is str and re.fullmatch('[0-9a-f]{64}', digest)
                and hashlib.sha256(real_path(str(repo / name)).read_bytes()).hexdigest() == digest,
                'd2-linux-runner-source')
    native = real_path(ticket['native_root'], directory=True, private=True)
    require(native == root / 'fixtures' / ('native' if ticket['phase'] == 'foundation' else 'd2'),
            'd2-linux-runner-native-root')
    require(ticket['result'] == str(root / 'jobs' / ('application-' + ticket['phase'] + '-tests.json'))
            and not Path(ticket['result']).exists(), 'd2-linux-runner-result')
    descriptor = os.open(path.with_suffix('.claimed'), os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    os.close(descriptor)
    return ticket


class D2Counts(Counts):
    def __init__(self):
        super().__init__()
        self.native_ids = set()
        self.collected_ids = []
        self.skipped = 0
        self.collection_finished = False

    def pytest_collection_finish(self, session):
        super().pytest_collection_finish(session)
        self.collected_ids = sorted(item.nodeid for item in session.items)
        self.native_ids = {item.nodeid for item in session.items if item.get_closest_marker('native')}
        self.collection_finished = True

    def pytest_runtest_logreport(self, report):
        super().pytest_runtest_logreport(report)
        if report.skipped:
            self.skipped += 1


def d2_run_tests(ticket, repo):
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(repo))
    os.environ['PYTEST_DISABLE_PLUGIN_AUTOLOAD'] = '1'
    os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
    os.environ['MDS_D2_SOURCE_SHA'] = ticket['source_sha']
    os.environ['MDS_D2_NATIVE_ROOT'] = ticket['native_root']
    os.environ['LNX03_SOURCE_SHA'] = ticket['source_sha']
    os.environ['LNX03_NATIVE_ROOT'] = ticket['native_root']
    module = importlib.import_module('tools.offline_guard_linux')
    guard = module.install_process_guard()
    module.attest_runtime()
    if ticket['phase'] == 'application':
        module.d2_runner_admission(ticket)
    import sysconfig
    prefix = real_path(str(Path(sys.prefix).absolute()), directory=True)
    require(prefix == Path(ticket['root']) / 'dev-replay', 'd2-linux-runner-environment')
    for key in ('purelib', 'platlib'):
        site = real_path(sysconfig.get_paths()[key], directory=True)
        require(site.is_relative_to(prefix), 'd2-linux-runner-site')
        if str(site) not in sys.path:
            sys.path.append(str(site))
    require(not os.environ.get('PYTEST_ADDOPTS') and not os.environ.get('PYTEST_PLUGINS'),
            'd2-linux-runner-pytest-override')
    import pytest
    foundation = ticket['phase'] == 'foundation'
    targets = TESTS if foundation else D2_TESTS
    conftest = repo / ('tests/linux_bootstrap/conftest.py' if foundation else 'tests/d2/conftest.py')
    counts = D2Counts()
    base = Path(ticket['root']) / 'fixtures' / ('pytest-' + ticket['phase'])
    require(not base.exists(), 'd2-linux-runner-fixture-exists')
    arguments = [str(repo / name) for name in targets] + [
        '-c', '/dev/null', '--rootdir', str(repo), '--confcutdir', str(conftest.parent),
        '--import-mode=importlib', '--strict-markers', '-q', '--tb=no',
        '-p', 'no:cacheprovider', '--basetemp', str(base),
    ]
    exit_code = int(pytest.main(arguments, plugins=[counts]))
    cases = []
    for loaded in tuple(sys.modules.values()):
        filename = getattr(loaded, '__file__', None)
        if filename and Path(filename).absolute() == conftest:
            cases = getattr(loaded, 'NATIVE_RESULTS', [])
            break
    passed = len(counts.passed_ids - counts.failed_ids)
    node_ids_digest = hashlib.sha256(('\n'.join(counts.collected_ids) + '\n').encode('utf-8')).hexdigest()
    expected_cases = set(CASES if foundation else module.D2_CASES + module.D2_BASELINE_CASES)
    complete = (exit_code == 0 and not counts.failed_ids and not counts.skipped
        and passed == counts.collected and counts.collected > len(expected_cases)
        and counts.deselected == 0 and type(cases) is list
        and len(cases) == len(expected_cases) and {item.get('case') for item in cases} == expected_cases
        and not guard.denials)
    if foundation:
        complete = (complete and counts.collected == 444 and len(counts.native_ids) == 22
                    and all(item.get('status') == 'PASS' for item in cases))
        inventory_known = complete
        failed_node_ids = [] if complete else None
    else:
        expected = json.loads((repo / 'tests/fixtures/d2/expected.json').read_bytes())
        inventory_known = (counts.collection_finished and counts.collected_ids == expected.get('expected_node_ids')
            and node_ids_digest == expected.get('expected_node_ids_sha256') == D2_NODE_IDS_SHA256
            and counts.failed_ids <= set(counts.collected_ids))
        positions = {node: index + 1 for index, node in enumerate(counts.collected_ids)}
        failed_node_ids = (sorted('d2-node-{:06d}'.format(positions[node]) for node in counts.failed_ids)
                           if inventory_known else None)
        complete = (complete and counts.collected == 300 and len(counts.native_ids) == 56
            and len(set(counts.collected_ids)) == 300
            and counts.collected_ids == expected.get('expected_node_ids')
            and node_ids_digest == expected.get('expected_node_ids_sha256') == D2_NODE_IDS_SHA256)
        complete = complete and all(item.get('controller', {}).get('settled') is True
            and item['controller'].get('exitcode') == 0 and not item['controller'].get('timed_out')
            for item in cases)
    return dict(schema_version='d2-linux-runner-result-v1', phase=ticket['phase'],
        status='PASS' if complete else 'FAIL', source_sha=ticket['source_sha'],
        collected=counts.collected, passed=passed, failed=len(counts.failed_ids),
        deselected=counts.deselected, skipped=counts.skipped,
        native_collected=len(counts.native_ids), native_passed=len(counts.native_ids & counts.passed_ids),
        node_ids_sha256=node_ids_digest,
        node_inventory_state='COMPLETE' if inventory_known else 'NOT_AVAILABLE',
        failed_node_ids=failed_node_ids,
        exit_code=0 if complete else (exit_code or 1), native_cases=cases,
        failure_code=None if complete else 'd2-linux-application-oracle-failed')


def d2_write_result(ticket, result):
    data = d2_admit_export_bytes(json_bytes(result))
    descriptor = os.open(ticket['result'], os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'wb') as handle:
        handle.write(data)


def d2_admit_export_bytes(data):
    require(type(data) is bytes and len(data) <= 1024 * 1024, 'd2-linux-runner-result-bound')
    require(b'D2_PRIVATE_CANARY_4c1e11' not in data
            and b'LNX03_NATIVE_PRIVATE_CANARY_7fe94d31' not in data
            and re.search(rb'(?i)(https?://|file://|Traceback [(]most recent|[A-Z]:[\\/])', data) is None
            and re.search(rb'(?:"|[\s])/[-A-Za-z0-9_.]', data) is None,
            'd2-linux-runner-export-refused')
    return data


def d2_main():
    ticket = None
    result = None
    try:
        require(sys.platform == 'linux' and sys.implementation.name == 'cpython'
                and sys.version_info[:3] == (3, 14, 8) and sys.flags.isolated
                and sys.flags.no_site and sys.flags.dont_write_bytecode, 'd2-linux-runner-platform')
        repo = Path(__file__).absolute().parents[1]
        ticket = d2_validate_ticket(os.environ, repo)
        import ctypes
        import signal
        library = ctypes.CDLL(None, use_errno=True)
        library.prctl.argtypes = (ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong)
        library.prctl.restype = ctypes.c_int
        require(library.prctl(1, signal.SIGKILL, 0, 0, 0) == 0
                and os.getppid() == ticket['parent_pid'], 'd2-linux-runner-parent-death')
        result = d2_run_tests(ticket, repo)
        d2_write_result(ticket, result)
        return result['exit_code']
    except BaseException:
        if ticket is not None:
            try:
                incomplete = dict(schema_version='d2-linux-runner-result-v1',
                    phase=ticket['phase'], status='INCOMPLETE', source_sha=ticket['source_sha'],
                    collected=0, passed=0, failed=0, deselected=0, skipped=0,
                    native_collected=0, native_passed=0,
                    node_ids_sha256=hashlib.sha256(b'\n').hexdigest(), exit_code=1,
                    node_inventory_state='NOT_AVAILABLE', failed_node_ids=None,
                    native_cases=[], failure_code='d2-linux-runner-incomplete')
                if result is not None:
                    for key in ('collected', 'passed', 'failed', 'deselected', 'skipped',
                                'native_collected', 'native_passed', 'node_ids_sha256',
                                'node_inventory_state', 'failed_node_ids'):
                        incomplete[key] = result[key]
                d2_write_result(ticket, incomplete)
            except BaseException:
                pass
        print('D2_LINUX_RUNNER_REFUSED')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
