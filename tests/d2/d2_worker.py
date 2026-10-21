"""Fixed D2 controller and baseline workers; no arbitrary script dispatch."""

import base64
import contextlib
import hashlib
import importlib.util
import io
import json
import os
import re
from pathlib import Path
import signal
import stat
import subprocess
import sys
import threading
import time


CANARY = 'D2_PRIVATE_CANARY_4c1e11'
BASELINE_SHA256 = '95945e89d8bc766f934132270a8ee5df75d5382cad86ff44771eb352d5e5acb1'
SUCCESS_TEXT = 'Скрипт загрузки данных закончил работу.'
PROJECT_NAME = 'проект D2 с пробелом'

# These modules are literal local doubles, never a general module capability.
# Real pandas reads and projects the literal CSV with its installed types/order.
PYGSHEETS = '''import json
from pathlib import Path
_root=Path(__file__).parent
_case=json.loads((_root/'scenario.json').read_bytes())['case']
_events=[]
def _record(kind,**fields):
    _events.append(dict(kind=kind,**fields))
    (_root.parent/'vendor-google.json').write_text(json.dumps(_events,sort_keys=True),encoding='utf-8')
class Sheet:
    def clear(self):
        _record('clear')
        if _case in ('google_error','partial_google','baseline_google_swallowed'):
            _record('fault',case=_case)
            raise RuntimeError('D2_PRIVATE_CANARY_4c1e11')
    def set_dataframe(self,frame,start,**options):
        _record('write',columns=list(frame.columns),rows=frame.values.tolist(),start=start,options=options)
class Client:
    def open_by_url(self,target):
        _record('target',target=target)
        return [Sheet()]
def authorize(**options):
    _record('authorize')
    return Client()
'''
FTPLIB = '''import hashlib,json
from pathlib import Path
_root=Path(__file__).parent
_case=json.loads((_root/'scenario.json').read_bytes())['case']
_events=[]
def _record(kind,**fields):
    _events.append(dict(kind=kind,**fields))
    (_root.parent/'vendor-ftp.json').write_text(json.dumps(_events,sort_keys=True),encoding='utf-8')
class FTP:
    def __init__(self,*arguments):
        if arguments!=('D2_FAKE_DOMAIN.invalid','D2_FAKE_USERNAME','D2_FAKE_PASSWORD'):
            raise AssertionError('D2_FIXTURE_FTP_ARGUMENTS')
        _record('connect')
    def storbinary(self,command,stream):
        data=stream.read()
        _record('transfer',command=command,sha256=hashlib.sha256(data).hexdigest())
        group='dividends' if command.startswith('STOR dividends/') else 'tickers' if command.startswith('STOR ticker_lists/') else 'root'
        count=sum(row['kind']=='transfer' for row in _events)
        if (_case=='ftp_error' or _case=='partial_ftp' and count==2
                or _case=='baseline_ftp_'+group+'_swallowed'):
            _record('fault',case=_case)
            raise RuntimeError('D2_PRIVATE_CANARY_4c1e11')
    def quit(self):
        _record('quit')
    def close(self):
        _record('close')
'''


def json_bytes(value):
    return (json.dumps(value, ensure_ascii=True, sort_keys=True,
                       separators=(',', ':'), allow_nan=False) + '\n').encode('ascii')


def write_private(path, value):
    data = json_bytes(value)
    if len(data) > 1048576:
        raise ValueError('D2_FIXTURE_RESULT_BOUND')
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'wb') as handle:
        handle.write(data)


def make_source_fixture(root, oracle):
    root = Path(root)
    project = root / PROJECT_NAME
    datasets = project / 'datasets'
    secrets = root / 'secrets'
    datasets.mkdir(parents=True, mode=0o700)
    secrets.mkdir(mode=0o700)
    (datasets / '10years_data_1d_interval.csv').write_bytes(oracle['csv']['text'].encode('utf-8'))
    for name, text in oracle['files'].items():
        target = datasets / name
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        target.write_bytes(text.encode('utf-8'))
    (secrets / 'moex.foykes.com.json').write_bytes(json_bytes(dict(
        domain='D2_FAKE_DOMAIN.invalid', username='D2_FAKE_USERNAME', password='D2_FAKE_PASSWORD')))
    (secrets / 'python-304621-1e403209f05f.json').write_bytes(b'{}\n')
    return dict(project=project, datasets=datasets, secrets=secrets)


def make_vendors(root, case):
    vendor = Path(root) / 'vendors'
    vendor.mkdir(mode=0o700)
    for name, data in (('pygsheets.py', PYGSHEETS.encode()), ('ftplib.py', FTPLIB.encode()),
                       ('scenario.json', json_bytes({'case': case}))):
        descriptor = os.open(vendor / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, 'wb') as handle:
            handle.write(data)
    return vendor


def _oracle(repo):
    return json.loads((repo / 'tests/fixtures/d2/expected.json').read_bytes())


def _original(oracle):
    data = base64.b64decode(oracle['baseline_upload_base64'], validate=True)
    if (len(data) != 7210 or hashlib.sha256(data).hexdigest() != BASELINE_SHA256
            or hashlib.sha1(b'blob 7210\0' + data).hexdigest() != 'dc1f12bc63eadebe589a666ffcb6a0a2bb908747'):
        raise ValueError('D2_BASELINE_SOURCE_CHANGED')
    return data


def _baseline_namespace(repo, scratch, fixture, case):
    # Native -S workers add only owned, verified replay package directories.
    import sysconfig
    for key in ('purelib', 'platlib'):
        path = sysconfig.get_paths()[key]
        if path not in sys.path:
            sys.path.append(path)
    vendor = Path(scratch) / 'vendors'
    sys.path.insert(0, str(vendor))
    sys.path[0] = str(vendor)
    namespace = dict(__name__='d2_original_source', __file__='<D2-original-upload-3320b44>')
    exec(compile(_original(_oracle(repo)), '<D2-original-upload-3320b44>', 'exec'), namespace)
    namespace['datasets_path'] = str(fixture['datasets']) + '/'
    return namespace


def baseline_worker(ticket):
    repo, scratch = Path(ticket['repo_root']), Path(ticket['scratch'])
    if (ticket['schema_version'] != 'd2-baseline-ticket-v1'
            or ticket['role'] not in {'baseline-google', 'baseline-ftp'}
            or ticket['parent_pid'] != os.getppid()
            or ticket['parent_start_ticks'] != int(Path('/proc/' + str(os.getppid()) + '/stat').read_text().rsplit(')', 1)[1].split()[19])
            or ticket['original_sha256'] != BASELINE_SHA256
            or scratch.resolve() != scratch):
        raise ValueError('D2_BASELINE_IDENTITY')
    from tools import offline_guard_linux as guard
    if guard._d2_sources(repo) != ticket['source_files_sha256']:
        raise ValueError('D2_BASELINE_SOURCE_CHANGED')
    claim = os.open(scratch / (ticket['role'] + '.claim'), os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    os.close(claim)
    role, case = ticket['role'], ticket['case']
    if role == 'baseline-google' and case == 'baseline_exit23':
        write_private(scratch / (role + '-boundary.json'), {'boundary': 'REAL_EXIT23', 'pid': os.getpid()})
        os._exit(23)
    if role == 'baseline-google' and case == 'baseline_hang':
        write_private(scratch / (role + '-boundary.json'), {'boundary': 'REAL_HANG', 'pid': os.getpid()})
        threading.Event().wait()
    fixture = dict(project=scratch / PROJECT_NAME, datasets=scratch / PROJECT_NAME / 'datasets')
    namespace = _baseline_namespace(repo, scratch, fixture, case)
    if role == 'baseline-google':
        namespace['gdoc_upload'](str(fixture['project']), 'D2_LITERAL_TARGET')
    else:
        namespace['ftp_upload'](str(fixture['project']))
    write_private(scratch / (role + '-returned.json'), {'returned': True, 'pid': os.getpid()})
    return 0


def _baseline_controller(ticket, guard, fixture):
    repo, scratch, case = Path(ticket['repo_root']), Path(ticket['scratch']), ticket['case']
    namespace = _baseline_namespace(repo, scratch, fixture, case)
    records, attempts = [], []
    deadline = time.monotonic() + 15
    class LegacyProcess:
        def __init__(self, *, target, args):
            role = 'baseline-google' if target is namespace['gdoc_upload'] else 'baseline-ftp'
            self.role = role
            self.record = None
        def start(self):
            issued = guard.issue_d2_legacy_ticket(repo, scratch, case, self.role, BASELINE_SHA256)
            self.record = guard.launch_d2_legacy(issued)
            records.append(self.record)
            attempts.append({'worker_id': self.role.removeprefix('baseline-'), 'pid': self.record['process'].pid})
        def join(self):
            self.record['process'].wait(timeout=max(0, deadline - time.monotonic()))
    namespace['Process'] = LegacyProcess
    output, blocked, returned = io.StringIO(), False, False
    try:
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            value = namespace['main'](str(fixture['project']), 'D2_LITERAL_TARGET')
            returned = value is None
    except subprocess.TimeoutExpired:
        blocked = True
    finally:
        settled = guard.settle_d2_legacy(records)
    observed = []
    for item, attempt in zip(records, attempts):
        observed.append(dict(attempt, exitcode=item['process'].returncode,
                             settled=item['process'].returncode is not None,
                             identity=item['identity']))
    fault_observed = False
    for name in ('vendor-google.json', 'vendor-ftp.json'):
        path = scratch / name
        if path.exists():
            events = json.loads(path.read_bytes())
            fault_observed |= any(row['kind'] == 'fault' and row['case'] == case for row in events)
    red = (blocked if case == 'baseline_hang' else
           returned and SUCCESS_TEXT in output.getvalue() and
           (any(row['exitcode'] == 23 for row in observed) if case == 'baseline_exit23' else fault_observed))
    return dict(case=case, application_exit=None, baseline_red=red,
                baseline_scope='ORIGINAL_TARGET_AND_PARENT_FLOW_FIXED_PROCESS_ADAPTER',
                legacy_parent_returned_none=returned, legacy_false_completion_observed=returned and SUCCESS_TEXT in output.getvalue(),
                legacy_operation_fault_observed=fault_observed, legacy_hang_observed=blocked,
                all_settled=settled, observations=observed)


def _projection(report):
    if report is None:
        return None
    return {key: report[key] for key in ('run_id', 'execution_outcome', 'exit_status',
        'delivery_outcome', 'storage_sealed', 'evidence_incomplete', 'expected_producers',
        'worker_outcomes', 'release_result')}


def _native_controller(ticket, guard, fixture, vendor):
    repo, scratch, case = Path(ticket['repo_root']), Path(ticket['scratch']), ticket['case']
    import sysconfig
    for key in ('purelib', 'platlib'):
        path = sysconfig.get_paths()[key]
        if path not in sys.path:
            sys.path.append(path)
    import upload
    import run_logging
    upload.datasets_path = str(fixture['datasets']) + '/'
    profile = guard.issue_d2_profile(repo, scratch, case, vendor)
    processes, reports, outcomes, phase_reads, releases, constructor = [], [], [], [], [], []
    real_popen = subprocess.Popen
    real_fork_exec = subprocess._fork_exec
    real_waitpid = os.waitpid
    real_finalize = run_logging.finalize_logging
    real_outcome = run_logging.record_worker_outcome
    real_read = upload._d2_read_checkpoint
    real_go = upload._d2_release_go
    real_inventory = upload._d2_run_inventory
    real_fdopen = upload._d2_fdopen
    go_released = False
    active_role = None
    preseal_observed = None
    reader_role = None
    reader_lifetimes, inventory_observations = [], []
    @contextlib.contextmanager
    def fdopen(descriptor, *args, **options):
        role = reader_role
        identity = os.fstat(descriptor)
        try:
            with real_fdopen(descriptor, *args, **options) as handle:
                yield handle
        finally:
            if role is not None:
                try:
                    closed = os.fstat(descriptor) != identity
                except OSError:
                    closed = True
                reader_lifetimes.append(dict(worker_id=role, descriptor=descriptor, closed=closed))
    def fd_inventory():
        result = {}
        for name in os.listdir('/proc/self/fd'):
            descriptor = int(name)
            try:
                info = os.fstat(descriptor)
            except OSError:
                continue
            result[descriptor] = (info.st_dev, info.st_ino, info.st_mode, info.st_rdev)
        return result
    def popen(command, *args, **options):
        nonlocal active_role
        role = None
        if '--d2-worker' in command:
            path = Path(command[-1])
            role = json.loads(path.read_bytes())['role']
        before = active_role
        active_role = role
        try:
            process = real_popen(command, *args, **options)
        finally:
            active_role = before
        processes.append((role, process))
        return process
    def fork_exec(*args, **options):
        pid = real_fork_exec(*args, **options)
        if case in {'constructor_error', 'tracker_death'}:
            constructor.append({'event': 'FORK_EXEC_RETURNED', 'worker_id': active_role, 'pid': pid})
        return pid
    def waitpid(pid, options):
        result = real_waitpid(pid, options)
        if case in {'constructor_error', 'tracker_death'} and active_role is not None and result[0] == pid:
            constructor.append({'event': 'WAITPID_RETURNED', 'worker_id': active_role,
                                'pid': pid, 'exitcode': os.waitstatus_to_exitcode(result[1])})
        return result
    def finalize(session, **options):
        nonlocal preseal_observed
        if case in {'parent_finalization_raises', 'parent_finalization_incomplete'} and preseal_observed is None:
            children = [process for role, process in processes if role in {'google', 'ftp'}]
            finals = [session['_worker_finals'].get(session['_expected'][role]) for role in ('google', 'ftp')]
            preseal_observed = dict(
                actual_children_settled=sum(process.poll() is not None for process in children),
                actual_children_exit_zero=sum(process.returncode == 0 for process in children),
                worker_final_completed_zero=sum(value is not None and value['execution_outcome'] == 'COMPLETED'
                    and value['exit_status'] == 0 for value in finals),
                final_absent=session['_final'] is None,
                summary_absent=not (session['_run_root'] / 'run_summary.json').exists())
        report = real_finalize(session, **options)
        reports.append(_projection(report))
        return report
    def outcome(session, role, **options):
        result = real_outcome(session, role, **options)
        outcomes.append(dict(worker_id=role, **options))
        return result
    def read(root, worker_id, **options):
        nonlocal reader_role
        actual = next((process for role, process in processes if role == worker_id), None)
        exitcode = None if actual is None else actual.poll()
        if go_released:
            phase_reads.append(dict(worker_id=worker_id, phase='AFTER_GO', actual_exit=exitcode,
                                    pid=None if actual is None else actual.pid))
        else:
            phase_reads.append(dict(worker_id=worker_id, phase='BEFORE_GO', actual_exit=exitcode,
                                    pid=None if actual is None else actual.pid))
        previous = reader_role
        reader_role = worker_id
        try:
            return real_read(root, worker_id, **options)
        finally:
            reader_role = previous
    def release(runtime, children):
        nonlocal go_released
        closed_before_go = all(row['closed'] for row in reader_lifetimes)
        result = real_go(runtime, children)
        releases.append(dict(ready_reads=sum(row['phase'] == 'BEFORE_GO' for row in phase_reads),
                             readers_closed=closed_before_go))
        go_released = True
        return result
    def inventory(root, limits, **options):
        actual = [process for role, process in processes if role in {'google', 'ftp'}]
        inventory_observations.append(dict(phase='AFTER_GO' if go_released else 'BEFORE_GO',
            worker_pids=[process.pid for process in actual], worker_exits=[process.poll() for process in actual]))
        return real_inventory(root, limits, **options)
    subprocess.Popen = popen
    subprocess._fork_exec = fork_exec
    os.waitpid = waitpid
    run_logging.finalize_logging = finalize
    run_logging.record_worker_outcome = outcome
    upload._d2_read_checkpoint = read
    upload._d2_release_go = release
    upload._d2_run_inventory = inventory
    upload._d2_fdopen = fdopen
    sink, status, primary = {}, 0, None
    output = io.StringIO()
    started = time.monotonic()
    old_cwd = Path.cwd()
    decoy_cwd = scratch / 'cwd decoy'
    decoy_cwd.mkdir(mode=0o700)
    (decoy_cwd / 'datasets').mkdir(mode=0o700)
    (decoy_cwd / 'datasets/10years_data_1d_interval.csv').write_bytes(b'DECOY_SOURCE_MUST_NOT_BE_READ\n')
    os.chdir(decoy_cwd)
    actual_cwd = Path.cwd()
    foreign = os.open(scratch / 'controller-foreign-fd', os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
    os.set_inheritable(foreign, True)
    before_fds = fd_inventory()
    try:
        with guard_profile(upload, profile), contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            try:
                upload.main(str(fixture['project']), 'D2_LITERAL_TARGET', result_sink=sink)
            except BaseException as error:
                primary = type(error).__name__
                status = 130 if isinstance(error, KeyboardInterrupt) else 1
        status = sink.get('final_exit_status') or status
    finally:
        subprocess.Popen = real_popen
        subprocess._fork_exec = real_fork_exec
        os.waitpid = real_waitpid
        run_logging.finalize_logging = real_finalize
        run_logging.record_worker_outcome = real_outcome
        upload._d2_read_checkpoint = real_read
        upload._d2_release_go = real_go
        upload._d2_run_inventory = real_inventory
        upload._d2_fdopen = real_fdopen
        os.chdir(old_cwd)
    observations = []
    for role, process in processes:
        code = process.poll()
        observations.append(dict(worker_id=role, pid=process.pid, exitcode=code,
                                 settled=code is not None, reaped=code is not None))
    persisted_files = list(scratch.rglob('parent-result.json'))
    persisted_before = persisted_files[0].read_bytes() if len(persisted_files) == 1 else None
    persisted = json.loads(persisted_before) if persisted_before is not None else None
    application_report = reports[-1] if reports else None
    cleanup_report = None
    # A pre-seal exception has no successful application report. Fixture
    # cleanup is real, FAILED, separately labelled, and never applied to sink.
    if case == 'parent_finalization_raises':
        runtime = sink.get('_runtime')
        if runtime is not None and runtime.context is not None:
            cleanup_report = _projection(real_finalize(runtime.context, execution_outcome='FAILED', exit_status=1))
            upload._d2_finish_transport(runtime)
    after_fds = fd_inventory()
    foreign_intact = after_fds.get(foreign) == before_fds[foreign]
    fd_clean = after_fds == before_fds
    os.close(foreign)
    # This repair is explicitly independent fixture cleanup. Preserve the
    # application's pre-cleanup observations and never upgrade its receipt.
    remaining = [{'process': process, 'pidfd': None} for role, process in processes
                 if process.returncode is None]
    if remaining:
        guard._settle(remaining, timeout=10)
    fixture_cleanup = all(process.poll() is not None for role, process in processes)
    tracker_settled = all(process.poll() is not None for role, process in processes if role == 'tracker')
    persisted_unchanged = (persisted_before is not None
        and persisted_files[0].read_bytes() == persisted_before)
    if preseal_observed is not None:
        preseal_observed['fault_boundary'] = (sink.get('_preseal_preconditions') or {}).get('fault_boundary')
    google_path, ftp_path = scratch / 'vendor-google.json', scratch / 'vendor-ftp.json'
    google_observed = None
    if google_path.exists():
        writes = [row for row in json.loads(google_path.read_bytes()) if row['kind'] == 'write']
        if writes:
            actual = writes[-1]
            literal = _oracle(repo)
            google_observed = (actual['columns'] == literal['csv']['columns']
                and actual['rows'] == literal['csv']['rows'] and actual['start'] == 'A1'
                and actual['options'] == {key: value for key, value in literal['google_arguments'].items() if key != 'start'})
    ftp_observed = None
    if ftp_path.exists():
        transfers = [row['command'] for row in json.loads(ftp_path.read_bytes()) if row['kind'] == 'transfer']
        ftp_observed = (set(transfers) == {'STOR README.md', 'STOR 10years_data_1d_interval.csv',
                                         'STOR dividends/dividend.csv', 'STOR ticker_lists/tickers.csv'})
    # Private witnesses are typed fixed fields. Never export a raw stream,
    # exception payload, credential, URL, or private filesystem path.
    return dict(case=case, application_exit=status, result=upload._d2_public_result(sink),
        logging=application_report, observed_reports=reports,
        cleanup_report=cleanup_report, cleanup_report_scope='TEST_CLEANUP' if cleanup_report is not None else None,
        logging_outcomes=outcomes, observations=observations,
        worker_witnesses=sink.get('_worker_witnesses', {}),
        tracker_probes=sink.get('_tracker_probes', {}),
        constructor_observations=constructor,
        fd_inventory_unchanged=fd_clean, foreign_fd_intact=foreign_intact,
        fixture_cleanup_settled=fixture_cleanup,
        fixture_tracker_settled=tracker_settled,
        inventory_observations=inventory_observations,
        readers_closed_before_go=all(row['readers_closed'] for row in releases),
        google_projection_observed=google_observed,
        ftp_targets_observed=ftp_observed,
        platform_evidence=dict(platform=sys.platform, implementation=sys.implementation.name,
            version=list(sys.version_info[:3]), architecture=os.uname().machine,
            filesystem_encoding=sys.getfilesystemencoding(),
            actual_path_unicode=any(ord(letter) > 127 for letter in str(fixture['project'])),
            actual_path_space=' ' in str(fixture['project']),
            actual_cwd_foreign=actual_cwd == decoy_cwd and actual_cwd != fixture['project'] and actual_cwd != repo),
        phase_reads=phase_reads, go_releases=releases, exception_type=primary,
        parent_elapsed_seconds=time.monotonic() - started,
        persisted_result=persisted,
        persisted_bytes_unchanged=persisted_unchanged,
        fault_witnesses=sink.get('_fault_witnesses', []),
        preseal_preconditions=preseal_observed,
        deadline_detected=sink.get('_deadline_detected', False),
        cleanup_requested=sink.get('_cleanup_requested', False),
        target_entered_before_deadline=sink.get('_target_entered_before_deadline', False),
        console_canary_absent=CANARY not in output.getvalue(),
        console_redaction_valid=(not any(token in output.getvalue() for token in
            (CANARY, 'D2_BOOTSTRAP_CANARY', 'Traceback', 'http://', 'https://',
             str(scratch), str(repo)))
            and re.search(r'(?i)(?:[a-z]:[\\/]|/(?:home|tmp|private|users)/)', output.getvalue()) is None),
        success_banner=SUCCESS_TEXT in output.getvalue())


@contextlib.contextmanager
def guard_profile(upload, profile):
    with upload._d2_test_profile(profile):
        yield


def _parent_death(parent_pid):
    import ctypes
    libc = ctypes.CDLL(None, use_errno=True)
    libc.prctl.argtypes = (ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong)
    libc.prctl.restype = ctypes.c_int
    if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0 or os.getppid() != parent_pid:
        raise ValueError('D2_FIXTURE_PARENT_CHANGED')


def main():
    if len(sys.argv) != 3 or sys.argv[1] != '--ticket':
        return 1
    path = Path(sys.argv[2])
    if not path.is_absolute() or path.resolve() != path:
        return 1
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    with os.fdopen(descriptor, 'rb') as handle:
        info = os.fstat(handle.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or info.st_uid != os.getuid() or info.st_mode & 0o077 or info.st_size > 65536):
            return 1
        ticket = json.loads(handle.read(65537))
    _parent_death(ticket['parent_pid'])
    repo = Path(__file__).resolve().parents[2]
    if ticket.get('repo_root') != str(repo):
        return 1
    sys.path.insert(0, str(repo))
    from tools import offline_guard_linux as guard
    if ticket.get('schema_version') == 'd2-baseline-ticket-v1':
        guard.install_process_guard()
        return baseline_worker(ticket)
    guard.d2_controller_admission(ticket)
    scratch = Path(ticket['scratch'])
    fixture = make_source_fixture(scratch, _oracle(repo))
    vendor = make_vendors(scratch, ticket['case'])
    if ticket['case'] in guard.D2_BASELINE_CASES:
        observation = _baseline_controller(ticket, guard, fixture)
    else:
        observation = _native_controller(ticket, guard, fixture, vendor)
    write_private(Path(ticket['result']), observation)
    return 0


if __name__ == '__main__':
    try:
        status = main()
    except BaseException:
        # Native DEVNULL contains pre-wrapper interpreter diagnostics as well.
        os._exit(1)
    raise SystemExit(status)
