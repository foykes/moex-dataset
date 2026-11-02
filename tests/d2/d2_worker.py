"""Fixed D2 controller and real spawn targets. No live dependency or service."""

import base64
import csv
import hashlib
import importlib.util
import io
import json
import multiprocessing
import os
from pathlib import Path
import sys
import threading
import time
import types
import uuid


ROOT = Path(__file__).resolve().parents[2]
for entry in (str(ROOT), str(Path(__file__).parent)):
    if entry not in sys.path:
        sys.path.insert(0, entry)
if 'mds_offline_guard' not in sys.modules:
    specification = importlib.util.spec_from_file_location('mds_offline_guard', ROOT / 'tools/offline_guard.py')
    guard = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = guard
    specification.loader.exec_module(guard)
else:
    guard = sys.modules['mds_offline_guard']


def _context():
    return guard._CONTEXT


def _contract():
    return _context().ticket['d2_contract']


def _json(path, document):
    path = Path(path)
    pending = path.with_name(path.name + '.fixture-pending')
    data = (json.dumps(document, ensure_ascii=True, sort_keys=True) + '\n').encode('utf-8')
    with pending.open('xb') as destination:
        destination.write(data)
        destination.flush()
    os.replace(pending, path)


def _oracle():
    return json.loads((ROOT / 'tests/fixtures/d2/expected.json').read_text(encoding='utf-8'))


def _baseline_bytes():
    oracle = _oracle()
    source = base64.b64decode(oracle['baseline_upload_base64'], validate=True)
    assert len(source) == oracle['baseline_upload_bytes']
    assert hashlib.sha256(source).hexdigest() == oracle['baseline_upload_sha256']
    blob = b'blob ' + str(len(source)).encode('ascii') + b'\0' + source
    assert hashlib.sha1(blob).hexdigest() == oracle['baseline_upload_git_blob']
    return source


def _seed_fixture(root):
    oracle = _oracle()
    datasets = root / 'project/datasets'
    datasets.mkdir(parents=True)
    csv_data = oracle['csv']['text'].encode('utf-8')
    assert len(csv_data) == oracle['csv']['bytes']
    assert hashlib.sha256(csv_data).hexdigest() == oracle['csv']['sha256']
    (datasets / '10years_data_1d_interval.csv').write_bytes(csv_data)
    for name, text in oracle['files'].items():
        path = datasets / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding='utf-8', newline='\n')
    secrets = root / 'secrets'
    secrets.mkdir()
    (secrets / 'moex.foykes.com.json').write_text(json.dumps(dict(domain='D2_FAKE_DOMAIN.invalid',
        username='D2_FAKE_USERNAME', password='D2_FAKE_PASSWORD')), encoding='utf-8')
    (secrets / 'python-304621-1e403209f05f.json').write_text(json.dumps(dict(private_key='D2_FAKE_PRIVATE_KEY')), encoding='utf-8')
    return datasets


def fail_on_unpickle():
    guard.d2_write_canary_streams()
    guard.d2_fault_witness('bootstrap_unpickle')
    raise RuntimeError(guard._D2_CANARY_TEXT)


class SpawnFault:
    def __init__(self, phase):
        self.phase = phase

    def __reduce__(self):
        if self.phase == 'pickle':
            guard.d2_fault_witness('start_serialization_failure', request=guard._THREAD_STATE.d2_request)
            raise RuntimeError(guard._D2_CANARY_TEXT)
        return fail_on_unpickle, ()


def worker_setup(case, worker_id):
    """Only the issued worker entry can invoke these fixed fixture faults."""
    if _contract()['case'] != case or _contract()['worker_id'] != worker_id:
        raise ValueError('D2_FIXTURE_WORKER_IDENTITY')
    if _context().role == 'd2-worker':
        guard.d2_write_canary_streams()
        import upload
        original_checkpoint = upload._d2_write_checkpoint
        def atomic_checkpoint(*arguments, **options):
            try:
                return original_checkpoint(*arguments, **options)
            except BaseException as error:
                guard.d2_atomic_fault(error)
                raise
        upload._d2_write_checkpoint = atomic_checkpoint
    if worker_id != 'google':
        return
    if case == 'success':
        import pandas
        import pygsheets
        import d2_issued_module
        slot = Path(_contract()['result_root']).parent
        _json(slot / 'dependencies-google.json', dict(pandas_imported=pandas.__name__ == 'pandas',
            pygsheets_imported=pygsheets.__name__ == 'pygsheets',
            issued_module_imported=d2_issued_module.FIXED == 'D2_ISSUED_MODULE',
            no_site_flag=sys.flags.no_site == 1,
            restored_sys_path_observed=str(slot / 'issued-site/site-packages') in sys.path,
            pth_not_executed=not (slot / 'pth-executed').exists(),
            pandas_origin_owned=Path(pandas.__file__).is_relative_to(ROOT / '.f1/venv-dev'),
            pygsheets_origin_owned=Path(pygsheets.__file__).is_relative_to(ROOT / '.f1/venv-dev')))
    if case == 'abnormal_before_ready':
        guard.d2_fault_witness('exit_before_ready')
        os._exit(23)
    import run_logging
    original = run_logging.finish_worker
    if case == 'normal_without_final':
        def without_final(producer, *, execution_outcome, exit_status):
            guard.d2_fault_witness('without_worker_final')
            producer['_queue'].cancel_join_thread()
            producer['_queue'].close()
            producer['_ack'].close()
            return dict(execution_outcome=execution_outcome, exit_status=exit_status, confirmed=False)
        run_logging.finish_worker = without_final
    elif case == 'ack_failure':
        def without_ack(producer, **arguments):
            guard.d2_fault_witness('ack_closed')
            producer['_ack'].close()
            return original(producer, **arguments)
        run_logging.finish_worker = without_ack
    elif case == 'abnormal_after_final':
        def exit_after_final(producer, **arguments):
            report = original(producer, **arguments)
            if (arguments != {'execution_outcome': 'COMPLETED', 'exit_status': 0}
                    or report.get('confirmed') is not True):
                raise RuntimeError('D2_FIXTURE_PHASE_NOT_REACHED')
            guard.d2_fault_witness('exit_after_worker_final')
            os._exit(23)
        run_logging.finish_worker = exit_after_final
    elif case == 'final_then_exit23':
        import atexit
        def after_checkpoint():
            path = Path(_contract()['result_root']) / 'google.json'
            checkpoint = json.loads(path.read_text(encoding='utf-8'))
            if (checkpoint.get('execution_state') != 'RETURNED'
                    or checkpoint.get('normal_return_observed') is not True
                    or not getattr(_context(), 'd2_confirmed_worker_final', False)):
                return
            guard.d2_fault_witness('exit_after_checkpoint')
            os._exit(23)
        def confirmed_final(producer, **arguments):
            report = original(producer, **arguments)
            _context().d2_confirmed_worker_final = (arguments == {'execution_outcome': 'COMPLETED', 'exit_status': 0}
                and report.get('confirmed') is True)
            return report
        run_logging.finish_worker = confirmed_final
        atexit.register(after_checkpoint)


def _fault_checkpoint(case):
    root = Path(_contract()['result_root'])
    path = root / 'google.json'
    guard.d2_fault_witness(case)
    if case == 'checkpoint_missing':
        path.unlink()
        threading.Event().wait()
    elif case == 'checkpoint_corrupt':
        path.write_bytes(b'{')
    elif case == 'checkpoint_oversize':
        path.write_bytes(b'X' * 65537)
    else:
        document = json.loads(path.read_text(encoding='utf-8'))
        field, value = {'checkpoint_stale_run': ('run_id', '0' * 32),
            'checkpoint_stale_pid': ('pid', os.getpid() + 1000000),
            'checkpoint_stale_sequence': ('sequence', 0)}[case]
        document[field] = value
        # The fixture writes the existing fixed checkpoint; no new result files.
        path.write_text(json.dumps(document, ensure_ascii=True), encoding='utf-8')
    # Keep the actual faulty snapshot until the parent's real deadline/settlement.
    threading.Event().wait()


def _target(channel, source_plan, output_observer):
    case = _contract()['case']
    if case == channel + '_error':
        guard.d2_fault_witness('target_error')
        raise RuntimeError(guard._D2_CANARY_TEXT)
    if channel == 'google':
        if case == 'exit23':
            guard.d2_fault_witness('target_exit23')
            os._exit(23)
        if case == 'system_exit0':
            guard.d2_fault_witness('target_system_exit0')
            raise SystemExit(0)
        if case == 'system_exit_string':
            guard.d2_fault_witness('target_system_exit_string')
            raise SystemExit(guard._D2_CANARY_TEXT)
        if case.startswith('checkpoint_'):
            _fault_checkpoint(case)
        if case == 'foreign_handle':
            inherited = json.loads((Path(_contract()['result_root']).parent / 'foreign.json').read_text(encoding='utf-8'))
            attempted = False
            try:
                os.write(inherited['fd'], b'D2_SYNTHETIC_CANARY_FOREIGN_EFFECT')
                attempted = True
            except OSError:
                pass
            observation = guard.d2_foreign_inheritance_probe(inherited)
            observation['crt_write_succeeded'] = attempted
            _json(Path(_contract()['result_root']).parent / 'foreign-observation.json', observation)
    if case == 'hang_' + channel or case == 'cleanup_unsettled' and channel == 'google':
        guard.d2_fault_witness('target_hang')
        _json(Path(_contract()['result_root']).parent / ('target-entered-' + channel + '.json'),
            dict(pid=os.getpid(), entered_at=time.monotonic()))
        threading.Event().wait()
    for index, item in enumerate(source_plan):
        output_observer(item['output_id'], 'UNKNOWN')
        if case == 'partial_' + channel and index == (0 if channel == 'google' else 1):
            guard.d2_fault_witness('partial_transfer')
            raise RuntimeError(guard._D2_CANARY_TEXT)
        with open(item['source_path'], 'rb') as source:
            data = source.read()
        if channel == 'google':
            oracle = _oracle()
            assert hashlib.sha256(data).hexdigest() == oracle['csv']['sha256']
            rows = list(csv.reader(io.StringIO(data.decode('utf-8'))))
            assert len(rows) == 3
        output_observer(item['output_id'], 'ACKNOWLEDGED')


def google_target(current_path, gdoc_to_write, *, source_plan=None, output_observer=None):
    return _target('google', source_plan, output_observer)


def ftp_target(current_path, *, source_plan=None, output_observer=None):
    return _target('ftp', source_plan, output_observer)


def _fake_legacy_module(case, root):
    calls = dict(fault_called=False, transfers=0)
    class Frame:
        columns = ['ticker', 'close', 'begin', 'open', 'volume', 'high', 'low', 'value', 'end', 'RSI14', 'extra']
        def drop(self, columns, **options):
            assert columns == ['extra']
    pandas = types.ModuleType('pandas')
    pandas.read_csv = lambda path: Frame()
    class Sheet:
        def clear(self):
            if case == 'baseline_google_swallowed':
                calls['fault_called'] = True
                guard.d2_fault_witness('legacy_google_clear')
                raise RuntimeError('D2_SYNTHETIC_CANARY_LEGACY_GOOGLE')
        def set_dataframe(self, *arguments, **options):
            calls['transfers'] += 1
    class Google:
        def open_by_url(self, url):
            return [Sheet()]
    pygsheets = types.ModuleType('pygsheets')
    pygsheets.authorize = lambda **options: Google()
    sys.modules['pandas'], sys.modules['pygsheets'] = pandas, pygsheets
    namespace = dict(__name__='d2_original_upload', __file__=str(root / 'original_upload.py'))
    exec(compile(_baseline_bytes(), '<D2-original-upload-3320b44>', 'exec'), namespace)
    class FTP:
        def __init__(self, *arguments):
            assert arguments == ('D2_FAKE_DOMAIN.invalid', 'D2_FAKE_USERNAME', 'D2_FAKE_PASSWORD')
        def storbinary(self, command, stream):
            group = 'dividends' if command.startswith('STOR dividends/') else 'tickers' if command.startswith('STOR ticker_lists/') else 'root'
            if case == 'baseline_ftp_' + group + '_swallowed':
                calls['fault_called'] = True
                guard.d2_fault_witness('legacy_ftp_' + group)
                raise RuntimeError('D2_SYNTHETIC_CANARY_LEGACY_FTP')
            calls['transfers'] += 1
            stream.read()
        def quit(self):
            pass
    namespace['ftplib'] = types.SimpleNamespace(FTP=FTP)
    namespace['datasets_path'] = str(root / 'project/datasets') + '/'
    return namespace, calls


def baseline_worker_entry(channel, case, root_string):
    guard.d2_worker_context(case, channel)
    root = Path(root_string)
    if case == 'baseline_exit23' and channel == 'google':
        guard.d2_fault_witness('legacy_exit23')
        os._exit(23)
    if case == 'baseline_hang' and channel == 'google':
        guard.d2_fault_witness('legacy_hang')
        threading.Event().wait()
    namespace, calls = _fake_legacy_module(case, root)
    function = namespace['gdoc_upload' if channel == 'google' else 'ftp_upload']
    arguments = ((root / 'project').as_posix(), 'D2_FAKE_GOOGLE_DESTINATION') if channel == 'google' else ((root / 'project').as_posix(),)
    function(*arguments)
    _json(root / ('baseline-' + channel + '.json'), dict(returned=True, **calls))
    guard.finish(0)


def _observe_adapter(upload, observations):
    original = upload._d2_native_observation
    def observe(state):
        result = original(state)
        if result['pid'] is not None:
            observations[result['pid']] = dict(result)
        guard.d2_observe_native_state(state, result)
        return result
    upload._d2_native_observation = observe


def _baseline_controller(case, root, upload):
    namespace, calls = _fake_legacy_module(case, root)
    processes = []
    deadline = time.monotonic() + 15.0
    class LegacyProcess:
        def __init__(self, *, target, args):
            self.channel = 'google' if target is namespace['gdoc_upload'] else 'ftp'
            self.process = multiprocessing.get_context('spawn').Process(target=baseline_worker_entry,
                args=(self.channel, case, str(root)))
            self.state = {}
            processes.append(self)
        def start(self):
            guard._d2_prepare_fixture_start(self.process, 'd2-baseline-worker', self.channel, case)
            assert upload._d2_start_worker(self.process, self.state)
        def join(self):
            while time.monotonic() < deadline:
                observed = upload._d2_native_observation(self.state)
                if observed['settled']:
                    return
                threading.Event().wait(.1)
            raise TimeoutError('D2_BASELINE_PARENT_HANG')
    namespace['Process'] = LegacyProcess
    returned, blocked = False, False
    try:
        parent_return = namespace['main']((root / 'project').as_posix(), 'D2_FAKE_GOOGLE_DESTINATION')
        assert parent_return is None
        returned = True
    except TimeoutError:
        blocked = True
    frames = []
    job_used = False
    for row in processes:
        before = upload._d2_native_observation(row.state)
        if not before['settled']:
            if case == 'baseline_hang' and row.channel == 'google':
                job_used = guard.d2_baseline_containment(row.process)
            else:
                upload._d2_native_terminate(row.state)
        stop = time.monotonic() + 10
        while not upload._d2_native_observation(row.state)['settled'] and time.monotonic() < stop:
            threading.Event().wait(.1)
        observed = upload._d2_native_observation(row.state)
        if observed['settled']:
            upload._d2_close_launch_state(row.state)
        child_receipt = root / ('baseline-' + row.channel + '.json')
        receipt = json.loads(child_receipt.read_text(encoding='utf-8')) if child_receipt.exists() else None
        frames.append(dict(worker_id=row.channel, pid=observed['pid'], actual_exit=observed['exitcode'],
            settled=observed['settled'], receipt=receipt, observed_before_cleanup=before))
    swallowed = any(frame['receipt'] and frame['receipt']['fault_called'] for frame in frames)
    red = (returned and swallowed) or (returned and any(frame['actual_exit'] == 23 for frame in frames)) or blocked
    return dict(baseline_red=red, legacy_operation_fault_observed=swallowed,
        legacy_parent_returned_none=returned, result=dict(legacy_parent_returned=returned, legacy_parent_blocked=blocked,
        legacy_return_is_none=returned, fake_fault_observed=swallowed,
        both_zero=all(frame['actual_exit'] == 0 for frame in frames),
        original_source_verified=True), persisted_result=None, frames=frames,
        baseline_watchdog_observed=blocked, job_used_for_settlement=job_used,
        actual_pid_settlement=guard.d2_actual_pid_settlement())


def lock_worker_entry(worker_id, case, root_string):
    guard.d2_worker_context(case, worker_id)
    import dataset_io
    import run_logging
    root = Path(root_string)
    log_path = Path(_contract()['log_root']) / worker_id / 'logs'
    session = run_logging.configure_logging(ROOT, run_id='d2-lock-' + worker_id + '-' + uuid.uuid4().hex,
        environment='offline', producer_sha=_context().tested_sha,
        config={'log_root': log_path.relative_to(ROOT).as_posix()})
    receipt = {}
    target = root / 'lock/current.csv'
    with dataset_io.resource_lock(target.name, resource_root=target.parent,
            validate_target=lambda stream: stream.read() == b'ticker,close\nAAA,1\n',
            logging_session=session, receipt=receipt, timeout_s=1.0):
        if worker_id == 'holder':
            guard.d2_fault_witness('lock_holder_ready')
            info = target.stat()
            _json(root / 'holder-ready.json', dict(pid=os.getpid(), lock_state=receipt['lock_state'],
                target_identity=[info.st_dev, info.st_ino]))
            threading.Event().wait()
    report = run_logging.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)
    data = target.read_bytes()
    info = target.stat()
    _json(root / 'contender-result.json', dict(pid=os.getpid(), lock_state=receipt['lock_state'],
        bytes=len(data), sha256=hashlib.sha256(data).hexdigest(), logging_exit=report['exit_status'],
        target_identity=[info.st_dev, info.st_ino]))
    guard.finish(0)


def _lock_controller(case, root, upload):
    target = root / 'lock/current.csv'
    target.parent.mkdir()
    original = b'ticker,close\nAAA,1\n'
    target.write_bytes(original)
    observations = []
    holder_ticket = None
    for worker_id in ('holder', 'contender'):
        process = multiprocessing.get_context('spawn').Process(target=lock_worker_entry, args=(worker_id, case, str(root)))
        ticket = guard._d2_prepare_fixture_start(process, 'd2-lock-worker', worker_id, case)
        if worker_id == 'holder':
            holder_ticket = ticket
        state = {}
        assert upload._d2_start_worker(process, state)
        deadline = time.monotonic() + 15
        if worker_id == 'holder':
            while not (root / 'holder-ready.json').exists() and time.monotonic() < deadline:
                if upload._d2_native_observation(state)['settled']:
                    raise RuntimeError('D2_LOCK_READY_FAILED')
                threading.Event().wait(.1)
            assert (root / 'holder-ready.json').exists()
            upload._d2_native_terminate(state)
        while not upload._d2_native_observation(state)['settled'] and time.monotonic() < deadline:
            threading.Event().wait(.1)
        observed = upload._d2_native_observation(state)
        assert observed['settled']
        assert upload._d2_close_launch_state(state)
        observations.append(dict(worker_id=worker_id, pid=observed['pid'], actual_exit=observed['exitcode'], settled=True))
    contender = json.loads((root / 'contender-result.json').read_text(encoding='utf-8'))
    holder = json.loads((root / 'holder-ready.json').read_text(encoding='utf-8'))
    assert contender['sha256'] == hashlib.sha256(original).hexdigest()
    holder_final = list((_context().report_dir / 'processes').glob(holder_ticket['ticket_id'] + '.final*.json'))
    holder_log = Path(_contract()['log_root']) / 'holder/logs'
    holder_terminal = False
    for path in holder_log.rglob('*.jsonl'):
        data = path.read_bytes()
        holder_terminal = holder_terminal or b'd1_lock_released' in data or b'run_completed' in data
    return dict(holder_settled=observations[0]['settled'],
        contender_acquired=contender['lock_state'] == 'RELEASED' and contender['logging_exit'] == 0,
        same_physical_target=holder['target_identity'] == contender['target_identity'],
        previous_bytes_preserved=target.read_bytes() == original,
        previous_hash_preserved=contender['sha256'] == hashlib.sha256(original).hexdigest(),
        result=dict(holder_killed=observations[0]['actual_exit'] == -15, contender=contender,
        holder_final_present=bool(holder_final), holder_terminal_present=holder_terminal,
        bytes_preserved=target.read_bytes() == original),
        persisted_result=None, frames=observations, actual_pid_settlement=guard.d2_actual_pid_settlement(),
        baseline_red=False, job_used_for_settlement=False)


def controller(case):
    c = guard.ensure_context(ROOT, lane='d2', install=True)
    if c.role != 'd2-controller' or c.ticket['d2_contract']['case'] != case:
        raise ValueError('D2_CONTROLLER_TICKET')
    c.nodeid = c.ticket['nodeid']
    root = Path(_contract()['result_root'])
    c.d2_controller_stage = 'FIXTURE_SEED'
    _seed_fixture(root)
    c.d2_controller_stage = 'IMPORT_UPLOAD'
    import upload
    guard.d2_bind_controller_source(upload)
    observations = {}
    _observe_adapter(upload, observations)
    if case.startswith('baseline_'):
        c.d2_controller_stage = 'BASELINE_EXECUTION'
        bundle = _baseline_controller(case, root, upload)
    elif case == 'd1_killed_holder':
        c.d2_controller_stage = 'LOCK_EXECUTION'
        bundle = _lock_controller(case, root, upload)
    else:
        c.d2_controller_stage = 'LOGGER_SETUP'
        import run_logging
        original_listen = run_logging._listen
        if case == 'listener_failure':
            def stopped_listener(session):
                guard.d2_fault_witness('listener_stopped', worker_id='controller')
            run_logging._listen = stopped_listener
        identifier = 'd2-' + uuid.uuid4().hex
        session = run_logging.configure_logging(ROOT, run_id=identifier, environment='offline',
            producer_sha=c.tested_sha, config={'log_root': (Path(_contract()['log_root']) / 'logs').relative_to(ROOT).as_posix()},
            sensitive_values=('D2_SYNTHETIC_CANARY', 'D2_FAKE_PASSWORD', 'D2_FAKE_PRIVATE_KEY', 'D2_FAKE_USERNAME'))
        run_logging._listen = original_listen
        upload.datasets_path = str(root / 'project/datasets') + '/'
        if case == 'success':
            issued = root / 'issued-site/site-packages'
            issued.mkdir(parents=True)
            (issued / 'd2_issued_module.py').write_text("FIXED='D2_ISSUED_MODULE'\n", encoding='utf-8')
            (issued / 'd2-unexecuted.pth').write_text("import pathlib; pathlib.Path(" + repr(str(root / 'pth-executed')) + ").write_bytes(b'BAD')\n", encoding='utf-8')
            sys.path.insert(0, str(issued))
        c.d2_controller_stage = 'SOURCE_PLAN'
        plan = upload._d2_build_source_plan()
        result_root = root / 'results'
        result_root.mkdir()
        c.d2_controller_stage = 'PREWRAPPER_SETUP'
        if case == 'bootstrap_replay_failure':
            replay = root / 'replay.py'
            replay.write_text("import sys\ng=sys.modules['mds_offline_guard']\n"
                "if g._CONTEXT.ticket['d2_contract']['worker_id']=='google':\n"
                " g.d2_write_canary_streams()\n g.d2_fault_witness('bootstrap_replay')\n"
                " raise RuntimeError(g._D2_CANARY_TEXT)\n", encoding='utf-8')
            sys.modules['__main__'].__file__ = str(replay)
        if case == 'bootstrap_import_failure':
            issued = root / 'issued-import'
            issued.mkdir()
            (issued / 'd2_import_fault.py').write_text("import sys\ng=sys.modules['mds_offline_guard']\n"
                "g.d2_write_canary_streams()\ng.d2_fault_witness('bootstrap_dependency_import')\n"
                "raise ImportError(g._D2_CANARY_TEXT)\n", encoding='utf-8')
            parent_module = types.ModuleType('d2_import_fault')
            exec("def fixed_import_target():\n return None\n", parent_module.__dict__)
            sys.modules[parent_module.__name__] = parent_module
            sys.path.insert(0, str(issued))
        if case == 'cleanup_unsettled':
            original_terminate = upload._d2_native_terminate
        else:
            original_terminate = upload._d2_native_terminate
        cleanup_requests = []
        def terminate(state):
            returned, observed = guard.d2_request_worker_cleanup(state, original_terminate)
            cleanup_requests.append(observed)
            return returned
        upload._d2_native_terminate = terminate
        foreign = None
        if case == 'foreign_handle':
            foreign = root / 'foreign-canary.bin'
            foreign.write_bytes(b'D2_FOREIGN_UNCHANGED')
            descriptor = os.open(foreign, os.O_RDWR)
            os.dup2(descriptor, 200, inheritable=True)
            os.close(descriptor)
            foreign_payload = guard.d2_create_foreign_fixture(200)
            _json(root / 'foreign.json', foreign_payload)
        parent_fence = case.startswith('parent_finalization_')
        supervision_started_at = time.monotonic()
        original_supervise = upload._d2_supervise
        persisted_before = None
        parent_faults = dict(seal_inventory_fail_count=0, finalization_raise_count=0,
            application_finalize_count=0, cleanup_finalize_count=0)
        preseal_preconditions = {}
        c.d2_controller_stage = 'NATIVE_SUPERVISION'
        if parent_fence:
            original_configure, original_finalize = run_logging.configure_logging, run_logging.finalize_logging
            original_freeze, original_inventory = run_logging._freeze_and_wait, run_logging._inventory
            original_source_sha = upload._d2_source_sha
            original_main, original_path, original_destination = upload.main, upload.current_path, upload.gdoc_to_write
            upload._d2_source_sha = lambda project_root: c.tested_sha
            def supervise(*arguments, **options):
                options.update(worker_targets={'google': google_target, 'ftp': ftp_target}, fixture_case=case)
                return original_supervise(*arguments, **options)
            upload._d2_supervise = supervise
            upload._d2_result_root = lambda project_root, run_id, attempt_id: result_root
            run_logging.configure_logging = lambda *arguments, **options: session
            def operation_fence():
                nonlocal persisted_before
                actual = guard.d2_actual_pid_settlement()
                persisted_before = (result_root / 'result.json').read_bytes()
                previous = json.loads(persisted_before)
                if (previous.get('parent_outcome') != 'RETURNED' or previous.get('error_code') is not None
                        or len(actual) != 2 or {row['worker_id'] for row in actual} != {'google', 'ftp'}
                        or any(not row['settled'] or row['actual_exit'] != 0 or not row['exit_receipt_verified'] for row in actual)
                        or not upload._d2_workers_ok(previous)):
                    raise RuntimeError('D2_FIXTURE_PHASE_NOT_REACHED')
                for worker_id in ('google', 'ftp'):
                    final_worker = session['_worker_finals'].get(session['_expected'][worker_id])
                    if (final_worker is None or final_worker['execution_outcome'] != 'COMPLETED'
                            or final_worker['exit_status'] != 0
                            or previous['workers'][worker_id]['pid'] != next(row['pid'] for row in actual if row['worker_id'] == worker_id)):
                        raise RuntimeError('D2_FIXTURE_PHASE_NOT_REACHED')
            def fail_freeze(context):
                if (context is not session or sys._getframe(1).f_code is not run_logging._finalize_logging.__code__
                        or session['_state'] != 'ACTIVE' or session['_final'] is not None
                        or session['_registration_frozen'] or session['_closed']):
                    raise RuntimeError('D2_FIXTURE_PHASE_NOT_REACHED')
                operation_fence()
                preseal_preconditions.update(fault_boundary='FREEZE_ENTRY', application_state=session['_state'],
                    final_absent=session['_final'] is None, registration_frozen=session['_registration_frozen'],
                    finalizer_lock_held=session['_finalizer_lock'].locked(),
                    summary_absent=not (session['_run_root'] / 'run_summary.json').exists(),
                    transport_closed=False, actual_children_settled=2, actual_children_exit_zero=2,
                    worker_final_completed_zero=2, persisted_operation_returned=True)
                parent_faults['finalization_raise_count'] += 1
                guard.d2_fault_witness('finalization_raises', worker_id='controller')
                raise RuntimeError('D2_SYNTHETIC_FINALIZATION_RAISED')
            def fail_inventory(path, boundary):
                if (Path(path) != session['_run_root'] or Path(boundary) != session['_checkout']
                        or sys._getframe(1).f_code is not run_logging._finalize_logging.__code__
                        or session['_state'] != 'FINALIZING' or session['_final'] is not None
                        or session['_thread'].is_alive() or not session['_listener_closed']
                        or not session['_queue']._closed
                        or any(not endpoint.closed for endpoint in (*session['_receivers'], *session['_ack_senders'].values()))):
                    raise RuntimeError('D2_FIXTURE_PHASE_NOT_REACHED')
                operation_fence()
                preseal_preconditions.update(fault_boundary='SEAL_INVENTORY', application_state=session['_state'],
                    final_absent=session['_final'] is None, registration_frozen=session['_registration_frozen'],
                    finalizer_lock_held=session['_finalizer_lock'].locked(),
                    summary_absent=not (session['_run_root'] / 'run_summary.json').exists(),
                    transport_closed=True, actual_children_settled=2, actual_children_exit_zero=2,
                    worker_final_completed_zero=2, persisted_operation_returned=True)
                parent_faults['seal_inventory_fail_count'] += 1
                guard.d2_fault_witness('seal_inventory_failure', worker_id='controller')
                raise OSError('D2_SYNTHETIC_SEAL_INVENTORY_FAILED')
            def finalize(*arguments, **options):
                c.d2_controller_stage = 'PARENT_FINALIZATION'
                parent_faults['application_finalize_count'] += 1
                operation_fence()
                if case == 'parent_finalization_raises':
                    run_logging._freeze_and_wait = fail_freeze
                else:
                    run_logging._inventory = fail_inventory
                try:
                    return original_finalize(*arguments, **options)
                finally:
                    run_logging._freeze_and_wait, run_logging._inventory = original_freeze, original_inventory
            run_logging.finalize_logging = finalize
            result = {}
            application_calls = []
            def main_with_sink(current_path, destination):
                if current_path != str(root / 'project') or destination != 'D2_FAKE_GOOGLE_DESTINATION':
                    raise RuntimeError('D2_FIXTURE_PHASE_NOT_REACHED')
                application_calls.append(1)
                return original_main(current_path, destination, result_sink=result)
            upload.main = main_with_sink
            upload.current_path, upload.gdoc_to_write = str(root / 'project'), 'D2_FAKE_GOOGLE_DESTINATION'
            try:
                main_status = upload._d2_run_cli()
            finally:
                run_logging.configure_logging, run_logging.finalize_logging = original_configure, original_finalize
                run_logging._freeze_and_wait, run_logging._inventory = original_freeze, original_inventory
                upload.main, upload.current_path, upload.gdoc_to_write = original_main, original_path, original_destination
                upload._d2_supervise = original_supervise
                upload._d2_source_sha = original_source_sha
            if len(application_calls) != 1 or parent_faults['application_finalize_count'] != 1:
                raise RuntimeError('D2_FIXTURE_PHASE_NOT_REACHED')
            preseal_preconditions['application_lock_released'] = not session['_finalizer_lock'].locked()
            if case == 'parent_finalization_raises':
                if (session['_state'] != 'ACTIVE' or session['_final'] is not None
                        or session['_registration_frozen'] or session['_finalizer_lock'].locked()
                        or result.get('storage_sealed') != 'UNKNOWN' or main_status == 0):
                    raise RuntimeError('D2_FIXTURE_PHASE_NOT_REACHED')
                parent_faults['cleanup_finalize_count'] += 1
                final = original_finalize(session, execution_outcome='FAILED', exit_status=1, release_result=None)
            else:
                final = session['_final']
                if (final is None or final['delivery_outcome'] != 'INCOMPLETE' or final['exit_status'] != 1
                        or final['storage_sealed'] is not False
                        or final['null_reasons'].get('authoritative_summary') != 'LOG_SEAL_FAILED'):
                    raise RuntimeError('D2_FIXTURE_PHASE_NOT_REACHED')
        else:
            result = upload._d2_supervise(session, plan, result_root, identifier, uuid.uuid4().hex,
                upload._d2_validate_limits(), str(root / 'project'), 'D2_FAKE_GOOGLE_DESTINATION',
                worker_targets={'google': google_target, 'ftp': ftp_target}, fixture_case=case)
        supervision_ended_at = time.monotonic()
        status = 0 if result['parent_outcome'] == 'RETURNED' else 1
        if not parent_fence:
            c.d2_controller_stage = 'PARENT_FINALIZATION'
            final = run_logging.finalize_logging(session, execution_outcome='COMPLETED' if status == 0 else 'FAILED',
                exit_status=status, release_result=None)
            upload._d2_apply_finalization(result, final, status=status)
        c.d2_controller_stage = 'RESULT_EXPORT'
        settlement = guard.d2_actual_pid_settlement()
        physically_quiescent = all(row['settled'] for row in settlement)
        persisted = None
        if physically_quiescent:
            persisted = json.loads((result_root / 'result.json').read_text(encoding='utf-8')) if (result_root / 'result.json').exists() else None
        worker_fences = {}
        for worker_id in ('google', 'ftp'):
            checkpoint_path = result_root / (worker_id + '.json')
            settled_slot = next((row for row in settlement if row['worker_id'] == worker_id), None)
            fence = dict(checkpoint_present=None, checkpoint_execution=None,
                normal_return=None, checkpoint_pid=None, checkpoint_sequence=None, checkpoint_diagnostics=None,
                checkpoint_error_code=None, checkpoint_output_states=None, flog_final=None)
            if settled_slot is not None and settled_slot['settled']:
                fence['checkpoint_present'] = checkpoint_path.exists()
            if fence['checkpoint_present']:
                raw_checkpoint = checkpoint_path.read_bytes()
                if len(raw_checkpoint) <= 65536:
                    try:
                        document = json.loads(raw_checkpoint)
                        if isinstance(document, dict):
                            fence.update(checkpoint_execution=document.get('execution_state'),
                                normal_return=document.get('normal_return_observed'), checkpoint_pid=document.get('pid'),
                                checkpoint_sequence=document.get('sequence'), checkpoint_diagnostics=document.get('diagnostics_state'),
                                checkpoint_error_code=document.get('error_code'),
                                checkpoint_output_states=[item.get('transfer_state') for item in document.get('outputs', [])])
                    except (ValueError, UnicodeError):
                        pass
            terminal = session['_worker_finals'].get(session['_expected'].get(worker_id))
            if terminal is not None:
                fence['flog_final'] = {key: terminal[key] for key in ('execution_outcome', 'exit_status')}
            worker_fences[worker_id] = fence
        frames = [dict(pid=pid, **{key: value for key, value in observed.items() if key != 'pid'}) for pid, observed in observations.items()]
        bundle = dict(result=result, persisted_result=persisted, frames=frames,
            actual_pid_settlement=settlement, baseline_red=False, job_used_for_settlement=False,
            worker_fences=worker_fences, cleanup_requests=cleanup_requests,
            logging_report={key: final.get(key) for key in ('run_id', 'producer_sha', 'execution_outcome', 'exit_status',
                'delivery_outcome', 'storage_sealed', 'evidence_incomplete', 'expected_producers', 'worker_outcomes', 'release_result')})
        bundle['logging_report']['producer_sha'] = session['producer_sha']
        bundle['reader_lifetime'] = dict(getattr(c, 'd2_reader_counts',
            dict(reads=0, lifetime_complete=0, native_closes=0, fd_closes=0)))
        bundle['phase_witnesses'] = list(getattr(c, 'd2_phase_history', []))
        bundle['ready_reader_witnesses'] = {worker_id: dict(observed)
            for worker_id, observed in getattr(c, 'd2_closed_ready', {}).items()}
        bundle['slot_effects'] = guard.d2_slot_write_witnesses()
        if case in {'hang_google', 'hang_ftp', 'cleanup_unsettled'}:
            target_path = root / ('target-entered-' + ('ftp' if case == 'hang_ftp' else 'google') + '.json')
            entered = json.loads(target_path.read_text(encoding='utf-8')) if target_path.exists() else None
            bundle.update(parent_deadline_expired=result['error_code'] == 'D2_TIMEOUT' and any(row['timed_out'] for row in result['workers'].values()),
                parent_cleanup_requested=bool(cleanup_requests),
                parent_observed_unsettled=any(row['alive'] is True and not row['settled'] for row in observations.values()),
                target_entered_before_deadline=entered is not None and entered['entered_at'] < supervision_started_at + 15.0,
                parent_deadline_elapsed_s=supervision_ended_at - (c.d2_work_deadline - 15.0),
                cleanup_requests=cleanup_requests)
        if foreign is not None:
            bundle['foreign_handle_effect_absent'] = foreign.read_bytes() == b'D2_FOREIGN_UNCHANGED'
            bundle['foreign_write_observation'] = json.loads((root / 'foreign-observation.json').read_text(encoding='utf-8'))
            bundle['foreign_event_effect_absent'] = guard.d2_finish_foreign_fixture(foreign_payload)
            bundle['foreign_event_unchanged'] = bundle['foreign_event_effect_absent']
            bundle['foreign_file_bytes_preserved'] = bundle['foreign_handle_effect_absent']
            bundle['caller_inheritance_preserved'] = foreign_payload['file_inheritable'] and foreign_payload['event_inheritable'] and os.get_inheritable(200)
            bundle['native_stdio_handle_list_exact'] = all(request.get('stdio') is not None
                and len(request['nul_duplicates']) == 2 for request in guard._D2_REQUESTS.values())
            os.close(200)
        if case == 'success':
            bundle['dependency_observation'] = json.loads((root / 'dependencies-google.json').read_text(encoding='utf-8'))
            bundle['dependency_observation']['pth_positive_control_executed'] = guard.d2_site_positive_control(issued)
        if parent_fence:
            persisted_after = (result_root / 'result.json').read_bytes()
            bundle.update(persisted_result_before_sha256=hashlib.sha256(persisted_before).hexdigest(),
                persisted_result_after_sha256=hashlib.sha256(persisted_after).hexdigest(),
                persisted_bytes_unchanged=persisted_before == persisted_after,
                main_status=main_status, operation_fence_complete=all(row['exitcode'] == 0 and row['settled'] for row in result['workers'].values()))
            bundle['parent_fault_invocations'] = parent_faults
            bundle['parent_faults'] = parent_faults
            bundle['preseal_preconditions'] = preseal_preconditions
            bundle['logging_report_scope'] = 'TEST_CLEANUP' if case == 'parent_finalization_raises' else 'APPLICATION'
            bundle['authoritative_summary_reason'] = final.get('null_reasons', {}).get('authoritative_summary')
            bundle['persisted_sha256_before'] = bundle['persisted_result_before_sha256']
            bundle['persisted_sha256_after'] = bundle['persisted_result_after_sha256']
        if case == 'canary_positive_control':
            print('D2_SYNTHETIC_CANARY_CAPTURE_POSITIVE', flush=True)
    c.d2_controller_stage = 'DECLARATION'
    bundle.update(version=1, case=case, controller_pid=os.getpid(), fault_witnesses=guard.d2_fault_witnesses())
    _json(root / 'controller-result.json', bundle)
    guard.d2_declare_completed_case(bundle)
    if case == 'cleanup_unsettled':
        c.d2_controller_stage = 'AFTER_RESULT_CONTAINMENT'
        guard.d2_contain_after_result(case)
    exit_status = 0 if case.startswith('baseline_') or case == 'd1_killed_holder' else bundle['result']['final_exit_status']
    c.d2_controller_stage = 'GUARD_FINALIZATION'
    guard.finish(exit_status)
    return exit_status


def main():
    if len(sys.argv) != 3 or sys.argv[1] != '--case' or sys.argv[2] not in guard.D2_CASES:
        raise ValueError('D2_CONTROLLER_ARGUMENTS')
    # Stable importable module identity is required by real Windows spawn.
    import d2_worker
    return d2_worker.controller(sys.argv[2])


if __name__ == '__main__':
    try:
        code = main()
    except BaseException as error:
        sys.stderr.write('D2_CONTROLLER_FAILED\n')
        try:
            guard.d2_controller_error(error)
            guard.d2_controller_exception_cleanup()
            guard.finish(1)
        except BaseException:
            sys.stderr.write('D2_GUARD_FINAL_FAILED\n')
        code = 1
    raise SystemExit(code)
