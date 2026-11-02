# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
import time, datetime, ftplib, sys, json
import os, stat, unicodedata, uuid, multiprocessing, re
from pathlib import Path
from os import listdir
from os.path import isfile, join

from multiprocessing import Process
pd = pygsheets = None
import hashlib
import os
import signal
import sys
import threading
import types


_D2_WINDOWS_START_LOCK = threading.Lock()
_D2_WINDOWS_START_LOCAL = threading.local()
_D2_WINDOWS_SOURCE_HASHES = {
    'popen_spawn_win32.py': '92731df48729b713148f0de65cb21bbb60a2602a490abb84ab102e13a7799bb1',
    'spawn.py': '9477cd637a5d3957173c06db09fb9448fee612d0cf3b094a9548855cf8a0a5c2',
    'process.py': '856bf9fbf407a1c0f08bca765a251aa7f39467c7dbe004c2891f960041c84979',
    'util.py': 'b5875b7f7238afbe01ad42740f3cd1d183ff0c80c464055f1e40e887a3c90ac8',
    'context.py': 'e1accac02502a67eb7926fc5738e803566258244c33c09cb65093a3db72f52a3',
}
_D2_WINDOWS_EXE_HASH = '434b361fe0c5960d404974feed493db47f2603a1ace2574312104fbed0182c6f'
_D2_WINDOWS_READER_BINDINGS = None
_D2_WINDOWS_READER_MODULES = None


def _d2_adapter_error(code):
    raise RuntimeError(code) from None


def _d2_verified_source(module, filename):
    source_path = getattr(module, '__file__', None)
    if not isinstance(source_path, str) or os.path.basename(source_path) != filename:
        _d2_adapter_error('D2_NATIVE_SOURCE_PROFILE')
    with open(source_path, 'rb') as source_file:
        source_bytes = source_file.read(262145)
    if (len(source_bytes) > 262144
            or hashlib.sha256(source_bytes).hexdigest() != _D2_WINDOWS_SOURCE_HASHES[filename]):
        _d2_adapter_error('D2_NATIVE_SOURCE_PROFILE')
    return compile(source_bytes, source_path, 'exec', dont_inherit=True,
                   optimize=sys.flags.optimize)


def _d2_find_code(module_code, name):
    matches = [value for value in module_code.co_consts
               if isinstance(value, types.CodeType) and value.co_name == name]
    if len(matches) != 1:
        _d2_adapter_error('D2_NATIVE_SOURCE_PROFILE')
    return matches[0]


def _d2_verified_function(function, module, expected_code):
    if (not isinstance(function, types.FunctionType)
            or function.__globals__ is not vars(module)
            or function.__closure__ is not None):
        _d2_adapter_error('D2_NATIVE_BINDING_PROFILE')
    actual = function.__code__
    for field in ('co_code', 'co_consts', 'co_names', 'co_varnames',
                  'co_argcount', 'co_kwonlyargcount', 'co_flags'):
        if getattr(actual, field) != getattr(expected_code, field):
            _d2_adapter_error('D2_NATIVE_BINDING_PROFILE')


def _d2_windows_profile():
    try:
        if (sys.platform != 'win32' or sys.implementation.name != 'cpython'
                or sys.version_info[:3] != (3, 14, 8)
                or getattr(sys, 'frozen', False)
                or not hasattr(sys, '_is_gil_enabled') or not sys._is_gil_enabled()):
            _d2_adapter_error('D2_NATIVE_PLATFORM_UNSUPPORTED')
        import multiprocessing.context as context
        import multiprocessing.popen_spawn_win32 as windows
        import multiprocessing.process as process_module
        import multiprocessing.spawn as spawn
        import multiprocessing.util as util
        import _winapi
        import msvcrt
        import subprocess

        modules = {'popen_spawn_win32.py': windows, 'spawn.py': spawn,
                   'process.py': process_module, 'util.py': util, 'context.py': context}
        sources = {name: _d2_verified_source(module, name)
                   for name, module in modules.items()}
        _d2_verified_function(spawn.get_command_line, spawn,
                              _d2_find_code(sources['spawn.py'], 'get_command_line'))
        _d2_verified_function(windows._close_handles, windows,
                              _d2_find_code(sources['popen_spawn_win32.py'], '_close_handles'))
        popen_code = _d2_find_code(sources['popen_spawn_win32.py'], 'Popen')
        _d2_verified_function(windows.Popen.__init__, windows,
                              _d2_find_code(popen_code, '__init__'))
        finalize_code = _d2_find_code(sources['util.py'], 'Finalize')
        _d2_verified_function(util.Finalize.__init__, util,
                              _d2_find_code(finalize_code, '__init__'))
        _d2_verified_function(context.get_spawning_popen, context,
                              _d2_find_code(sources['context.py'], 'get_spawning_popen'))
        _d2_verified_function(context.set_spawning_popen, context,
                              _d2_find_code(sources['context.py'], 'set_spawning_popen'))

        parent_exe = os.path.normcase(os.path.abspath(sys.executable))
        base_exe = os.path.normcase(os.path.abspath(sys._base_executable))
        if os.path.normcase(os.path.abspath(os.fsdecode(spawn.get_executable()))) != parent_exe:
            _d2_adapter_error('D2_NATIVE_EXECUTABLE_PROFILE')
        with open(base_exe, 'rb') as executable_file:
            digest = hashlib.file_digest(executable_file, 'sha256').hexdigest()
        if digest != _D2_WINDOWS_EXE_HASH:
            _d2_adapter_error('D2_NATIVE_EXECUTABLE_PROFILE')
        if windows.WINENV != (parent_exe != base_exe):
            _d2_adapter_error('D2_NATIVE_EXECUTABLE_PROFILE')
        return {'windows': windows, 'spawn': spawn, 'util': util,
                'winapi': _winapi, 'msvcrt': msvcrt, 'subprocess': subprocess,
                'parent_exe': parent_exe, 'base_exe': base_exe,
                'windows_venv': windows.WINENV,
                'profile_id': 'windows-cpython-3.14.8-gil',
                'exe_sha256': _D2_WINDOWS_EXE_HASH,
                'source_sha256': dict(_D2_WINDOWS_SOURCE_HASHES)}
    except BaseException:
        _d2_adapter_error('D2_NATIVE_PROFILE_FAILED')


def _d2_remember_error(state, code):
    if state.get('_adapter_error') is None:
        state['_adapter_error'] = code


def _d2_capture_vendor_finalizer(process, state):
    if not state.get('_vendor_transferred') or state.get('_vendor_finalizer') is not None:
        return
    profile = state['_profile']
    windows, util = profile['windows'], profile['util']
    pair = (state['_original_process_handle'], state['_bootstrap_pipe_handle'])
    matches = []
    for finalizer in tuple(util._finalizer_registry.values()):
        if (type(finalizer) is util.Finalize
                and getattr(finalizer, '_callback', None) is windows._close_handles
                and getattr(finalizer, '_args', None) == pair
                and getattr(finalizer, '_kwargs', None) == {}
                and getattr(finalizer, '_pid', None) == os.getpid()
                and util._finalizer_registry.get(getattr(finalizer, '_key', None)) is finalizer):
            referent = finalizer._weakref()
            if (type(referent) is windows.Popen
                    and getattr(referent, '_handle', None) == pair[0]
                    and getattr(referent, 'sentinel', None) == pair[0]
                    and getattr(referent, 'pid', None) == state['_native_pid']
                    and getattr(referent, 'finalizer', None) is finalizer):
                matches.append((finalizer, referent))
    if len(matches) == 1:
        state['_vendor_finalizer'], state['_vendor_popen'] = matches[0]
    else:
        _d2_remember_error(state, 'D2_NATIVE_FINALIZER_UNKNOWN')


def _d2_start_worker(process, state):
    # Все handles и внутренние ссылки остаются только в private state.
    if not isinstance(state, dict) or '_profile' in state:
        _d2_adapter_error('D2_NATIVE_STATE_INVALID')
    state.update(_adapter_error=None, _native_pid=None, _native_handle=None,
                 _native_entered=False, _native_creation_unknown=False,
                 _vendor_transferred=False, _vendor_finalizer=None,
                 _vendor_popen=None, _closed=False, _start_returned=False)
    if not _D2_WINDOWS_START_LOCK.acquire(blocking=False):
        _d2_remember_error(state, 'D2_NATIVE_START_BUSY')
        return False
    original_command = original_create = observer = command_hook = None
    guard = token = None
    try:
        profile = state['_profile'] = _d2_windows_profile()
        if getattr(process, '_start_method', None) not in {None, 'spawn'}:
            _d2_adapter_error('D2_NATIVE_START_METHOD')
        spawn, winapi = profile['spawn'], profile['winapi']
        guard = sys.modules.get('mds_offline_guard')
        if guard is not None:
            token = guard.d2_begin_native_launch(process)
        original_create = winapi.CreateProcess
        original_command = spawn.get_command_line
        if guard is None:
            if (not isinstance(original_create, types.BuiltinFunctionType)
                    or original_create.__module__ != '_winapi'
                    or original_create.__name__ != 'CreateProcess'):
                _d2_adapter_error('D2_NATIVE_FOREIGN_BINDING')
            guard_prefix = ''
        else:
            if guard.d2_native_identity(original_create, token) is not True:
                _d2_adapter_error('D2_NATIVE_FOREIGN_BINDING')
            guard_prefix = guard.d2_bootstrap_prefix(token)
            if (not isinstance(guard_prefix, str) or not guard_prefix.isascii()
                    or any(value in guard_prefix for value in ('"', '\r', '\n', '\0'))):
                _d2_adapter_error('D2_NATIVE_BOOTSTRAP_PROFILE')
        if guard_prefix and not guard_prefix.endswith(';'):
            guard_prefix += ';'
        quiet_prefix = ("import os,sys;_d2_null=open(os.devnull,'w');"
                        "os.dup2(_d2_null.fileno(),1,inheritable=False);"
                        "os.dup2(_d2_null.fileno(),2,inheritable=False);"
                        "sys.stdout=sys.stderr=sys.__stdout__=sys.__stderr__=_d2_null;")
        request = object()
        _D2_WINDOWS_START_LOCAL.request = request

        def command_hook(**arguments):
            if getattr(_D2_WINDOWS_START_LOCAL, 'request', None) is not request:
                return original_command(**arguments)
            if (set(arguments) != {'parent_pid', 'pipe_handle'}
                    or type(arguments['parent_pid']) is not int
                    or arguments['parent_pid'] != os.getpid()
                    or type(arguments['pipe_handle']) is not int
                    or arguments['pipe_handle'] <= 0):
                _d2_adapter_error('D2_NATIVE_COMMAND_PROFILE')
            state['_bootstrap_pipe_handle'] = arguments['pipe_handle']
            vendor_program = ('from multiprocessing.spawn import spawn_main; '
                              'spawn_main(parent_pid=%d, pipe_handle=%d)'
                              % (arguments['parent_pid'], arguments['pipe_handle']))
            return [spawn.get_executable(), '-I', '-S', '-B', '-X', 'utf8', '-c',
                    quiet_prefix + guard_prefix + vendor_program,
                    '--multiprocessing-fork']

        def observer(*arguments, **keywords):
            if getattr(_D2_WINDOWS_START_LOCAL, 'request', None) is not request:
                return original_create(*arguments, **keywords)
            if (keywords or len(arguments) != 9 or state['_native_entered']
                    or arguments[2:4] != (None, None) or arguments[4] is not False
                    or arguments[5] != 0 or arguments[7] is not None):
                _d2_adapter_error('D2_NATIVE_CREATE_PROFILE')
            expected_exe = profile['base_exe'] if profile['windows_venv'] else profile['parent_exe']
            if os.path.normcase(os.path.abspath(os.fsdecode(arguments[0]))) != expected_exe:
                _d2_adapter_error('D2_NATIVE_EXECUTABLE_PROFILE')
            incoming = arguments[8]
            if (type(incoming) is not profile['subprocess'].STARTUPINFO
                    or incoming.dwFlags != profile['subprocess'].STARTF_FORCEOFFFEEDBACK
                    or incoming.wShowWindow != 0
                    or any(value is not None for value in
                           (incoming.hStdInput, incoming.hStdOutput, incoming.hStdError))
                    or incoming.lpAttributeList != {'handle_list': []}):
                _d2_adapter_error('D2_NATIVE_STARTUPINFO_PROFILE')
            descriptors, duplicates = [], []
            try:
                for flags in (os.O_RDONLY, os.O_WRONLY):
                    descriptor = os.open(os.devnull, flags)
                    descriptors.append(descriptor)
                    if os.get_inheritable(descriptor):
                        _d2_adapter_error('D2_NATIVE_STDIO_PROFILE')
                    base_handle = profile['msvcrt'].get_osfhandle(descriptor)
                    duplicate = winapi.DuplicateHandle(winapi.GetCurrentProcess(), base_handle,
                                                      winapi.GetCurrentProcess(), 0, True,
                                                      winapi.DUPLICATE_SAME_ACCESS)
                    duplicates.append(duplicate)
                if guard is not None:
                    guard.d2_register_native_stdio(token, duplicates[0], duplicates[1])
                startup = incoming.copy()
                startup.dwFlags |= profile['subprocess'].STARTF_USESTDHANDLES
                startup.hStdInput = duplicates[0]
                startup.hStdOutput = startup.hStdError = duplicates[1]
                startup.lpAttributeList = {'handle_list': list(duplicates)}
                parameters = list(arguments)
                parameters[4], parameters[8] = True, startup
                if guard is not None:
                    parameters[6] = guard.d2_child_environment(token, parameters[6])
                state['_native_entered'] = True
                try:
                    result = original_create(*parameters)
                except BaseException:
                    state['_native_creation_unknown'] = True
                    raise
                hp, ht, pid, tid = result
                state['_original_process_handle'] = hp
                state['_native_pid'] = pid
                try:
                    state['_native_handle'] = winapi.DuplicateHandle(
                        winapi.GetCurrentProcess(), hp, winapi.GetCurrentProcess(),
                        0, False, winapi.DUPLICATE_SAME_ACCESS)
                except BaseException:
                    # Не теряем witness: hp ещё наш, vendor его не получил.
                    state['_native_handle'] = hp
                    try:
                        winapi.TerminateProcess(hp, profile['windows'].TERMINATE)
                        state['_termination_requested'] = True
                        state['_terminate_requests'] = state.get('_terminate_requests', 0) + 1
                    except BaseException:
                        _d2_remember_error(state, 'D2_NATIVE_TERMINATE_FAILED')
                    try:
                        winapi.CloseHandle(ht)
                    except BaseException:
                        _d2_remember_error(state, 'D2_NATIVE_THREAD_CLOSE_FAILED')
                    _d2_adapter_error('D2_NATIVE_WITNESS_FAILED')
                state['_vendor_transferred'] = True
                return result
            finally:
                for handle in reversed(duplicates):
                    try:
                        winapi.CloseHandle(handle)
                    except BaseException:
                        _d2_remember_error(state, 'D2_NATIVE_STDIO_CLOSE_FAILED')
                for descriptor in reversed(descriptors):
                    try:
                        os.close(descriptor)
                    except BaseException:
                        _d2_remember_error(state, 'D2_NATIVE_STDIO_CLOSE_FAILED')

        spawn.get_command_line = command_hook
        winapi.CreateProcess = observer
        try:
            process.start()
            state['_start_returned'] = True
        except BaseException:
            # Во время except traceback ещё удерживает partial vendor Popen.
            _d2_capture_vendor_finalizer(process, state)
            _d2_remember_error(state, 'D2_NATIVE_START_FAILED')
        else:
            _d2_capture_vendor_finalizer(process, state)
            if state.get('_native_pid') is None or state.get('_native_handle') is None:
                _d2_remember_error(state, 'D2_NATIVE_WITNESS_MISSING')
    except BaseException:
        _d2_remember_error(state, 'D2_NATIVE_START_FAILED')
    finally:
        if original_command is not None and command_hook is not None:
            if state['_profile']['spawn'].get_command_line is command_hook:
                state['_profile']['spawn'].get_command_line = original_command
            else:
                _d2_remember_error(state, 'D2_NATIVE_HOOK_DRIFT')
        if original_create is not None and observer is not None:
            if state['_profile']['winapi'].CreateProcess is observer:
                state['_profile']['winapi'].CreateProcess = original_create
            else:
                _d2_remember_error(state, 'D2_NATIVE_HOOK_DRIFT')
        _D2_WINDOWS_START_LOCAL.request = None
        if token is not None:
            try:
                guard.d2_end_native_launch(token)
            except BaseException:
                _d2_remember_error(state, 'D2_NATIVE_GUARD_CLEANUP_FAILED')
        _D2_WINDOWS_START_LOCK.release()
    return state['_start_returned'] and state['_adapter_error'] is None


def _d2_native_observation(state):
    result = {'started': state.get('_native_pid') is not None,
              'pid': state.get('_native_pid'), 'settled': False,
              'exitcode': None, 'alive': None,
              'error_code': state.get('_adapter_error')}
    if state.get('_closed'):
        result.update(state['_last_observation'])
        result['error_code'] = state.get('_adapter_error')
        return result
    handle = state.get('_native_handle')
    if handle is None:
        previous = state.get('_last_observation')
        if isinstance(previous, dict) and previous.get('settled') is True:
            result.update(previous)
            result['error_code'] = state.get('_adapter_error')
        elif not state.get('_native_entered'):
            result.update(settled=True, alive=False)
        else:
            result['error_code'] = result['error_code'] or 'D2_NATIVE_CREATION_UNKNOWN'
        return result
    winapi = state['_profile']['winapi']
    try:
        waited = winapi.WaitForSingleObject(handle, 0)
        if waited == winapi.WAIT_OBJECT_0:
            actual_exit = winapi.GetExitCodeProcess(handle)
            state['_raw_windows_exitcode'] = actual_exit
            if actual_exit == state['_profile']['windows'].TERMINATE:
                actual_exit = -signal.SIGTERM
            result.update(settled=True, alive=False,
                          exitcode=actual_exit)
        elif waited == winapi.WAIT_TIMEOUT:
            result['alive'] = True
        else:
            _d2_remember_error(state, 'D2_NATIVE_WAIT_FAILED')
    except BaseException:
        _d2_remember_error(state, 'D2_NATIVE_WAIT_FAILED')
    result['error_code'] = state.get('_adapter_error')
    state['_last_observation'] = dict(result)
    return result


def _d2_native_terminate(state):
    observed = _d2_native_observation(state)
    if observed['settled']:
        return True
    handle = state.get('_native_handle')
    if handle is None:
        return False
    try:
        state['_profile']['winapi'].TerminateProcess(handle, state['_profile']['windows'].TERMINATE)
        state['_termination_requested'] = True
        state['_terminate_requests'] = state.get('_terminate_requests', 0) + 1
        return True
    except BaseException:
        if _d2_native_observation(state)['settled']:
            return True
        _d2_remember_error(state, 'D2_NATIVE_TERMINATE_FAILED')
        return False


def _d2_close_launch_state(state):
    if state.get('_closed'):
        return True
    observed = _d2_native_observation(state)
    if not observed['settled']:
        _d2_remember_error(state, 'D2_NATIVE_CLOSE_UNSETTLED')
        return False
    state['_last_observation'] = dict(observed)
    completed = True
    if state.get('_vendor_transferred'):
        # Cache только после WAIT_OBJECT_0 retained witness, не по Process.pid.
        referent = state.get('_vendor_popen')
        if referent is not None and type(observed['exitcode']) is int:
            referent.returncode = observed['exitcode']
        finalizer = state.get('_vendor_finalizer')
        if finalizer is None or state.get('_vendor_release_failed'):
            _d2_remember_error(state, 'D2_NATIVE_FINALIZER_UNKNOWN')
            completed = False
        else:
            try:
                finalizer()
                state['_vendor_released'] = True
            except BaseException:
                state['_vendor_release_failed'] = True
                _d2_remember_error(state, 'D2_NATIVE_FINALIZER_FAILED')
                completed = False
    handle = state.get('_native_handle')
    if handle is not None:
        try:
            state['_profile']['winapi'].CloseHandle(handle)
            state['_native_handle'] = None
        except BaseException:
            _d2_remember_error(state, 'D2_NATIVE_WITNESS_CLOSE_FAILED')
            completed = False
    state['_closed'] = completed
    state['_vendor_popen'] = None
    return completed




current_path = sys.path[0]

## Префикс секретов
secrets_prefix = current_path.rsplit("/", 1)[0] + "/secrets/"

## Гугл док для сохранения данных
gdoc_to_write = "https://docs.google.com/spreadsheets/d/1HXXoxcDVqIrWN6QEg5ij88AxcNAKT-G-xm2UTUfQe1Q/edit?usp=sharing"


datasets_path = current_path + "/datasets/"


## Начальное время
start_time = time.time()

# %%
def _d2_gdoc_upload(current_path, gdoc_to_write, *, source_plan=None, output_observer=None):
    _d2_google_dependencies()
    print('Начал загрузку данных на Google Диск и FTP.\nВремя старта: {}'.format(datetime.datetime.now()))

    ## Префикс секретов
    secrets_prefix = current_path.rsplit("/", 1)[0] + "/secrets/"

    service_account_json = secrets_prefix + '/python-304621-1e403209f05f.json'
    gc = pygsheets.authorize(service_file=service_account_json)

    ### Подготовка данных для гугл дока
    if source_plan is None:
        df = pd.read_csv(datasets_path + "10years_data_1d_interval.csv")
    else:
        item = source_plan[0]
        with open(item['source_path'], 'rb') as source:
            _d2_check_source_handle(source, item)
            df = pd.read_csv(source)
            _d2_check_source_handle(source, item)
    # df = df.round(decimals = 2) #округление до 2 цифр
    # df.sort_values(by=0, ascending=True, inplace=True) #сортировка

    ### Убирание ненужных колонок, чтобы не упираться в лимит в 10кк ячеек гугл доков
    columns = df.columns
    white_list_columns = ['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end',
        'ticker', 'RSI14']
    columns_to_remove = [i for i in columns if i not in white_list_columns]
    columns_to_remove
    df.drop(columns_to_remove, axis=1,inplace=True)


    ### Загрузка данных на гугл док

    try:
        google_doc = gc.open_by_url(gdoc_to_write)

        #opening needed sheet
        needed_sheet = google_doc[0]

        #clearing all data
        if output_observer is not None:
            _d2_check_source(item)
            output_observer(item['output_id'], 'UNKNOWN')
        needed_sheet.clear()

        #writing data
        needed_sheet.set_dataframe(df, 'A1', copy_index=False, copy_head=True, fit=False, escape_formulae=False, nan='NaN')
        if output_observer is not None:
            _d2_check_source(item)
            output_observer(item['output_id'], 'ACKNOWLEDGED')
    except Exception as e:
        # Сообщение библиотеки может содержать адрес назначения или секрет.
        raise RuntimeError(_d2_code(e, 'D2_GOOGLE_FAILED')) from None


    # конечное время
    end_time = time.time()

    # разница между конечным и начальным временем
    elapsed_time = end_time - start_time

    mins, secs = divmod(elapsed_time, 60)
    hours, mins = divmod(mins, 60)

    print('Обновление данных на Google Диске закончено.\nЗаняло времени: {} часов, {} минут, {} секунд. Суммарно в секундах: {}'. format(round(hours), round(mins), round(secs), round(elapsed_time, 3), 'сек'))
    print('-----------------------')

# %%
def gdoc_upload(current_path, gdoc_to_write, *, source_plan=None, output_observer=None):
    try:
        return _d2_gdoc_upload(current_path, gdoc_to_write,
            source_plan=source_plan, output_observer=output_observer)
    except BaseException as error:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        raise RuntimeError(_d2_code(error, 'D2_GOOGLE_FAILED')) from None


def ftp_upload(current_path, *, source_plan=None, output_observer=None):
    secrets_prefix = current_path.rsplit("/", 1)[0] + "/secrets/"
    ftp_server = None
    failed = False
    try:
        ## Получение секрета подключения к FTP
        with open(secrets_prefix + "moex.foykes.com.json") as ftp_secrets_path:
            ftp_data = json.load(ftp_secrets_path)
        ftp_server = ftplib.FTP(ftp_data['domain'], ftp_data['username'], ftp_data['password'])
        ftp_server.encoding = "utf-8"

        if source_plan is None:
            source_plan = []
            # Сохраняется прежний shallow scope, включая обычные файлы вроде README.
            for group in ('root', 'dividends', 'ticker_lists'):
                folder = Path(datasets_path) if group == 'root' else Path(datasets_path) / group
                for name in list(set(f for f in listdir(folder) if isfile(join(folder, f)))):
                    destination = name if group == 'root' else group + '/' + name
                    source_plan.append(dict(source_path=str(folder / name), destination=destination))

        for item in source_plan:
            with open(item['source_path'], 'rb') as source:
                if output_observer is not None:
                    _d2_check_source_handle(source, item)
                    _d2_check_source(item)
                    output_observer(item['output_id'], 'UNKNOWN')
                ftp_server.storbinary('STOR ' + item['destination'], source)
                if output_observer is not None:
                    _d2_check_source_handle(source, item)
                    _d2_check_source(item)
                    output_observer(item['output_id'], 'ACKNOWLEDGED')
    except BaseException as error:
        failed = True
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        raise RuntimeError(_d2_code(error, 'D2_FTP_FAILED')) from None
    finally:
        if ftp_server is not None:
            try:
                ftp_server.quit()
            except BaseException:
                if not failed:
                    raise RuntimeError('D2_FTP_CLOSE_FAILED') from None
            finally:
                try:
                    ftp_server.close()
                except BaseException:
                    if not failed:
                        raise RuntimeError('D2_FTP_CLOSE_FAILED') from None


    # конечное время
    end_time = time.time()

    # разница между конечным и начальным временем
    elapsed_time = end_time - start_time

    mins, secs = divmod(elapsed_time, 60)
    hours, mins = divmod(mins, 60)

    print('Загрузка данных на FTP закончена.\nЗаняло времени: {} часов, {} минут, {} секунд. Суммарно в секундах: {}'. format(round(hours), round(mins), round(secs), round(elapsed_time, 3), 'сек'))
    print('-----------------------')

# %%
def _d2_google_dependencies():
    global pd, pygsheets
    try:
        if pd is None:
            import pandas
            pd = pandas
        if pygsheets is None:
            import pygsheets as google_client
            pygsheets = google_client
    except Exception:
        raise RuntimeError('D2_GOOGLE_FAILED') from None


_D2_LIMITS = dict(max_ftp_outputs=512, max_checkpoint_bytes=65536,
    max_result_bytes=131072, max_run_bytes=1048576)
_D2_CODES = {'D2_SOURCE_INVALID', 'D2_SOURCE_CHANGED', 'D2_SOURCE_COLLISION',
    'D2_SOURCE_MISSING', 'D2_SOURCE_EMPTY', 'D2_OUTPUT_LIMIT', 'D2_LIMITS_INVALID',
    'D2_RESULT_LIMIT', 'D2_RESULT_WRITE_FAILED', 'D2_CHECKPOINT_INVALID',
    'D2_CHECKPOINT_WRITE_FAILED', 'D2_GOOGLE_FAILED', 'D2_FTP_FAILED',
    'D2_FTP_CLOSE_FAILED', 'D2_CHILD_FAILED', 'D2_CHILD_EXIT', 'D2_START_FAILED',
    'D2_TIMEOUT', 'D2_INTERRUPTED', 'D2_DIAGNOSTICS_INCOMPLETE',
    'D2_CLEANUP_UNSETTLED', 'D2_PROFILE_UNSUPPORTED', 'D2_FIXTURE_REFUSED'}
_D2_CODES.update(['D2_NATIVE_BINDING_PROFILE','D2_NATIVE_BOOTSTRAP_PROFILE','D2_NATIVE_CLOSE_UNSETTLED','D2_NATIVE_COMMAND_PROFILE','D2_NATIVE_CREATE_PROFILE','D2_NATIVE_CREATION_UNKNOWN','D2_NATIVE_EXECUTABLE_PROFILE','D2_NATIVE_FINALIZER_FAILED','D2_NATIVE_FINALIZER_UNKNOWN','D2_NATIVE_FOREIGN_BINDING','D2_NATIVE_GUARD_CLEANUP_FAILED','D2_NATIVE_HOOK_DRIFT','D2_NATIVE_PLATFORM_UNSUPPORTED','D2_NATIVE_PROFILE_FAILED','D2_NATIVE_SOURCE_PROFILE','D2_NATIVE_STARTUPINFO_PROFILE','D2_NATIVE_START_BUSY','D2_NATIVE_START_FAILED','D2_NATIVE_START_METHOD','D2_NATIVE_STATE_INVALID','D2_NATIVE_STDIO_CLOSE_FAILED','D2_NATIVE_STDIO_PROFILE','D2_NATIVE_TERMINATE_FAILED','D2_NATIVE_THREAD_CLOSE_FAILED','D2_NATIVE_WAIT_FAILED','D2_NATIVE_WITNESS_CLOSE_FAILED','D2_NATIVE_WITNESS_FAILED','D2_NATIVE_WITNESS_MISSING'])


def _d2_code(error, fallback='D2_CHILD_FAILED'):
    if (type(error) is RuntimeError and len(error.args) == 1
            and type(error.args[0]) is str and error.args[0] in _D2_CODES):
        return error.args[0]
    return fallback


def _d2_clone(value):
    return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False))


def _d2_validate_limits(diagnostic_limits=None):
    result = dict(_D2_LIMITS)
    if diagnostic_limits is not None:
        if type(diagnostic_limits) is not dict or set(diagnostic_limits) - set(result):
            raise RuntimeError('D2_LIMITS_INVALID')
        for key, value in diagnostic_limits.items():
            if type(value) is not int or value <= 0 or value > result[key]:
                raise RuntimeError('D2_LIMITS_INVALID')
            result[key] = value
    return result


def _d2_runtime_profile(fixture_case=None):
    profile = dict(work_seconds=3600.0, poll_seconds=0.1,
        cleanup_seconds=10.0, logging_seconds=10.0)
    if fixture_case is not None:
        guard = sys.modules.get('mds_offline_guard')
        admitted = getattr(guard, '_d2_fixture_profile_admitted', None)
        if admitted is None or admitted(fixture_case) is not True:
            raise RuntimeError('D2_FIXTURE_REFUSED')
        profile['work_seconds'] = 15.0
    return profile


def _d2_identity(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)


def _d2_plain(info, directory=False):
    expected = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
    return expected and not (getattr(info, 'st_file_attributes', 0) & 1024) and (directory or info.st_nlink == 1)


def _d2_checked_path(path, *, directory=False):
    path = Path(os.path.abspath(path))
    if str(path).startswith('\\\\'):
        raise RuntimeError('D2_SOURCE_INVALID')
    try:
        for parent in reversed(path.parents):
            if not _d2_plain(parent.lstat(), directory=True):
                raise RuntimeError('D2_SOURCE_INVALID')
        info = path.lstat()
        if not _d2_plain(info, directory=directory):
            raise RuntimeError('D2_SOURCE_INVALID')
        return info
    except (OSError, ValueError):
        raise RuntimeError('D2_SOURCE_INVALID') from None


def _d2_check_source_handle(source, item):
    try:
        info = os.fstat(source.fileno())
        if _d2_plain(info) and _d2_identity(info) == tuple(item['identity']):
            return
    except (OSError, ValueError):
        pass
    raise RuntimeError('D2_SOURCE_CHANGED') from None


def _d2_check_source(item):
    try:
        if _d2_identity(_d2_checked_path(item['source_path'])) == tuple(item['identity']):
            return
    except (OSError, ValueError, RuntimeError):
        pass
    raise RuntimeError('D2_SOURCE_CHANGED') from None


def _d2_build_source_plan():
    root = Path(os.path.abspath(datasets_path))
    result = dict(directories=[], google=[], ftp=[])
    destinations = set()
    try:
        for group in ('root', 'dividends', 'ticker_lists'):
            folder = root if group == 'root' else root / group
            info = _d2_checked_path(folder, directory=True)
            result['directories'].append(dict(path=str(folder), identity=(info.st_dev, info.st_ino)))
            names = []
            with os.scandir(folder) as entries:
                for entry in entries:
                    entry_info = entry.stat(follow_symlinks=False)
                    path = folder / entry.name
                    if stat.S_ISDIR(entry_info.st_mode) and not (getattr(entry_info, 'st_file_attributes', 0) & 1024):
                        _d2_checked_path(path, directory=True)
                        continue
                    # Windows DirEntry cache не содержит полного file/link identity.
                    entry_info = _d2_checked_path(path)
                    if not _d2_plain(entry_info):
                        raise RuntimeError('D2_SOURCE_INVALID')
                    names.append(entry.name)
                    if len(result['ftp']) + len(names) > _D2_LIMITS['max_ftp_outputs']:
                        raise RuntimeError('D2_OUTPUT_LIMIT')
            for name in sorted(names):
                if any(ord(char) < 32 for char in name) or '\\' in name or '/' in name:
                    raise RuntimeError('D2_SOURCE_INVALID')
                destination = name if group == 'root' else group + '/' + name
                key = unicodedata.normalize('NFC', destination).casefold()
                if key in destinations:
                    raise RuntimeError('D2_SOURCE_COLLISION')
                destinations.add(key)
                path = folder / name
                info = _d2_checked_path(path)
                item = dict(output_id='ftp-' + str(len(result['ftp']) + 1).zfill(6),
                    group=group, source_path=str(path), destination=destination,
                    identity=_d2_identity(info))
                with path.open('rb') as source:
                    _d2_check_source_handle(source, item)
                    source.read(1)
                    _d2_check_source_handle(source, item)
                _d2_check_source(item)
                result['ftp'].append(item)
                if group == 'root' and name == '10years_data_1d_interval.csv':
                    if info.st_size == 0:
                        raise RuntimeError('D2_SOURCE_EMPTY')
                    google = dict(item)
                    google.update(output_id='google-000001', group='google', destination='google')
                    result['google'] = [google]
        if not result['google']:
            raise RuntimeError('D2_SOURCE_MISSING')
        return result
    except (OSError, ValueError):
        raise RuntimeError('D2_SOURCE_INVALID') from None


def _d2_recheck_source_plan(plan):
    try:
        current = _d2_build_source_plan()
    except Exception:
        raise RuntimeError('D2_SOURCE_CHANGED') from None
    if current != plan:
        raise RuntimeError('D2_SOURCE_CHANGED')


def _d2_outputs(items):
    return [dict(output_id=item['output_id'], group=item['group'],
        transfer_state='NOT_ATTEMPTED', commit_state='NOT_ATTEMPTED') for item in items]


def _d2_empty_worker(worker_id, items):
    return dict(worker_id=worker_id, required=True, started=False, pid=None,
        exitcode=None, settled=False, alive=False, timed_out=False,
        cleanup_confirmed=False, checkpoint_state='MISSING', execution_state='NOT_STARTED',
        normal_return_observed=False, diagnostics_state='NOT_OBSERVED',
        error_code=None, outputs=_d2_outputs(items))


def _d2_checkpoint(run_id, attempt_id, producer_sha, worker_id, items):
    return dict(kind='D2_WORKER_CHECKPOINT', receipt_version=1, run_id=run_id,
        attempt_id=attempt_id, producer_sha=producer_sha, worker_id=worker_id,
        pid=os.getpid(), sequence=0, execution_state='READY',
        normal_return_observed=False, diagnostics_state='ACTIVE',
        lock_state='NOT_USED', error_code=None, outputs=_d2_outputs(items))


def _d2_bytes(document):
    return (json.dumps(document, ensure_ascii=True, allow_nan=False,
        separators=(',', ':'), sort_keys=True) + '\n').encode('utf-8')


def _d2_run_inventory(root, limits):
    guard = sys.modules.get('mds_offline_guard')
    if guard is not None:
        guard.d2_admit_full_inventory(root)
    limits = _d2_validate_limits(limits)
    root = Path(root)
    try:
        _d2_checked_path(root, directory=True)
        total = 0
        for child in root.iterdir():
            if child.name not in {'google.json', 'google.pending', 'ftp.json', 'ftp.pending', 'result.json', 'result.pending'}:
                raise RuntimeError('D2_RESULT_WRITE_FAILED')
            info = child.lstat()
            if not _d2_plain(info):
                raise RuntimeError('D2_RESULT_WRITE_FAILED')
            total += info.st_size
            if total > limits['max_run_bytes']:
                raise RuntimeError('D2_RESULT_LIMIT')
        return total
    except (OSError, ValueError, RuntimeError) as error:
        if _d2_code(error) == 'D2_RESULT_LIMIT':
            raise RuntimeError('D2_RESULT_LIMIT') from None
        raise RuntimeError('D2_RESULT_WRITE_FAILED') from None


def _d2_atomic_json(root, name, document, limit, limits):
    if type(name) is not str or name not in {'google.json', 'ftp.json', 'result.json'}:
        raise RuntimeError('D2_RESULT_WRITE_FAILED')
    root = Path(root)
    _d2_checked_path(root, directory=True)
    data = _d2_bytes(document)
    if len(data) > limit:
        raise RuntimeError('D2_RESULT_LIMIT')
    path = root / name
    pending = root / (name.removesuffix('.json') + '.pending')
    try:
        # Только quiescent phase: ни один slot writer больше не публикует.
        total = _d2_run_inventory(root, limits)
        if total + len(data) > limits['max_run_bytes']:
            raise RuntimeError('D2_RESULT_LIMIT')
        if path.exists():
            _d2_checked_path(path)
        with pending.open('xb') as destination:
            if destination.write(data) != len(data):
                raise RuntimeError('D2_RESULT_WRITE_FAILED')
            destination.flush()
        os.replace(pending, path)
    except (OSError, RuntimeError) as error:
        if _d2_code(error) == 'D2_RESULT_LIMIT':
            raise RuntimeError('D2_RESULT_LIMIT') from None
        raise RuntimeError('D2_RESULT_WRITE_FAILED') from None


def _d2_slot_atomic_json(root, name, document, limit, limits, reservation):
    guard = sys.modules.get('mds_offline_guard')
    if guard is not None:
        guard.d2_admit_slot_write(root, name, document, limit, limits, reservation)
    if type(name) is not str or name not in {'google.json', 'ftp.json'}:
        raise RuntimeError('D2_CHECKPOINT_INVALID')
    limits = _d2_validate_limits(limits)
    proof = _d2_validate_reservation(reservation, limits)
    worker_id = name.removesuffix('.json')
    outputs = _d2_outputs(proof['plan'][worker_id])
    _d2_validate_checkpoint_record(document, worker_id, run_id=proof['run_id'],
        attempt_id=proof['attempt_id'], producer_sha=proof['producer_sha'],
        pid=os.getpid(), outputs=outputs)
    bound = proof['budget']['checkpoint_bytes'][0 if worker_id == 'google' else 1]
    if type(limit) is not int or limit != limits['max_checkpoint_bytes']:
        raise RuntimeError('D2_LIMITS_INVALID')
    try:
        data = _d2_bytes(document)
    except Exception:
        raise RuntimeError('D2_CHECKPOINT_INVALID') from None
    if len(data) > bound or len(data) > limit:
        raise RuntimeError('D2_RESULT_LIMIT')
    root = Path(root)
    path = root / name
    pending = root / (worker_id + '.pending')
    try:
        root_info = _d2_checked_path(root, directory=True)
        if (root_info.st_dev, root_info.st_ino) != proof['root_identity']:
            raise RuntimeError('D2_RESULT_WRITE_FAILED')
        # Имена разрешено перечислить; metadata соседних leaf не читаем.
        with os.scandir(root) as entries:
            for entry in entries:
                if entry.name not in {'google.json', 'google.pending', 'ftp.json', 'ftp.pending', 'result.json', 'result.pending'}:
                    raise RuntimeError('D2_RESULT_WRITE_FAILED')
        try:
            previous = path.lstat()
        except FileNotFoundError:
            previous = None
        if previous is not None:
            if not _d2_plain(previous):
                raise RuntimeError('D2_RESULT_WRITE_FAILED')
            if previous.st_size > bound:
                raise RuntimeError('D2_RESULT_LIMIT')
        try:
            pending.lstat()
        except FileNotFoundError:
            pass
        else:
            # Оставшийся pending не удаляется и не переиспользуется.
            raise RuntimeError('D2_RESULT_WRITE_FAILED')
        with pending.open('xb') as destination:
            info = os.fstat(destination.fileno())
            if not _d2_plain(info):
                raise RuntimeError('D2_RESULT_WRITE_FAILED')
            identity = (info.st_dev, info.st_ino)
            if destination.write(data) != len(data):
                raise RuntimeError('D2_RESULT_WRITE_FAILED')
            destination.flush()
            info = os.fstat(destination.fileno())
            if (not _d2_plain(info) or (info.st_dev, info.st_ino) != identity
                    or info.st_size != len(data)):
                raise RuntimeError('D2_RESULT_WRITE_FAILED')
        info = _d2_checked_path(pending)
        if (info.st_dev, info.st_ino) != identity or info.st_size != len(data):
            raise RuntimeError('D2_RESULT_WRITE_FAILED')
        os.replace(pending, path)
    except (OSError, ValueError, RuntimeError) as error:
        if _d2_code(error) == 'D2_RESULT_LIMIT':
            raise RuntimeError('D2_RESULT_LIMIT') from None
        raise RuntimeError('D2_RESULT_WRITE_FAILED') from None


def _d2_write_checkpoint(root, worker_id, document, limits=None, *, reservation=None):
    if type(worker_id) is not str or worker_id not in {'google', 'ftp'}:
        raise RuntimeError('D2_CHECKPOINT_INVALID')
    limits = _d2_validate_limits(limits)
    if reservation is None:
        # Direct offline schema/byte oracles имеют quiescent owned root.
        _d2_atomic_json(root, worker_id + '.json', document, limits['max_checkpoint_bytes'], limits)
    else:
        _d2_slot_atomic_json(root, worker_id + '.json', document,
            limits['max_checkpoint_bytes'], limits, reservation)


def _d2_checkpoint_reader_bindings(profile=None):
    global _D2_WINDOWS_READER_BINDINGS, _D2_WINDOWS_READER_MODULES
    try:
        if (sys.platform != 'win32' or sys.implementation.name != 'cpython'
                or sys.version_info[:3] != (3, 14, 8)
                or getattr(sys, 'frozen', False)
                or not hasattr(sys, '_is_gil_enabled') or not sys._is_gil_enabled()
                or (os.O_RDONLY, os.O_BINARY, os.O_NOINHERIT) != (0, 32768, 128)):
            raise RuntimeError('D2_CHECKPOINT_INVALID')
        if _D2_WINDOWS_READER_BINDINGS is None:
            import inspect
            if profile is None:
                # Direct read не требует admission Process.start/multiprocessing.
                import _winapi
                import msvcrt
                with open(sys._base_executable, 'rb') as executable_file:
                    digest = hashlib.file_digest(executable_file, 'sha256').hexdigest()
                if digest != _D2_WINDOWS_EXE_HASH:
                    raise RuntimeError('D2_CHECKPOINT_INVALID')
            else:
                if (type(profile) is not dict
                        or profile.get('profile_id') != 'windows-cpython-3.14.8-gil'
                        or profile.get('exe_sha256') != _D2_WINDOWS_EXE_HASH):
                    raise RuntimeError('D2_CHECKPOINT_INVALID')
                _winapi, msvcrt = profile['winapi'], profile['msvcrt']
            bindings = {'CreateFile': _winapi.CreateFile,
                'CloseHandle': _winapi.CloseHandle,
                'open_osfhandle': msvcrt.open_osfhandle}
            for name, binding in bindings.items():
                if (not isinstance(binding, types.BuiltinFunctionType)
                        or binding.__module__ != ('msvcrt' if name == 'open_osfhandle' else '_winapi')
                        or binding.__name__ != name):
                    raise RuntimeError('D2_CHECKPOINT_INVALID')
            if (str(inspect.signature(bindings['CreateFile'])) !=
                    '(file_name, desired_access, share_mode, security_attributes, creation_disposition, flags_and_attributes, template_file, /)'
                    or str(inspect.signature(bindings['open_osfhandle'])) != '(handle, flags, /)'):
                raise RuntimeError('D2_CHECKPOINT_INVALID')
            _D2_WINDOWS_READER_MODULES = (_winapi, msvcrt)
            _D2_WINDOWS_READER_BINDINGS = bindings
        _winapi, msvcrt = _D2_WINDOWS_READER_MODULES
        bindings = _D2_WINDOWS_READER_BINDINGS
        if (type(bindings) is not dict or set(bindings) != {'CreateFile', 'CloseHandle', 'open_osfhandle'}
                or _winapi.CreateFile is not bindings['CreateFile']
                or _winapi.CloseHandle is not bindings['CloseHandle']
                or msvcrt.open_osfhandle is not bindings['open_osfhandle']):
            raise RuntimeError('D2_CHECKPOINT_INVALID')
        return bindings
    except (OSError, ValueError, TypeError, AttributeError, KeyError, RuntimeError):
        raise RuntimeError('D2_CHECKPOINT_INVALID') from None


def _d2_read_json_bytes(path, limit):
    path = Path(os.path.abspath(path))
    if (path.name not in {'google.json', 'ftp.json', 'result.json'}
            or type(limit) is not int or limit <= 0
            or limit > _D2_LIMITS['max_result_bytes' if path.name == 'result.json' else 'max_checkpoint_bytes']):
        raise RuntimeError('D2_CHECKPOINT_INVALID')
    guard = sys.modules.get('mds_offline_guard')
    if guard is not None:
        guard.d2_admit_checkpoint_path(path)
    _d2_checked_path(path)
    bindings = _d2_checkpoint_reader_bindings()
    # FILE_SHARE_READ|WRITE|DELETE; OPEN_EXISTING; OPEN_REPARSE_POINT|NORMAL.
    # Null security/template pointers и O_NOINHERIT запрещают inheritance.
    parameters = (str(path), 0x80000000, 7, 0, 3, 0x00200080, 0)
    flags = os.O_RDONLY | os.O_BINARY | os.O_NOINHERIT
    guard = sys.modules.get('mds_offline_guard')
    token = handle = descriptor = stream = None
    try:
        if guard is not None:
            token = guard.d2_begin_checkpoint_read(path, limit, bindings)
            if token is None:
                raise RuntimeError('D2_CHECKPOINT_INVALID')
            handle = guard.d2_checkpoint_create_file(token, *parameters)
        else:
            handle = bindings['CreateFile'](*parameters)
        if type(handle) is not int or handle <= 0:
            raise RuntimeError('D2_CHECKPOINT_INVALID')
        if token is not None:
            descriptor = guard.d2_checkpoint_open_osfhandle(token, handle, flags)
        else:
            descriptor = bindings['open_osfhandle'](handle, flags)
        # С этого return HANDLE принадлежит только CRT descriptor.
        handle = None
        if type(descriptor) is not int or descriptor < 0 or os.get_inheritable(descriptor):
            raise RuntimeError('D2_CHECKPOINT_INVALID')
        with os.fdopen(descriptor, 'rb', closefd=False) as stream:
            data = stream.read(limit + 1)
        return data
    except Exception:
        raise RuntimeError('D2_CHECKPOINT_INVALID') from None
    finally:
        try:
            try:
                if stream is not None:
                    if not stream.closed:
                        stream.close()
                    if token is not None:
                        guard.d2_checkpoint_stream_closed(token, stream)
            finally:
                try:
                    if descriptor is not None:
                        os.close(descriptor)
                    elif handle is not None:
                        if token is not None:
                            guard.d2_checkpoint_close_handle(token, handle)
                        else:
                            bindings['CloseHandle'](handle)
                finally:
                    if token is not None:
                        guard.d2_end_checkpoint_read(token)
        except Exception:
            raise RuntimeError('D2_CHECKPOINT_INVALID') from None


def _d2_read_json(path, limit):
    try:
        def object_document(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise RuntimeError('D2_CHECKPOINT_INVALID')
                result[key] = value
            return result

        def reject_constant(value):
            raise RuntimeError('D2_CHECKPOINT_INVALID')

        data = _d2_read_json_bytes(path, limit)
        if len(data) > limit:
            raise RuntimeError('D2_CHECKPOINT_INVALID')
        record = json.loads(data.decode('utf-8'), object_pairs_hook=object_document,
            parse_constant=reject_constant)
        # Один canonical документ: duplicate keys, NaN и trailing data запрещены.
        if _d2_bytes(record) != data:
            raise RuntimeError('D2_CHECKPOINT_INVALID')
        return record
    except Exception:
        raise RuntimeError('D2_CHECKPOINT_INVALID') from None


def _d2_read_checkpoint(root, worker_id, *, run_id, attempt_id, pid, outputs,
                       minimum_sequence=0, producer_sha=None, limits=None):
    if type(worker_id) is not str or worker_id not in {'google', 'ftp'}:
        raise RuntimeError('D2_CHECKPOINT_INVALID')
    limits = _d2_validate_limits(limits)
    record = _d2_read_json(Path(root) / (worker_id + '.json'), limits['max_checkpoint_bytes'])
    return _d2_validate_checkpoint_record(record, worker_id, run_id=run_id,
        attempt_id=attempt_id, pid=pid, outputs=outputs,
        minimum_sequence=minimum_sequence, producer_sha=producer_sha)


def _d2_validate_checkpoint_record(record, worker_id, *, run_id, attempt_id,
        pid, outputs, minimum_sequence=0, producer_sha=None):
    keys = {'kind', 'receipt_version', 'run_id', 'attempt_id', 'producer_sha',
        'worker_id', 'pid', 'sequence', 'execution_state', 'normal_return_observed',
        'diagnostics_state', 'lock_state', 'error_code', 'outputs'}
    valid = (type(record) is dict and set(record) == keys and
        record['kind'] == 'D2_WORKER_CHECKPOINT' and type(record['receipt_version']) is int and
        record['receipt_version'] == 1 and record['run_id'] == run_id and
        record['attempt_id'] == attempt_id and record['worker_id'] == worker_id and
        type(record['pid']) is int and record['pid'] == pid and
        type(record['sequence']) is int and minimum_sequence <= record['sequence'] <= 2 * len(outputs) + 8 and
        record['sequence'] >= 1 and type(record['execution_state']) is str and
        record['execution_state'] in {'READY', 'RUNNING', 'RETURNED', 'FAILED', 'INTERRUPTED'} and
        type(record['normal_return_observed']) is bool and
        type(record['diagnostics_state']) is str and record['diagnostics_state'] in {'ACTIVE', 'COMPLETE', 'INCOMPLETE'} and
        record['lock_state'] == 'NOT_USED' and
        (record['error_code'] is None or type(record['error_code']) is str and record['error_code'] in _D2_CODES) and
        type(record['producer_sha']) is str and re.fullmatch('[0-9a-f]{7,40}', record['producer_sha']) and
        (producer_sha is None or record['producer_sha'] == producer_sha) and
        type(record['outputs']) is list and len(record['outputs']) == len(outputs))
    if not valid:
        raise RuntimeError('D2_CHECKPOINT_INVALID')
    for actual, expected in zip(record['outputs'], outputs):
        if (type(actual) is not dict or set(actual) != {'output_id', 'group', 'transfer_state', 'commit_state'} or
            actual['output_id'] != expected['output_id'] or actual['group'] != expected['group'] or
            type(actual['transfer_state']) is not str or actual['transfer_state'] not in {'NOT_ATTEMPTED', 'UNKNOWN', 'ACKNOWLEDGED'} or
            actual['commit_state'] != ('NOT_ATTEMPTED' if actual['transfer_state'] == 'NOT_ATTEMPTED' else 'UNKNOWN')):
            raise RuntimeError('D2_CHECKPOINT_INVALID')
    if record['normal_return_observed'] and record['execution_state'] not in {'RETURNED', 'FAILED', 'INTERRUPTED'}:
        raise RuntimeError('D2_CHECKPOINT_INVALID')
    return record


def _d2_result(run_id, attempt_id, producer_sha, plan):
    return dict(kind='D2_RESULT', receipt_version=1, run_id=run_id, attempt_id=attempt_id,
        producer_sha=producer_sha, artifact_id='d2-results-' + attempt_id,
        receipt_scope='PREFINAL_SUPERVISION', finalization_state='NOT_OBSERVED',
        diagnostic_success_allowed=False, diagnostics_state='NOT_OBSERVED',
        final_exit_status=None, storage_sealed='UNKNOWN', parent_outcome='UNKNOWN',
        error_code=None, failed_required_channels=[],
        workers={worker: _d2_empty_worker(worker, plan.get(worker, [])) for worker in ('google', 'ftp')},
        committed_outputs=[], uncommitted_outputs=[], unknown_outputs=[])


def _d2_aggregate(result):
    result['failed_required_channels'] = []
    result['uncommitted_outputs'], result['unknown_outputs'] = [], []
    for worker, row in result['workers'].items():
        if not _d2_worker_ok(row):
            result['failed_required_channels'].append(worker)
        for output in row['outputs']:
            key = 'uncommitted_outputs' if output['transfer_state'] == 'NOT_ATTEMPTED' else 'unknown_outputs'
            result[key].append(output['output_id'])


def _d2_worker_ok(row):
    return (type(row) is dict and row.get('required') is True and
        type(row.get('pid')) is int and row['pid'] > 0 and
        row.get('started') is True and row.get('settled') is True and
        row.get('alive') is False and type(row.get('exitcode')) is int and row['exitcode'] == 0 and
        row.get('timed_out') is False and row.get('cleanup_confirmed') is True and
        row.get('checkpoint_state') == 'VALID' and row.get('execution_state') == 'RETURNED' and
        row.get('normal_return_observed') is True and row.get('diagnostics_state') == 'COMPLETE' and
        row.get('error_code') is None and type(row.get('outputs')) is list and
        all(type(output) is dict and output.get('transfer_state') == 'ACKNOWLEDGED'
            for output in row['outputs']))


def _d2_workers_ok(result):
    workers = result.get('workers')
    return (type(workers) is dict and set(workers) == {'google', 'ftp'} and
        all(_d2_worker_ok(row) and row.get('worker_id') == worker for worker, row in workers.items()) and
        workers['google']['pid'] != workers['ftp']['pid'])


def _d2_apply_finalization(result_sink, report, *, status=0):
    if type(result_sink) is not dict or not result_sink:
        return status if type(status) is int and status else 1
    same_report = (type(report) is dict and type(result_sink.get('run_id')) is str and
        report.get('run_id') == result_sink.get('run_id') and
        ('producer_sha' not in report or report['producer_sha'] == result_sink.get('producer_sha')))
    complete = (same_report and type(report.get('exit_status')) is int and
        _d2_workers_ok(result_sink) and
        report.get('exit_status') == 0 and report.get('delivery_outcome') == 'COMPLETE' and
        report.get('storage_sealed') is True and report.get('evidence_incomplete') is False and
        report.get('execution_outcome') == 'COMPLETED' and type(status) is int and status == 0 and
        result_sink.get('parent_outcome') == 'RETURNED')
    report_status = report.get('exit_status') if same_report else None
    final_status = status if type(status) is int and status else report_status if type(report_status) is int and report_status else 0 if complete else 1
    result_sink.update(receipt_scope='FINAL_IN_MEMORY',
        finalization_state='OBSERVED_COMPLETE' if complete else 'OBSERVED_INCOMPLETE',
        diagnostic_success_allowed=complete, diagnostics_state='COMPLETE' if complete else 'INCOMPLETE',
        final_exit_status=final_status,
        storage_sealed=report['storage_sealed'] if same_report and type(report.get('storage_sealed')) is bool else 'UNKNOWN')
    return final_status


def _d2_completion_allowed(result_sink):
    return (type(result_sink) is dict and result_sink.get('diagnostic_success_allowed') is True and
        result_sink.get('finalization_state') == 'OBSERVED_COMPLETE' and
        type(result_sink.get('final_exit_status')) is int and result_sink['final_exit_status'] == 0 and
        result_sink.get('storage_sealed') is True and result_sink.get('parent_outcome') == 'RETURNED' and
        _d2_workers_ok(result_sink))


def _d2_error(code, result):
    error = RuntimeError(code)
    error.d2_result = _d2_clone(result)
    error._d2_result = error.d2_result
    return error


def _d2_validate_budget(plan, limits, run_id, attempt_id, producer_sha):
    limits = _d2_validate_limits(limits)
    if len(plan['ftp']) > limits['max_ftp_outputs']:
        raise RuntimeError('D2_OUTPUT_LIMIT')
    sizes = []
    for worker_id in ('google', 'ftp'):
        record = _d2_checkpoint(run_id, attempt_id, producer_sha, worker_id, plan[worker_id])
        record.update(pid=4294967295, sequence=2 * len(plan[worker_id]) + 8,
            execution_state='INTERRUPTED', normal_return_observed=False,
            diagnostics_state='INCOMPLETE', error_code=max(_D2_CODES, key=len))
        # NOT_ATTEMPTED в обоих полях длиннее остальных допустимых переходов.
        size = len(_d2_bytes(record))
        if size > limits['max_checkpoint_bytes']:
            raise RuntimeError('D2_RESULT_LIMIT')
        sizes.append(size)
    result = _d2_result(run_id, attempt_id, producer_sha, plan)
    result.update(parent_outcome='INTERRUPTED', error_code=max(_D2_CODES, key=len))
    for row in result['workers'].values():
        row.update(started=False, pid=4294967295, exitcode=-2147483648,
            checkpoint_state='INVALID', execution_state='INTERRUPTED',
            diagnostics_state='NOT_OBSERVED', error_code=max(_D2_CODES, key=len))
    _d2_aggregate(result)
    result['unknown_outputs'] = [output['output_id'] for row in result['workers'].values()
        for output in row['outputs']]
    result['uncommitted_outputs'] = []
    size = len(_d2_bytes(result))
    if size > limits['max_result_bytes'] or 2 * (sum(sizes) + size) > limits['max_run_bytes']:
        raise RuntimeError('D2_RESULT_LIMIT')
    return dict(checkpoint_bytes=sizes, result_bytes=size,
        peak_run_bytes=2 * (sum(sizes) + size))


def _d2_make_reservation(plan, limits, run_id, attempt_id, producer_sha, root_identity):
    limits = _d2_validate_limits(limits)
    try:
        google = tuple((item['output_id'], item['group']) for item in plan['google'])
        ftp = tuple((item['output_id'], item['group']) for item in plan['ftp'])
        budget = _d2_validate_budget(plan, limits, run_id, attempt_id, producer_sha)
        bounds = tuple(budget['checkpoint_bytes']) + (budget['result_bytes'], budget['peak_run_bytes'])
        reservation = (run_id, attempt_id, producer_sha, google, ftp,
            tuple(sorted(limits.items())), bounds, tuple(root_identity))
        _d2_validate_reservation(reservation, limits)
        return reservation
    except (KeyError, TypeError, ValueError, RuntimeError) as error:
        if _d2_code(error) in {'D2_RESULT_LIMIT', 'D2_OUTPUT_LIMIT'}:
            raise RuntimeError(_d2_code(error)) from None
        raise RuntimeError('D2_CHECKPOINT_INVALID') from None


def _d2_validate_reservation(reservation, limits):
    limits = _d2_validate_limits(limits)
    try:
        if type(reservation) is not tuple or len(reservation) != 8:
            raise RuntimeError('D2_CHECKPOINT_INVALID')
        run_id, attempt_id, producer_sha, google, ftp, limit_pairs, bounds, root_identity = reservation
        if (type(run_id) is not str or re.fullmatch('[A-Za-z0-9][A-Za-z0-9_.-]{0,79}', run_id) is None
                or type(attempt_id) is not str or re.fullmatch('[0-9a-f]{32}', attempt_id) is None
                or type(producer_sha) is not str or re.fullmatch('[0-9a-f]{7,40}', producer_sha) is None
                or type(google) is not tuple or len(google) != 1
                or type(ftp) is not tuple or len(ftp) > limits['max_ftp_outputs']
                or type(limit_pairs) is not tuple or len(limit_pairs) != len(limits)
                or type(bounds) is not tuple or len(bounds) != 4
                or any(type(value) is not int or value <= 0 for value in bounds)
                or type(root_identity) is not tuple or len(root_identity) != 2
                or any(type(value) is not int or value < 0 for value in root_identity)
                or root_identity[1] == 0):
            raise RuntimeError('D2_CHECKPOINT_INVALID')
        for pair in limit_pairs:
            if (type(pair) is not tuple or len(pair) != 2 or type(pair[0]) is not str
                    or type(pair[1]) is not int):
                raise RuntimeError('D2_CHECKPOINT_INVALID')
        if limit_pairs != tuple(sorted(limits.items())):
            raise RuntimeError('D2_CHECKPOINT_INVALID')
        plan = dict(google=[], ftp=[])
        for worker_id, tokens in (('google', google), ('ftp', ftp)):
            for index, token in enumerate(tokens):
                if (type(token) is not tuple or len(token) != 2
                        or type(token[0]) is not str or type(token[1]) is not str):
                    raise RuntimeError('D2_CHECKPOINT_INVALID')
                expected_id = worker_id + '-' + str(index + 1).zfill(6)
                groups = {'google'} if worker_id == 'google' else {'root', 'dividends', 'ticker_lists'}
                if token[0] != expected_id or token[1] not in groups:
                    raise RuntimeError('D2_CHECKPOINT_INVALID')
                plan[worker_id].append(dict(output_id=token[0], group=token[1]))
        budget = _d2_validate_budget(plan, limits, run_id, attempt_id, producer_sha)
        expected_bounds = tuple(budget['checkpoint_bytes']) + (budget['result_bytes'], budget['peak_run_bytes'])
        if bounds != expected_bounds:
            raise RuntimeError('D2_CHECKPOINT_INVALID')
        return dict(run_id=run_id, attempt_id=attempt_id, producer_sha=producer_sha,
            root_identity=root_identity, plan=plan, budget=budget)
    except (KeyError, TypeError, ValueError, RuntimeError) as error:
        if _d2_code(error) in {'D2_RESULT_LIMIT', 'D2_OUTPUT_LIMIT'}:
            raise RuntimeError(_d2_code(error)) from None
        raise RuntimeError('D2_CHECKPOINT_INVALID') from None


def _d2_before_start(process, worker_id, fixture_case):
    if fixture_case is not None:
        guard = sys.modules.get('mds_offline_guard')
        hook = getattr(guard, '_d2_prepare_upload_start', None)
        if hook is None:
            raise RuntimeError('D2_FIXTURE_REFUSED')
        hook(process, worker_id, fixture_case)


def _d2_after_start(process, state, worker_id, fixture_case):
    if fixture_case is not None:
        guard = sys.modules.get('mds_offline_guard')
        hook = getattr(guard, '_d2_complete_upload_start', None)
        if hook is None:
            raise RuntimeError('D2_FIXTURE_REFUSED')
        hook(process, state, worker_id, fixture_case)


def _d2_worker_entry(bootstrap, worker_id, channel_plan, result_root, attempt_id,
        limits, current_path, gdoc_to_write, target, fixture_case, go, reservation):
    producer = None
    record = None
    status, outcome, code, normal = 1, 'FAILED', None, False
    ready_publication_started, go_observed = False, False
    try:
        if fixture_case is not None:
            guard = sys.modules.get('mds_offline_guard')
            context = getattr(guard, 'd2_worker_context', None)
            if context is None:
                raise RuntimeError('D2_FIXTURE_REFUSED')
            context(fixture_case, worker_id)
        proof = _d2_validate_reservation(reservation, limits)
        tokens = [dict(output_id=item['output_id'], group=item['group']) for item in channel_plan]
        if (proof['run_id'] != bootstrap['run_id'] or proof['attempt_id'] != attempt_id
                or proof['producer_sha'] != bootstrap['producer_sha']
                or worker_id not in {'google', 'ftp'} or tokens != proof['plan'][worker_id]):
            raise RuntimeError('D2_CHECKPOINT_INVALID')
        import run_logging
        producer = run_logging.configure_worker(bootstrap)
        record = _d2_checkpoint(bootstrap['run_id'], attempt_id,
            bootstrap['producer_sha'], worker_id, channel_plan)

        def checkpoint():
            record['sequence'] += 1
            _d2_write_checkpoint(result_root, worker_id, record, limits,
                reservation=reservation)

        def observer(output_id, state):
            if state not in {'UNKNOWN', 'ACKNOWLEDGED'}:
                raise RuntimeError('D2_CHECKPOINT_INVALID')
            matches = [output for output in record['outputs'] if output['output_id'] == output_id]
            if len(matches) != 1:
                raise RuntimeError('D2_CHECKPOINT_INVALID')
            output = matches[0]
            previous = output['transfer_state']
            if (state == 'UNKNOWN' and previous != 'NOT_ATTEMPTED'
                    or state == 'ACKNOWLEDGED' and previous != 'UNKNOWN'):
                raise RuntimeError('D2_CHECKPOINT_INVALID')
            if not run_logging.flush_logging(producer).get('confirmed'):
                raise RuntimeError('D2_DIAGNOSTICS_INCOMPLETE')
            output.update(transfer_state=state, commit_state='UNKNOWN')
            checkpoint()

        run_logging.emit_event(producer, 'INFO', 'd2_worker_ready',
            'Upload worker ready', {'stage': 'upload', 'outcome': 'READY'})
        if not run_logging.flush_logging(producer).get('confirmed'):
            raise RuntimeError('D2_DIAGNOSTICS_INCOMPLETE')
        # Даже exception после replace не должен переписать опубликованный READY.
        ready_publication_started = True
        checkpoint()
        while go.value != 1:
            parent = multiprocessing.parent_process()
            if parent is not None and not parent.is_alive():
                raise RuntimeError('D2_INTERRUPTED')
            time.sleep(0.1)
        go_observed = True
        record['execution_state'] = 'RUNNING'
        checkpoint()
        if worker_id == 'google':
            target(current_path, gdoc_to_write, source_plan=channel_plan, output_observer=observer)
        else:
            target(current_path, source_plan=channel_plan, output_observer=observer)
        normal = True
        if not all(output['transfer_state'] == 'ACKNOWLEDGED' for output in record['outputs']):
            raise RuntimeError('D2_CHILD_FAILED')
        status, outcome = 0, 'COMPLETED'
    except BaseException as error:
        code = 'D2_INTERRUPTED' if isinstance(error, (KeyboardInterrupt, SystemExit)) else _d2_code(error)
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            outcome = 'INTERRUPTED'
            status = 130 if isinstance(error, KeyboardInterrupt) else 1
    finally:
        confirmed = False
        if producer is not None:
            try:
                report = run_logging.finish_worker(producer,
                    execution_outcome=outcome, exit_status=status)
                confirmed = report.get('confirmed') is True
            except BaseException:
                confirmed = False
        if not confirmed:
            status = status or 1
            code = code or 'D2_DIAGNOSTICS_INCOMPLETE'
        # До GO READY остаётся immutable; real F-LOG final/status всё равно emitted.
        if record is not None and (not ready_publication_started or go_observed):
            record.update(execution_state='RETURNED' if outcome == 'COMPLETED' else outcome,
                normal_return_observed=normal,
                diagnostics_state='COMPLETE' if confirmed else 'INCOMPLETE', error_code=code)
            try:
                checkpoint()
            except BaseException:
                status = status or 1
        # Не оставляем Exception/traceback или произвольный SystemExit текст в console.
    raise SystemExit(status)


def _d2_observe_row(row, state):
    observation = _d2_native_observation(state)
    for key in ('started', 'pid', 'settled', 'exitcode', 'alive'):
        row[key] = observation[key]
    if observation['error_code'] is not None:
        row['error_code'] = (observation['error_code'] if type(observation['error_code']) is str
            and observation['error_code'] in _D2_CODES else 'D2_START_FAILED')
    return observation


def _d2_accept_checkpoint(row, root, run_id, attempt_id, producer_sha, limits,
        minimum_sequence, previous_record=None):
    record = _d2_read_checkpoint(root, row['worker_id'], run_id=run_id,
        attempt_id=attempt_id, producer_sha=producer_sha, pid=row['pid'],
        outputs=row['outputs'], minimum_sequence=minimum_sequence, limits=limits)
    progress = {'NOT_ATTEMPTED': 0, 'UNKNOWN': 1, 'ACKNOWLEDGED': 2}
    transitions = sum(progress[output['transfer_state']] for output in record['outputs'])
    state = record['execution_state']
    if state in {'READY', 'RUNNING'}:
        expected = 1 if state == 'READY' else 2 + transitions
        if (record['sequence'] != expected or state == 'READY' and transitions != 0
                or record['normal_return_observed'] or record['diagnostics_state'] != 'ACTIVE'
                or record['error_code'] is not None):
            raise RuntimeError('D2_CHECKPOINT_INVALID')
    else:
        # До READY/GO возможны final 1/2; после RUNNING каждый observer даёт +1.
        expected = {3 + transitions} if record['normal_return_observed'] or transitions else {1, 2, 3}
        if (record['sequence'] not in expected or record['diagnostics_state'] == 'ACTIVE'
                or state == 'RETURNED' and (not record['normal_return_observed']
                    or transitions != 2 * len(record['outputs']))):
            raise RuntimeError('D2_CHECKPOINT_INVALID')
    if previous_record is not None:
        if (type(previous_record) is not dict or set(previous_record) != set(record)
                or type(previous_record['sequence']) is not int
                or type(previous_record['execution_state']) is not str
                or previous_record['execution_state'] not in {'READY', 'RUNNING', 'RETURNED', 'FAILED', 'INTERRUPTED'}
                or type(previous_record['outputs']) is not list
                or len(previous_record['outputs']) != len(record['outputs'])
                or record['sequence'] < previous_record['sequence']):
            raise RuntimeError('D2_CHECKPOINT_INVALID')
        for key in ('kind', 'receipt_version', 'run_id', 'attempt_id', 'producer_sha', 'worker_id', 'pid', 'lock_state'):
            if record[key] != previous_record[key]:
                raise RuntimeError('D2_CHECKPOINT_INVALID')
        for actual, previous in zip(record['outputs'], previous_record['outputs']):
            if (type(previous) is not dict or set(previous) != set(actual)
                    or previous['output_id'] != actual['output_id'] or previous['group'] != actual['group']
                    or type(previous['transfer_state']) is not str or previous['transfer_state'] not in progress):
                raise RuntimeError('D2_CHECKPOINT_INVALID')
        if record['sequence'] == previous_record['sequence']:
            if record != previous_record:
                raise RuntimeError('D2_CHECKPOINT_INVALID')
        else:
            previous_state = previous_record['execution_state']
            if (previous_state in {'RETURNED', 'FAILED', 'INTERRUPTED'} or state == 'READY'
                    or previous_state == 'RUNNING' and state not in {'RUNNING', 'RETURNED', 'FAILED', 'INTERRUPTED'}):
                raise RuntimeError('D2_CHECKPOINT_INVALID')
            for actual, previous in zip(record['outputs'], previous_record['outputs']):
                if progress[actual['transfer_state']] < progress[previous['transfer_state']]:
                    raise RuntimeError('D2_CHECKPOINT_INVALID')
            if previous_state == 'RUNNING' and state != 'RUNNING' and record['sequence'] != 3 + transitions:
                raise RuntimeError('D2_CHECKPOINT_INVALID')
    row.update(checkpoint_state='VALID', execution_state=record['execution_state'],
        normal_return_observed=record['normal_return_observed'],
        diagnostics_state=record['diagnostics_state'], outputs=record['outputs'])
    row['error_code'] = row['error_code'] or record['error_code']
    return record


def _d2_unknown_outputs(row):
    # Без валидного checkpoint нельзя доказать, какой вызов remote client начался.
    for output in row['outputs']:
        if output['transfer_state'] != 'ACKNOWLEDGED':
            output.update(transfer_state='UNKNOWN', commit_state='UNKNOWN')


def _d2_supervise(logging_context, source_plan, result_root, run_id, attempt_id,
        limits, current_path, gdoc_to_write, *, worker_targets=None, fixture_case=None):
    import run_logging
    limits = _d2_validate_limits(limits)
    profile = _d2_runtime_profile(fixture_case)
    result = _d2_result(run_id, attempt_id, logging_context['producer_sha'], source_plan)
    states = {worker: {} for worker in ('google', 'ftp')}
    sequences = dict(google=0, ftp=0)
    checkpoints = dict(google=None, ftp=None)
    registered, processes = [], {}
    parent_code, interrupted = None, False
    released = False
    deadline = None
    reservation = None
    terminal_seen = set()
    try:
        if run_id != logging_context['run_id']:
            raise RuntimeError('D2_DIAGNOSTICS_INCOMPLETE')
        for name in ('shutdown_deadline_s', 'critical_deadline_s'):
            value = logging_context['config'].get(name)
            if type(value) not in (int, float) or not 0 < value <= profile['logging_seconds']:
                raise RuntimeError('D2_DIAGNOSTICS_INCOMPLETE')
        root_info = _d2_checked_path(result_root, directory=True)
        if any(Path(result_root).iterdir()):
            raise RuntimeError('D2_RESULT_WRITE_FAILED')
        reservation = _d2_make_reservation(source_plan, limits, run_id, attempt_id,
            result['producer_sha'], (root_info.st_dev, root_info.st_ino))
        _d2_checkpoint_reader_bindings(_d2_windows_profile())
        targets = dict(google=gdoc_upload, ftp=ftp_upload)
        if worker_targets is not None:
            if fixture_case is None or type(worker_targets) is not dict or set(worker_targets) != {'google', 'ftp'}:
                raise RuntimeError('D2_FIXTURE_REFUSED')
            targets = worker_targets
        spawn = multiprocessing.get_context('spawn')
        go = spawn.RawValue('b', 0)
        for worker_id in ('google', 'ftp'):
            bootstrap = run_logging.prepare_worker(logging_context, worker_id)
            registered.append(worker_id)
            processes[worker_id] = spawn.Process(target=_d2_worker_entry,
                args=(bootstrap, worker_id, source_plan[worker_id], str(result_root),
                    attempt_id, limits, current_path, gdoc_to_write,
                    targets[worker_id], fixture_case, go, reservation), name='D2-' + worker_id)
        run_logging.emit_event(logging_context, 'INFO', 'd2_upload_prepared',
            'Upload sources and diagnostics prepared', {'stage': 'upload', 'outcome': 'READY',
                'artifact_id': result['artifact_id']})
        if (not run_logging.flush_logging(logging_context).get('confirmed')
                or not run_logging.check_logging_health(logging_context).get('healthy')):
            raise RuntimeError('D2_DIAGNOSTICS_INCOMPLETE')
        for worker_id in ('google', 'ftp'):
            _d2_recheck_source_plan(source_plan)
            if not run_logging.check_logging_health(logging_context).get('healthy'):
                raise RuntimeError('D2_DIAGNOSTICS_INCOMPLETE')
            process, state = processes[worker_id], states[worker_id]
            try:
                if deadline is None:
                    # Approved clock: preflight/registration/initial log flush завершены.
                    deadline = time.monotonic() + profile['work_seconds']
                _d2_before_start(process, worker_id, fixture_case)
                started = _d2_start_worker(process, state)
            finally:
                _d2_after_start(process, state, worker_id, fixture_case)
            observation = _d2_observe_row(result['workers'][worker_id], state)
            if not started:
                raise RuntimeError(observation['error_code'] or 'D2_START_FAILED')
        ready = set()
        while len(ready) != 2:
            if time.monotonic() >= deadline:
                raise RuntimeError('D2_TIMEOUT')
            if not run_logging.check_logging_health(logging_context).get('healthy'):
                raise RuntimeError('D2_DIAGNOSTICS_INCOMPLETE')
            for worker_id, row in result['workers'].items():
                observation = _d2_observe_row(row, states[worker_id])
                if observation['error_code']:
                    raise RuntimeError(observation['error_code'])
                if observation['settled']:
                    raise RuntimeError('D2_CHILD_EXIT')
                if worker_id in ready:
                    continue
                path = Path(result_root) / (worker_id + '.json')
                if path.exists():
                    record = _d2_accept_checkpoint(row, result_root, run_id, attempt_id,
                        result['producer_sha'], limits, sequences[worker_id], checkpoints[worker_id])
                    sequences[worker_id] = record['sequence']
                    checkpoints[worker_id] = _d2_clone(record)
                    if (record['execution_state'] != 'READY' or record['normal_return_observed']
                            or record['diagnostics_state'] != 'ACTIVE' or record['error_code'] is not None
                            or any(output['transfer_state'] != 'NOT_ATTEMPTED' for output in record['outputs'])):
                        raise RuntimeError('D2_CHILD_FAILED')
                    ready.add(worker_id)
                elif sequences[worker_id] > 0:
                    raise RuntimeError('D2_CHECKPOINT_INVALID')
            time.sleep(profile['poll_seconds'])
        # Оба READY immutable до GO, и все их reader handles уже закрыты.
        _d2_run_inventory(result_root, limits)
        if {child.name for child in Path(result_root).iterdir()} != {'google.json', 'ftp.json'}:
            raise RuntimeError('D2_RESULT_WRITE_FAILED')
        root_info = _d2_checked_path(result_root, directory=True)
        if (root_info.st_dev, root_info.st_ino) != reservation[7]:
            raise RuntimeError('D2_RESULT_WRITE_FAILED')
        _d2_recheck_source_plan(source_plan)
        if (not run_logging.flush_logging(logging_context).get('confirmed')
                or not run_logging.check_logging_health(logging_context).get('healthy')):
            raise RuntimeError('D2_DIAGNOSTICS_INCOMPLETE')
        if time.monotonic() >= deadline:
            raise RuntimeError('D2_TIMEOUT')
        # Cancellation после видимого GO должна сохранять UNKNOWN без final evidence.
        released = True
        go.value = 1
        while True:
            if not run_logging.check_logging_health(logging_context).get('healthy'):
                raise RuntimeError('D2_DIAGNOSTICS_INCOMPLETE')
            all_settled = True
            for worker_id, row in result['workers'].items():
                observed = _d2_observe_row(row, states[worker_id])
                all_settled = all_settled and observed['settled']
                if observed['error_code']:
                    raise RuntimeError(observed['error_code'])
                # После GO active slot не читается; exit подтверждён native witness.
                if not observed['settled']:
                    continue
                if worker_id not in terminal_seen:
                    record = _d2_accept_checkpoint(row, result_root, run_id, attempt_id,
                        result['producer_sha'], limits, sequences[worker_id], checkpoints[worker_id])
                    sequences[worker_id] = record['sequence']
                    checkpoints[worker_id] = _d2_clone(record)
                    terminal_seen.add(worker_id)
                    if record['error_code'] is not None or record['execution_state'] in {'FAILED', 'INTERRUPTED'}:
                        raise RuntimeError(record['error_code'] or 'D2_CHILD_FAILED')
                if observed['settled'] and (observed['exitcode'] != 0
                        or row['execution_state'] != 'RETURNED' or not row['normal_return_observed']
                        or row['diagnostics_state'] != 'COMPLETE'):
                    raise RuntimeError('D2_CHILD_EXIT')
            if time.monotonic() >= deadline:
                raise RuntimeError('D2_TIMEOUT')
            if all_settled:
                break
            time.sleep(profile['poll_seconds'])
    except BaseException as error:
        interrupted = isinstance(error, (KeyboardInterrupt, SystemExit))
        parent_code = 'D2_INTERRUPTED' if interrupted else _d2_code(error)
    finally:
        cleanup_started = time.monotonic()
        cleanup_deadline = cleanup_started + profile['cleanup_seconds']
        terminate_deadline = cleanup_started + profile['cleanup_seconds'] / 2
        for worker_id, row in result['workers'].items():
            observed = _d2_observe_row(row, states[worker_id])
            if parent_code == 'D2_TIMEOUT' and observed['started'] and not observed['settled']:
                row['timed_out'] = True
            if observed['started'] and not observed['settled']:
                states[worker_id]['_cleanup_phase'] = 'TERMINATE'
                _d2_native_terminate(states[worker_id])
        while time.monotonic() < terminate_deadline:
            if all(_d2_observe_row(result['workers'][worker], state)['settled']
                    for worker, state in states.items()):
                break
            time.sleep(profile['poll_seconds'])
        for worker_id, row in result['workers'].items():
            observed = _d2_observe_row(row, states[worker_id])
            if observed['started'] and not observed['settled']:
                # Windows kill использует тот же kernel API; это второй запрос.
                states[worker_id]['_cleanup_phase'] = 'KILL'
                _d2_native_terminate(states[worker_id])
        while time.monotonic() < cleanup_deadline:
            if all(_d2_observe_row(result['workers'][worker], state)['settled']
                    for worker, state in states.items()):
                break
            time.sleep(profile['poll_seconds'])
        all_started_settled = True
        for worker_id, row in result['workers'].items():
            observed = _d2_observe_row(row, states[worker_id])
            all_started_settled = all_started_settled and observed['settled']
            if observed['settled']:
                row['cleanup_confirmed'] = _d2_close_launch_state(states[worker_id])
                _d2_observe_row(row, states[worker_id])
                if states[worker_id].get('_start_returned'):
                    try:
                        processes[worker_id].join(0)
                    except BaseException:
                        row['cleanup_confirmed'] = False
                        row['error_code'] = row['error_code'] or 'D2_CLEANUP_UNSETTLED'
            if not observed['settled'] or not row['cleanup_confirmed']:
                parent_code = parent_code or 'D2_CLEANUP_UNSETTLED'
                row['error_code'] = row['error_code'] or 'D2_CLEANUP_UNSETTLED'
            if row['started'] and observed['settled']:
                try:
                    record = _d2_accept_checkpoint(row, result_root, run_id, attempt_id,
                        result['producer_sha'], limits, sequences[worker_id], checkpoints[worker_id])
                    sequences[worker_id] = record['sequence']
                    checkpoints[worker_id] = _d2_clone(record)
                    if record['execution_state'] not in {'RETURNED', 'FAILED', 'INTERRUPTED'}:
                        row['error_code'] = row['error_code'] or 'D2_CHILD_EXIT'
                        if released:
                            _d2_unknown_outputs(row)
                except BaseException:
                    row['checkpoint_state'] = 'INVALID' if (Path(result_root) / (worker_id + '.json')).exists() else 'MISSING'
                    row['error_code'] = row['error_code'] or 'D2_CHECKPOINT_INVALID'
                    if released:
                        _d2_unknown_outputs(row)
            elif row['started'] and released:
                # Нельзя читать живой slot даже после исчерпания cleanup budget.
                _d2_unknown_outputs(row)
            if worker_id in registered:
                try:
                    run_logging.record_worker_outcome(logging_context, worker_id,
                        started=row['started'], exit_status=row['exitcode'], timed_out=row['timed_out'])
                except BaseException:
                    parent_code = parent_code or 'D2_DIAGNOSTICS_INCOMPLETE'
        _d2_aggregate(result)
        if result['failed_required_channels']:
            parent_code = parent_code or 'D2_CHILD_FAILED'
        try:
            if not run_logging.check_logging_health(logging_context).get('healthy'):
                parent_code = parent_code or 'D2_DIAGNOSTICS_INCOMPLETE'
        except BaseException:
            parent_code = parent_code or 'D2_DIAGNOSTICS_INCOMPLETE'
        persist_allowed = all_started_settled and reservation is not None
        if persist_allowed:
            try:
                root_info = _d2_checked_path(result_root, directory=True)
                if (root_info.st_dev, root_info.st_ino) != reservation[7]:
                    raise RuntimeError('D2_RESULT_WRITE_FAILED')
                _d2_run_inventory(result_root, limits)
                names = {child.name for child in Path(result_root).iterdir()}
                if names & {'google.pending', 'ftp.pending', 'result.pending'}:
                    parent_code = parent_code or 'D2_RESULT_WRITE_FAILED'
                if names & {'result.json', 'result.pending'}:
                    persist_allowed = False
                    parent_code = parent_code or 'D2_RESULT_WRITE_FAILED'
                if parent_code is None and names != {'google.json', 'ftp.json'}:
                    parent_code = 'D2_RESULT_WRITE_FAILED'
            except BaseException as error:
                persist_allowed = False
                parent_code = parent_code or _d2_code(error, 'D2_RESULT_WRITE_FAILED')
        result.update(parent_outcome='INTERRUPTED' if interrupted else 'FAILED' if parent_code else 'RETURNED',
            error_code=parent_code)
        try:
            if persist_allowed:
                proof = _d2_validate_reservation(reservation, limits)
                if len(_d2_bytes(result)) > proof['budget']['result_bytes']:
                    raise RuntimeError('D2_RESULT_LIMIT')
                _d2_atomic_json(result_root, 'result.json', result, limits['max_result_bytes'], limits)
                _d2_run_inventory(result_root, limits)
                names = {child.name for child in Path(result_root).iterdir()}
                if ('result.pending' in names or parent_code is None
                        and names != {'google.json', 'ftp.json', 'result.json'}):
                    raise RuntimeError('D2_RESULT_WRITE_FAILED')
            run_logging.emit_event(logging_context, 'ERROR' if parent_code else 'INFO', 'd2_upload_result',
                'Upload supervision failed' if parent_code else 'Upload supervision returned',
                {'stage': 'upload', 'outcome': result['parent_outcome'], 'artifact_id': result['artifact_id']})
            if not run_logging.flush_logging(logging_context).get('confirmed'):
                raise RuntimeError('D2_DIAGNOSTICS_INCOMPLETE')
        except BaseException as error:
            result.update(parent_outcome='FAILED', error_code=parent_code or _d2_code(error, 'D2_DIAGNOSTICS_INCOMPLETE'))
    return result


def _d2_source_sha(root):
    # Standalone upload читает только Git metadata, без import main/subprocess.
    git = root / '.git'
    if git.is_file():
        git = (root / git.read_text(encoding='utf-8').strip().split(': ', 1)[1]).resolve()
    head = (git / 'HEAD').read_text(encoding='utf-8').strip()
    if not head.startswith('ref: '):
        return head
    ref, common = head[5:], git
    if (git / 'commondir').exists():
        common = (git / (git / 'commondir').read_text(encoding='utf-8').strip()).resolve()
    if (common / ref).exists():
        return (common / ref).read_text(encoding='utf-8').strip()
    for line in (common / 'packed-refs').read_text(encoding='utf-8').splitlines():
        if line.endswith(' ' + ref):
            return line.split(' ', 1)[0]
    raise RuntimeError('D2_DIAGNOSTICS_INCOMPLETE')


def _d2_result_root(root, run_id, attempt_id):
    if (type(run_id) is not str or re.fullmatch('[A-Za-z0-9][A-Za-z0-9_.-]{0,79}', run_id) is None
            or type(attempt_id) is not str or re.fullmatch('[0-9a-f]{32}', attempt_id) is None):
        raise RuntimeError('D2_DIAGNOSTICS_INCOMPLETE')
    parent = root / '.f-log' / 'd2-results'
    parent.mkdir(parents=True, exist_ok=True)
    _d2_checked_path(parent, directory=True)
    result = parent / (run_id + '-' + attempt_id)
    result.mkdir()
    _d2_checked_path(result, directory=True)
    return result


def main(current_path, gdoc_to_write, *, logging_context=None, result_sink=None,
        diagnostic_limits=None):
    import run_logging
    own_context = logging_context is None
    context, report, result, code = logging_context, None, None, None
    root = Path(__file__).resolve().parent
    attempt_id = uuid.uuid4().hex
    try:
        if result_sink is not None and type(result_sink) is not dict:
            raise RuntimeError('D2_LIMITS_INVALID')
        if own_context:
            context = run_logging.configure_logging(root, run_id=uuid.uuid4().hex,
                environment='local', producer_sha=_d2_source_sha(root))
        if (type(context) is not dict or type(context.get('run_id')) is not str
                or re.fullmatch('[A-Za-z0-9][A-Za-z0-9_.-]{0,79}', context['run_id']) is None
                or type(context.get('producer_sha')) is not str
                or re.fullmatch('[0-9a-f]{7,40}', context['producer_sha']) is None):
            raise RuntimeError('D2_DIAGNOSTICS_INCOMPLETE')
        limits = _d2_validate_limits(diagnostic_limits)
        result = _d2_result(context['run_id'], attempt_id, context['producer_sha'], {})
        plan = _d2_build_source_plan()
        result = _d2_result(context['run_id'], attempt_id, context['producer_sha'], plan)
        _d2_validate_budget(plan, limits, context['run_id'], attempt_id, context['producer_sha'])
        result_root = _d2_result_root(root, context['run_id'], attempt_id)
        result = _d2_supervise(context, plan, result_root, context['run_id'], attempt_id,
            limits, current_path, gdoc_to_write)
        code = result['error_code']
    except BaseException as error:
        code = 'D2_INTERRUPTED' if isinstance(error, (KeyboardInterrupt, SystemExit)) else _d2_code(error)
        if result is not None:
            result.update(parent_outcome='INTERRUPTED' if code == 'D2_INTERRUPTED' else 'FAILED', error_code=code)
            _d2_aggregate(result)
    finally:
        if result is not None and result_sink is not None:
            result_sink.clear()
            result_sink.update(_d2_clone(result))
        if own_context and context is not None:
            application_error = None
            if code:
                try:
                    application_error = run_logging.make_error(context,
                        RuntimeError(code), 'upload', code=code)
                    fields = dict(stage='upload', outcome='FAILED', error=application_error)
                    if result is not None:
                        fields['artifact_id'] = result['artifact_id']
                    run_logging.emit_event(context, 'ERROR', 'd2_upload_failed',
                        'Upload execution failed', fields)
                except BaseException:
                    code = code or 'D2_DIAGNOSTICS_INCOMPLETE'
            try:
                report = run_logging.finalize_logging(context,
                    execution_outcome='INTERRUPTED' if code == 'D2_INTERRUPTED' else 'FAILED' if code else 'COMPLETED',
                    exit_status=130 if code == 'D2_INTERRUPTED' else 1 if code else 0,
                    application_error=application_error, release_result=None)
            except BaseException:
                code = code or 'D2_DIAGNOSTICS_INCOMPLETE'
            if result is not None:
                sink = result_sink if result_sink is not None else result
                status = _d2_apply_finalization(sink, report,
                    status=130 if code == 'D2_INTERRUPTED' else 1 if code else 0)
                if status:
                    code = code or 'D2_DIAGNOSTICS_INCOMPLETE'
                result = sink
    if code is not None:
        raise _d2_error(code, result or {}) from None
    if own_context and _d2_completion_allowed(result):
        print('Загрузка данных закончена. Локальные операции и диагностика завершены.')
    return None


def _d2_run_cli():
    try:
        main(current_path, gdoc_to_write)
        return 0
    except BaseException as error:
        code = 'D2_INTERRUPTED' if isinstance(error, KeyboardInterrupt) else _d2_code(error)
        sys.stderr.write(code + '\n')
        return 130 if code == 'D2_INTERRUPTED' else 1


if __name__ == '__main__':
    raise SystemExit(_d2_run_cli())
