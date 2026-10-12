"""LNX-02 only: admitted setup, immutable Linux locks and guarded offline replay.

This is not an application launcher, a generic sandbox or a lock update service.
All managed Python entries use -I -S -B and install the hook before third-party
imports. Native code is contained by a new rootless network namespace as well.
"""

import sys


class PolicyError(Exception):
    """A stable public failure code, without paths, URLs or exception payloads."""


class GuardDenied(PermissionError):
    pass


class AuditGuard:
    def __init__(self):
        self.phase = "offline"
        self.process = None
        self.probe = None
        self.expected = {}
        self.unexpected = {}
        self.public_addresses = set()
        self.registered = False

    def deny(self, event, args):
        expected = (
            self.probe == "dns" and event == "socket.getaddrinfo"
            and args and args[0] == "lnx02-audit.invalid"
        ) or (self.probe == "child" and event == "subprocess.Popen")
        counters = self.expected if expected else self.unexpected
        counters[event] = counters.get(event, 0) + 1
        raise GuardDenied("LNX02 audit denial: " + event)

    def __call__(self, event, args):
        if event in {"subprocess.Popen", "os.posix_spawn"}:
            if self.process is not None and len(args) > 1:
                if args[0] == self.process[0] and tuple(args[1]) == self.process:
                    return
            self.deny(event, args)
        if event in {"os.system", "os.exec", "os.fork", "os.forkpty", "os.spawn"}:
            self.deny(event, args)
        if event == "urllib.Request":
            if self.phase != "setup":
                self.deny(event, args)
            public_url(args[0])
        if not event.startswith("socket."):
            return
        if event == "socket.__new__":
            family = args[1] if len(args) > 1 else None
            if self.phase == "netlink" and family == 16:
                return
            if self.phase == "kernel-probe" and family == 2:
                return
            if self.phase == "setup" and family in {2, 10}:
                return
            self.deny(event, args)
        elif event == "socket.getaddrinfo":
            if self.phase == "setup" and args[0] in PUBLIC_HOSTS:
                return
            self.deny(event, args)
        elif event == "socket.connect":
            if self.phase == "kernel-probe" and args[1] == ("192.0.2.1", 9):
                return
            if (self.phase == "setup" and args[1][0] in self.public_addresses
                    and len(args[1]) >= 2 and args[1][1] == 443):
                return
            self.deny(event, args)
        elif event in {"socket.bind", "socket.sendto"}:
            if self.phase == "netlink" and getattr(args[0], "family", None) == 16:
                return
            self.deny(event, args)
        elif event in {
            "socket.gethostbyname", "socket.gethostbyaddr", "socket.getnameinfo",
            "socket.sethostname", "socket.sendmsg",
        }:
            self.deny(event, args)

    def evidence(self):
        return {
            "registered_before_third_party": self.registered,
            "pid": os.getpid(),
            "expected_denials": dict(sorted(self.expected.items())),
            "unexpected_denials": dict(sorted(self.unexpected.items())),
        }

GUARD = AuditGuard()
sys.addaudithook(GUARD)
GUARD.registered = True

import argparse
import copy
import email.parser
import errno
import hashlib
import importlib.metadata
import ipaddress
import json
import os
from pathlib import Path
import re
import runpy
import shutil
import socket
import stat
import struct
import subprocess
import sysconfig
import tempfile
import time
from urllib.parse import unquote, urlsplit
import urllib.request
import uuid
import venv
import zipfile


PROFILES = ("runtime", "test", "dev")
PUBLIC_HOSTS = frozenset({"pypi.org", "files.pythonhosted.org"})
PIP_VERSION = "26.2.1"
PIP_FILENAME = "pip-26.2.1-py3-none-any.whl"
PIP_SHA256 = "71138adf1f4ca900cdb7d289c21b7494329f2332b6d85f0e1c42108c0384ed3e"
LOCK_NAMES = {p: "linux-cp314-" + p + ".lock" for p in PROFILES}
GRANT_NAMES = (
    "MDS_LNX02_SETUP_APPROVED", "MDS_LNX02_APPROVED_SHA",
    "MDS_LNX02_APPROVED_BASE", "MDS_LNX02_OPERATION",
)
GITHUB_NAMES = (
    "GITHUB_EVENT_NAME", "GITHUB_REPOSITORY", "GITHUB_REF", "GITHUB_SHA",
    "GITHUB_ACTOR", "GITHUB_TRIGGERING_ACTOR",
    "TASK_EVENT", "TASK_EVENT_SHA", "TASK_EVENT_REF", "TASK_REPOSITORY",
    "TASK_ACTOR", "TASK_TRIGGERING_ACTOR", "TASK_RUNNER_ENVIRONMENT",
    "TASK_DELETED", "TASK_DISPATCH_ALLOW", "TASK_DISPATCH_SHA",
)
HEX = re.compile(r"[0-9a-f]{64}\Z")
SHA = re.compile(r"[0-9a-f]{40}\Z")
NAME = r"[a-z0-9]+(?:-[a-z0-9]+)*"
VERSION = r"[0-9]+(?:\.[0-9]+)*(?:(?:a|b|rc)[0-9]+|\.post[0-9]+|\.dev[0-9]+)?"
PIN = re.compile(r"(" + NAME + r")==(" + VERSION + r")\Z")
LOCK_LINE = re.compile(r"(" + NAME + r")==(" + VERSION + r") --hash=sha256:([0-9a-f]{64})\Z")
MAX_ARTIFACT = 256 * 1024 * 1024
MAX_JSON = 16 * 1024 * 1024
OWNER = ".lnx02-owner.json"
ORIGINAL_GETADDRINFO = socket.getaddrinfo
# urllib3 otherwise opens an IPv6 socket merely to probe local IPv6 support at
# import time. Offline workers deny every new IP socket; target wheels are x64,
# and public PyPI SETUP does not require an IPv6 connection.
socket.has_ipv6 = False


def canonical(name):
    if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", name):
        raise PolicyError("invalid-package-name")
    return re.sub(r"[-_.]+", "-", name).lower()


def digest(path):
    regular(path)
    value = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def regular(path):
    try:
        info = Path(path).lstat()
    except OSError:
        raise PolicyError("missing-regular-file") from None
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise PolicyError("aliased-or-nonregular-file")
    return info


def real_components(path, directory=False):
    path = Path(path)
    if not path.is_absolute() or ".." in path.parts:
        raise PolicyError("absolute-nontraversing-path-required")
    current = Path(path.anchor)
    for component in path.parts[1:]:
        current /= component
        if not current.exists() and not current.is_symlink():
            break
        info = current.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 1024:
            raise PolicyError("linked-path-component")
        if current != path and not stat.S_ISDIR(info.st_mode):
            raise PolicyError("non-directory-path-component")
    if directory and path.exists() and not stat.S_ISDIR(path.lstat().st_mode):
        raise PolicyError("directory-required")
    return path


def contained(root, path):
    root = real_components(Path(root).absolute(), directory=True)
    path = real_components(Path(path).absolute())
    try:
        path.relative_to(root)
    except ValueError:
        raise PolicyError("path-outside-owned-root") from None
    if path == root:
        raise PolicyError("owned-root-is-not-a-file")
    return path


def write_new(root, path, payload):
    path = contained(root, path)
    real_components(path.parent, directory=True)
    if path.exists() or path.is_symlink():
        raise PolicyError("write-destination-not-fresh")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
    except BaseException:
        if path.exists() and stat.S_ISREG(path.lstat().st_mode):
            path.unlink()
        raise
    regular(path)


def json_bytes(value):
    return (json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n").encode("utf-8")


def read_json(path):
    info = regular(path)
    if info.st_size > MAX_JSON:
        raise PolicyError("json-too-large")
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (ValueError, UnicodeError):
        raise PolicyError("invalid-json") from None


def owned(root, nonce):
    root = real_components(Path(root).absolute(), directory=True)
    if not root.is_dir() or read_json(root / OWNER).get("nonce") != nonce:
        raise PolicyError("owned-root-identity-mismatch")
    if hasattr(os, "getuid") and root.lstat().st_uid != os.getuid():
        raise PolicyError("owned-root-uid-mismatch")
    return root


def new_scratch(temp_root):
    temp_root = real_components(Path(temp_root).absolute(), directory=True)
    if not temp_root.is_dir():
        raise PolicyError("temporary-parent-missing")
    root = Path(tempfile.mkdtemp(prefix="lnx02-", dir=temp_root))
    os.chmod(root, 0o700)
    nonce = uuid.uuid4().hex
    write_new(root, root / OWNER, json_bytes({"nonce": nonce, "purpose": "LNX-02"}))
    for name in ("jobs", "logs", "tmp", "home", "bootstrap", "wheelhouse", "locks", "constraints", "fixtures"):
        (root / name).mkdir(mode=0o700)
    return root, nonce


def cleanup(root, nonce):
    root = owned(root, nonce)
    count = 0
    for parent, directories, files in os.walk(root, followlinks=False):
        real_components(Path(parent), directory=True)
        for name in directories:
            real_components(Path(parent) / name, directory=True)
        for name in files:
            regular(Path(parent) / name)
        count += len(directories) + len(files)
        if count > 50000:
            raise PolicyError("cleanup-inventory-limit")
    owned(root, nonce)
    shutil.rmtree(root)


def parse_lock(text):
    records = {}
    order = []
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        match = LOCK_LINE.fullmatch(line)
        if not match or match[1] in records:
            raise PolicyError("invalid-or-duplicate-lock-line")
        records[match[1]] = (match[2], match[3])
        order.append(match[1])
    if not records or order != sorted(order):
        raise PolicyError("empty-or-unsorted-lock")
    return records


def lock_text(records):
    result = "# Generated by LNX-02; Ubuntu 24.04 x86-64 CPython 3.14.8 GIL; selected PyPI wheels.\n"
    for name in sorted(records):
        version, sha256 = records[name]
        result += name + "==" + version + " --hash=sha256:" + sha256 + "\n"
    parse_lock(result)
    return result


def constraint_text(records):
    return "".join(name + "==" + records[name][0] + "\n" for name in sorted(records))


def expected_profiles(repo):
    result = {}
    for profile in PROFILES:
        path = repo / "requirements" / ("windows-cp314-" + profile + ".lock")
        pins = parse_lock(path.read_text(encoding="utf-8"))
        pins.pop("colorama", None)
        if profile == "dev":
            pins.pop("pywin32", None)
        result[profile] = {name: value[0] for name, value in pins.items()}
        direct = direct_requirements(repo, profile)
        if any(result[profile].get(name) != version for name, version in direct.items()):
            raise PolicyError("direct-input-and-seed-pins-differ")
    for profile in ("test", "dev"):
        if any(result[profile].get(name) != version for name, version in result["runtime"].items()):
            raise PolicyError("windows-runtime-profile-drift")
    if any(result["dev"].get(name) != version for name, version in result["test"].items()):
        raise PolicyError("windows-test-profile-drift")
    return result


def direct_requirements(repo, profile):
    records = {}
    if profile in {"test", "dev"}:
        records.update(direct_requirements(repo, "runtime" if profile == "test" else "test"))
    regular(repo / "requirements" / (profile + ".in"))
    for line in (repo / "requirements" / (profile + ".in")).read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        if line.startswith("-r "):
            expected = "runtime.in" if profile == "test" else "test.in"
            if profile == "runtime" or line != "-r " + expected:
                raise PolicyError("unapproved-requirement-include")
            continue
        if "==" not in line:
            raise PolicyError("unpinned-direct-input")
        name, version = line.split("==", 1)
        name = canonical(name)
        if not PIN.fullmatch(name + "==" + version) or name in records:
            raise PolicyError("invalid-direct-input")
        records[name] = version
    return records


def validate_versions(records, expected):
    if {name: value[0] for name, value in records.items()} != expected:
        raise PolicyError("linux-closure-or-version-drift")


def public_url(value):
    if not isinstance(value, str):
        raise PolicyError("invalid-public-url")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        raise PolicyError("invalid-public-url") from None
    if (parsed.scheme != "https" or parsed.hostname not in PUBLIC_HOSTS
            or parsed.username or parsed.password or port not in {None, 443}
            or parsed.query or parsed.fragment or "\\" in value):
        raise PolicyError("non-public-or-non-https-source")
    return parsed


def wheel_name(filename):
    if not isinstance(filename, str) or "/" in filename or "\\" in filename or not filename.endswith(".whl"):
        raise PolicyError("wheel-filename-required")
    parts = filename[:-4].split("-")
    if len(parts) not in {5, 6}:
        raise PolicyError("invalid-wheel-filename")
    name, version = canonical(parts[0]), parts[1]
    if not PIN.fullmatch(name + "==" + version):
        raise PolicyError("invalid-wheel-version")
    python_tag, abi_tag, platform_tag = parts[-3:]
    compatible = False
    for python in python_tag.split("."):
        for abi in abi_tag.split("."):
            for platform in platform_tag.split("."):
                if python in {"py3", "py2"} and "py3" in python_tag.split(".") and abi == "none" and platform == "any":
                    compatible = True
                    continue
                ordinary = python == "cp314" and abi == "cp314"
                stable = re.fullmatch(r"cp3([0-9]+)", python)
                stable = stable and 2 <= int(stable[1]) <= 14 and abi == "abi3"
                if not ordinary and not stable:
                    continue
                legacy = {"manylinux1_x86_64", "manylinux2010_x86_64", "manylinux2014_x86_64"}
                numeric = re.fullmatch(r"manylinux_2_([0-9]+)_x86_64", platform)
                if platform in legacy or (numeric and int(numeric[1]) <= 39):
                    compatible = True
    if not compatible or "cp314t" in filename or "musllinux" in filename:
        raise PolicyError("non-target-wheel")
    return name, version


def parse_report(report, expected, local=False):
    if report.get("version") != "1" or report.get("pip_version") != PIP_VERSION:
        raise PolicyError("unexpected-pip-report-version")
    target = {
        "implementation_name": "cpython", "platform_python_implementation": "CPython",
        "sys_platform": "linux", "platform_system": "Linux",
        "platform_machine": "x86_64", "python_full_version": "3.14.8",
    }
    if any(report.get("environment", {}).get(key) != value for key, value in target.items()):
        raise PolicyError("non-target-pip-report")
    installs = report.get("install")
    if not isinstance(installs, list) or not installs:
        raise PolicyError("empty-pip-report")
    records, wheels = {}, {}
    for item in installs:
        if item.get("is_yanked") is not False:
            raise PolicyError("yanked-or-unclassified-package")
        if not local and item.get("is_direct") is not False:
            raise PolicyError("direct-url-requirement")
        download = item.get("download_info", {})
        if "vcs_info" in download or "dir_info" in download:
            raise PolicyError("vcs-or-editable-source")
        parsed = urlsplit(download.get("url", "")) if local else public_url(download.get("url"))
        if local and (parsed.scheme != "file" or parsed.netloc or parsed.query or parsed.fragment):
            raise PolicyError("replay-report-is-not-local")
        if not local and parsed.hostname != "files.pythonhosted.org":
            raise PolicyError("wheel-not-from-public-pypi-files")
        filename = unquote(parsed.path.rsplit("/", 1)[-1])
        name, version = wheel_name(filename)
        metadata = item.get("metadata", {})
        if canonical(metadata.get("name")) != name or metadata.get("version") != version or name in records:
            raise PolicyError("duplicate-or-mismatched-wheel-metadata")
        sha256 = download.get("archive_info", {}).get("hashes", {}).get("sha256")
        if not isinstance(sha256, str) or not HEX.fullmatch(sha256):
            raise PolicyError("missing-wheel-sha256")
        records[name] = (version, sha256)
        wheels[name] = {"name": name, "version": version, "filename": filename,
                       "sha256": sha256, "source_url": download["url"] if not local else "wheelhouse:" + filename}
    validate_versions(records, expected)
    return records, wheels


def validate_wheel(path, record):
    if path.name != record["filename"] or digest(path) != record["sha256"]:
        raise PolicyError("wheel-bytes-digest-mismatch")
    if wheel_name(path.name) != (record["name"], record["version"]):
        raise PolicyError("wheel-record-mismatch")
    try:
        with zipfile.ZipFile(path) as archive:
            metadata_files = []
            seen = set()
            for info in archive.infolist():
                name = info.filename
                parts = name.split("/")
                if (name in seen or name.startswith("/") or "\\" in name or ":" in name
                        or ".." in parts or stat.S_ISLNK(info.external_attr >> 16)):
                    raise PolicyError("unsafe-wheel-member")
                seen.add(name)
                if name.endswith(".dist-info/METADATA"):
                    metadata_files.append(info)
            if len(metadata_files) != 1 or metadata_files[0].file_size > 1024 * 1024:
                raise PolicyError("invalid-wheel-metadata-members")
            metadata = email.parser.BytesParser().parsebytes(archive.read(metadata_files[0]))
            if canonical(metadata.get("Name")) != record["name"] or metadata.get("Version") != record["version"]:
                raise PolicyError("wheel-embedded-metadata-mismatch")
    except (zipfile.BadZipFile, UnicodeError):
        raise PolicyError("invalid-wheel-archive") from None


def validate_grant(env, mode, sha, base):
    if mode not in {"generate", "verify"} or not SHA.fullmatch(sha) or not SHA.fullmatch(base):
        raise PolicyError("invalid-operation-identity")
    if (env.get("MDS_LNX02_SETUP_APPROVED") != "true"
            or env.get("MDS_LNX02_APPROVED_SHA") != sha
            or env.get("MDS_LNX02_APPROVED_BASE") != base
            or env.get("MDS_LNX02_OPERATION") != mode):
        raise PolicyError("setup-grant-missing-or-mismatched")
    if (env.get("GITHUB_REPOSITORY") != "foykes/moex-dataset"
            or env.get("GITHUB_ACTOR") != "foykes"
            or env.get("GITHUB_TRIGGERING_ACTOR") != "foykes"
            or env.get("GITHUB_SHA") != sha):
        raise PolicyError("unapproved-repository-actor-or-source")
    mirrors = {"TASK_EVENT": "GITHUB_EVENT_NAME", "TASK_EVENT_SHA": "GITHUB_SHA",
               "TASK_EVENT_REF": "GITHUB_REF", "TASK_REPOSITORY": "GITHUB_REPOSITORY",
               "TASK_ACTOR": "GITHUB_ACTOR", "TASK_TRIGGERING_ACTOR": "GITHUB_TRIGGERING_ACTOR"}
    if any(env.get(task) != env.get(github) for task, github in mirrors.items()):
        raise PolicyError("event-context-mirror-mismatch")
    if env.get("TASK_RUNNER_ENVIRONMENT") != "github-hosted" or env.get("TASK_DELETED") != "false":
        raise PolicyError("unapproved-runner-or-deleted-ref")
    if env.get("GITHUB_EVENT_NAME") == "push":
        if env.get("GITHUB_REF") != "refs/heads/codex/lnx-02":
            raise PolicyError("unapproved-push-ref")
    elif env.get("GITHUB_EVENT_NAME") == "workflow_dispatch":
        if (env.get("GITHUB_REF") != "refs/heads/main" or mode != "verify" or base != sha
                or env.get("TASK_DISPATCH_ALLOW") != "true"
                or env.get("TASK_DISPATCH_SHA") != sha):
            raise PolicyError("unapproved-dispatch-input")
    else:
        raise PolicyError("unapproved-event")


def flags():
    if not (sys.flags.isolated and sys.flags.no_site and sys.flags.dont_write_bytecode):
        raise PolicyError("python-requires-I-S-B")


def identity():
    flags()
    if (sys.implementation.name != "cpython" or sys.version_info[:3] != (3, 14, 8)
            or sys.platform != "linux" or struct.calcsize("P") != 8
            or os.uname().machine != "x86_64"
            or sysconfig.get_config_var("Py_GIL_DISABLED") not in {0, None}
            or not sys._is_gil_enabled()):
        raise PolicyError("requires-linux-x64-cp3148-gil")
    release = {}
    for line in Path("/etc/os-release").read_text(encoding="utf-8").splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            release[key] = value.strip('"')
    if release.get("ID") != "ubuntu" or release.get("VERSION_ID") != "24.04":
        raise PolicyError("requires-ubuntu-2404")
    glibc = os.confstr("CS_GNU_LIBC_VERSION")
    if glibc != "glibc 2.39":
        raise PolicyError("requires-ubuntu-glibc-239")
    return {
        "os": "Ubuntu", "os_version": "24.04", "architecture": "x86_64",
        "python": "3.14.8", "implementation": "CPython", "gil_enabled": True,
        "glibc": glibc, "python_build": sys.version.splitlines()[0],
        "soabi": sysconfig.get_config_var("SOABI"),
        "py_gil_disabled": sysconfig.get_config_var("Py_GIL_DISABLED"),
        "runner_image_os": os.environ.get("ImageOS", "NOT AVAILABLE"),
        "runner_image_version": os.environ.get("ImageVersion", "NOT AVAILABLE"),
        "python_executable_name": Path(sys.executable).name,
        "python_executable_sha256": digest(Path(sys.executable).resolve()),
        "helper_sha256": digest(Path(__file__).absolute()),
        "provisioning_boundary": "Trusted pre-existing toolcache; source archive bytes not revalidated",
    }


def network_ids():
    return {name: os.stat("/proc/self/ns/" + name).st_ino for name in ("user", "net")}


def effective_capabilities():
    for line in Path("/proc/self/status").read_text(encoding="ascii").splitlines():
        if line.startswith("CapEff:"):
            return int(line.split()[1], 16)
    raise PolicyError("effective-capabilities-unavailable")


def netlink_dump(kind, payload, response_kind):
    previous = GUARD.phase
    GUARD.phase = "netlink"
    try:
        with socket.socket(socket.AF_NETLINK, socket.SOCK_RAW, socket.NETLINK_ROUTE) as handle:
            handle.settimeout(2)
            handle.bind((0, 0))
            handle.sendto(struct.pack("IHHII", 16 + len(payload), kind, 0x301, 1, 0) + payload, (0, 0))
            responses = []
            for _ in range(64):
                data = handle.recv(65536)
                offset = 0
                while offset + 16 <= len(data):
                    length, message, _, sequence, _ = struct.unpack_from("IHHII", data, offset)
                    if length < 16 or offset + length > len(data) or sequence != 1:
                        raise PolicyError("invalid-netlink-response")
                    body = data[offset + 16:offset + length]
                    if message == 3:
                        return responses
                    if message == 2:
                        if len(body) < 4 or struct.unpack_from("i", body)[0] != 0:
                            raise PolicyError("netlink-query-failed")
                    elif message == response_kind:
                        responses.append(body)
                    else:
                        raise PolicyError("unexpected-netlink-message")
                    offset += (length + 3) & ~3
            raise PolicyError("netlink-response-limit")
    finally:
        GUARD.phase = previous


def negative_probes(use_requests=False):
    dns_before = GUARD.expected.get("socket.getaddrinfo", 0)
    GUARD.probe = "dns"
    try:
        if use_requests:
            from pip._vendor import requests
            with requests.Session() as session:
                session.trust_env = False
                try:
                    session.get("https://lnx02-audit.invalid/", timeout=1, allow_redirects=False)
                except requests.exceptions.RequestException:
                    pass
        else:
            try:
                socket.getaddrinfo("lnx02-audit.invalid", 443, type=socket.SOCK_STREAM)
            except GuardDenied:
                pass
    finally:
        GUARD.probe = None
    if GUARD.expected.get("socket.getaddrinfo", 0) != dns_before + 1:
        raise PolicyError("same-process-dns-guard-probe-failed")
    child_before = GUARD.expected.get("subprocess.Popen", 0)
    GUARD.probe = "child"
    try:
        try:
            subprocess.run([sys.executable, "-I", "-S", "-B", "-c", "raise SystemExit(93)"], check=False)
        except GuardDenied:
            pass
    finally:
        GUARD.probe = None
    if GUARD.expected.get("subprocess.Popen", 0) != child_before + 1:
        raise PolicyError("same-process-child-guard-probe-failed")
    if GUARD.unexpected:
        raise PolicyError("unexpected-audit-denial")
    return {"pid": os.getpid(), "dns_denied_before_resolver": True,
            "child_denied_before_launch": True, "requests_fixture": use_requests}


def parse_ipv4_no_routes(text):
    """Accept an empty proc table or its header, with no route records."""
    header = "Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT".split()
    if text and (len(text) > 512 or len(text.splitlines()) != 1 or text.split() != header):
        raise PolicyError("namespace-ipv4-proc-has-route-or-invalid-header")


def parse_ipv6_null_routes(text):
    """Allow only kernel unreachable sentinels, never a usable IPv6 route."""
    if len(text) > 4096:
        raise PolicyError("namespace-ipv6-proc-size-limit")
    rows = text.splitlines()
    if len(rows) > 16:
        raise PolicyError("namespace-ipv6-proc-row-limit")
    for row in rows:
        fields = row.split()
        if (len(fields) != 10 or fields[0] != "0" * 32 or fields[1] != "00"
                or fields[2] != "0" * 32 or fields[3] != "00"
                or fields[4] != "0" * 32 or fields[5] != "ffffffff"
                or not re.fullmatch(r"[0-9a-f]{8}", fields[6])
                or not re.fullmatch(r"[0-9a-f]{8}", fields[7])
                or fields[8] != "00200200" or fields[9] != "lo"):
            raise PolicyError("namespace-ipv6-proc-has-nonsentinel-route")
    return len(rows)


def namespace_proof(parent):
    if (os.getuid(), os.geteuid(), os.getgid()) != tuple(parent["ids"]):
        raise PolicyError("namespace-changed-process-uid-or-gid")
    namespaces = network_ids()
    if any(namespaces[key] == parent["namespaces"][key] for key in ("user", "net")):
        raise PolicyError("namespace-is-not-independent")
    if effective_capabilities() != 0:
        raise PolicyError("namespace-effective-capabilities-not-zero")
    links = netlink_dump(18, struct.pack("BBHiII", 0, 0, 0, 0, 0, 0), 16)
    names = []
    for body in links:
        if len(body) < 16:
            raise PolicyError("invalid-link-response")
        _, _, _, _, link_flags, _ = struct.unpack_from("BBHiII", body)
        if link_flags & 1:
            raise PolicyError("namespace-interface-is-up")
        offset, interface = 16, None
        while offset + 4 <= len(body):
            length, attribute = struct.unpack_from("HH", body, offset)
            if length < 4 or offset + length > len(body):
                raise PolicyError("invalid-link-attribute")
            if attribute == 3:
                interface = body[offset + 4:offset + length].rstrip(b"\0").decode("ascii")
            offset += (length + 3) & ~3
        names.append(interface)
    if names != ["lo"]:
        raise PolicyError("namespace-has-non-loopback-interface")
    addresses = netlink_dump(22, struct.pack("BBBBI", 0, 0, 0, 0, 0), 20)
    routes = netlink_dump(26, b"\0" * 12, 24)
    if addresses or routes:
        raise PolicyError("namespace-has-addresses-or-routes")
    parse_ipv4_no_routes(Path("/proc/self/net/route").read_text(encoding="ascii"))
    ipv6_null_count = parse_ipv6_null_routes(Path("/proc/self/net/ipv6_route").read_text(encoding="ascii"))
    previous = GUARD.phase
    GUARD.phase = "kernel-probe"
    kernel_errno = None
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
            handle.settimeout(1)
            try:
                handle.connect(("192.0.2.1", 9))
            except GuardDenied:
                raise PolicyError("kernel-probe-intercepted-by-audit") from None
            except OSError as error:
                kernel_errno = error.errno
    finally:
        GUARD.phase = previous
    if kernel_errno != errno.ENETUNREACH:
        raise PolicyError("namespace-kernel-network-not-unreachable")
    return {"uid_preserved": True, "gid_preserved": True, "cap_eff": 0,
            "parent_ids": list(parent["ids"]), "child_ids": [os.getuid(), os.geteuid(), os.getgid()],
            "parent_namespace_inodes": parent["namespaces"], "child_namespace_inodes": namespaces,
            "distinct_user_namespace": True, "distinct_network_namespace": True,
            "interfaces": [{"name": "lo", "up": False}], "address_count": 0,
            "route_count": 0, "ipv6_null_route_count": ipv6_null_count, "kernel_probe_errno": kernel_errno,
            "kernel_probe": "192.0.2.1:9", "kernel_probe_reached_os": True}


def restricted_getaddrinfo(host, *args, **kwargs):
    results = ORIGINAL_GETADDRINFO(host, *args, **kwargs)
    if GUARD.phase == "setup":
        if host not in PUBLIC_HOSTS:
            raise PolicyError("setup-dns-host-not-public-pypi")
        for result in results:
            address = result[4][0]
            if not ipaddress.ip_address(address).is_global:
                raise PolicyError("setup-dns-returned-nonpublic-address")
            GUARD.public_addresses.add(address)
    return results


socket.getaddrinfo = restricted_getaddrinfo


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, handle, code, message, headers, new_url):
        raise PolicyError("source-redirect-rejected")


def public_bytes(url, maximum):
    public_url(url)
    previous = GUARD.phase
    GUARD.phase = "setup"
    try:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
        request = urllib.request.Request(url, headers={"User-Agent": "LNX02-source-review/1"})
        with opener.open(request, timeout=60) as response:
            if response.status != 200 or response.geturl() != url:
                raise PolicyError("public-source-response-mismatch")
            data = response.read(maximum + 1)
        if len(data) > maximum:
            raise PolicyError("public-source-size-limit")
        return data
    finally:
        GUARD.phase = previous


def release_metadata(name, version):
    url = "https://pypi.org/pypi/" + name + "/" + version + "/json"
    try:
        metadata = json.loads(public_bytes(url, MAX_JSON))
    except (ValueError, UnicodeError):
        raise PolicyError("invalid-public-pypi-metadata") from None
    if canonical(metadata.get("info", {}).get("name")) != name or metadata["info"].get("version") != version:
        raise PolicyError("public-pypi-release-mismatch")
    return metadata


def confirm_windows_markers():
    checks = (
        ("build", "1.6.1", 'colorama; os_name == "nt"'),
        ("pytest", "8.4.2", 'colorama>=0.4; sys_platform == "win32"'),
        ("jupyter-core", "5.8.1", 'pywin32>=300; sys_platform == "win32" and platform_python_implementation != "PyPy"'),
    )
    for name, version, marker in checks:
        if marker not in release_metadata(name, version).get("info", {}).get("requires_dist", []):
            raise PolicyError("windows-only-marker-provenance-changed")


def public_wheel(metadata, name, version, sha256=None, filename=None):
    matches = []
    for item in metadata.get("urls", []):
        candidate = item.get("filename", "")
        if filename is not None and candidate != filename:
            continue
        if sha256 is not None and item.get("digests", {}).get("sha256") != sha256:
            continue
        try:
            if wheel_name(candidate) != (name, version):
                continue
            parsed = public_url(item.get("url"))
        except PolicyError:
            continue
        if parsed.hostname != "files.pythonhosted.org" or item.get("yanked") is not False or item.get("packagetype") != "bdist_wheel":
            continue
        value = item.get("digests", {}).get("sha256")
        if not isinstance(value, str) or not HEX.fullmatch(value):
            continue
        matches.append({"name": name, "version": version, "filename": candidate,
                        "sha256": value, "source_url": item["url"]})
    if len(matches) != 1:
        raise PolicyError("pypi-wheel-missing-ambiguous-or-hash-mismatched")
    return matches[0]


def fetch_wheel(root, directory, record):
    target = contained(root, directory / record["filename"])
    if target.exists():
        validate_wheel(target, record)
        return
    data = public_bytes(record["source_url"], MAX_ARTIFACT)
    if hashlib.sha256(data).hexdigest() != record["sha256"]:
        raise PolicyError("downloaded-wheel-sha256-mismatch")
    write_new(root, target, data)
    validate_wheel(target, record)


def execute(command, cwd, environment):
    if GUARD.process is not None:
        raise PolicyError("nested-controller-process-admission")
    GUARD.process = tuple(command)
    try:
        return subprocess.run(command, cwd=cwd, env=environment, capture_output=True, timeout=600, check=False)
    finally:
        GUARD.process = None


def git_read(repo, args, allowed_failure=False):
    executable = shutil.which("git")
    if executable is None:
        raise PolicyError("native-git-unavailable")
    environment = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LANG": "C.UTF-8",
                   "GIT_CONFIG_SYSTEM": os.devnull, "GIT_CONFIG_GLOBAL": os.devnull,
                   "GIT_TERMINAL_PROMPT": "0", "GIT_OPTIONAL_LOCKS": "0"}
    command = [executable, "-c", "core.hooksPath=" + os.devnull, "-C", str(repo)] + args
    result = execute(command, str(repo), environment)
    if result.returncode and not allowed_failure:
        raise PolicyError("native-git-read-failed")
    return result


def source_identity(repo, sha, base):
    if git_read(repo, ["rev-parse", "HEAD"]).stdout.decode("ascii").strip() != sha:
        raise PolicyError("source-head-drift")
    if git_read(repo, ["rev-parse", "refs/remotes/origin/main"]).stdout.decode("ascii").strip() != base:
        raise PolicyError("main-base-drift")
    if git_read(repo, ["merge-base", "--is-ancestor", base, sha], True).returncode:
        raise PolicyError("base-is-not-source-ancestor")
    if git_read(repo, ["diff", "--exit-code", "HEAD"], True).returncode:
        raise PolicyError("tracked-source-is-dirty")


def source_manifest(repo, mode):
    files = [".github/lnx02/verify_env.py"]
    files += ["requirements/" + profile + ".in" for profile in PROFILES]
    files += ["requirements/windows-cp314-" + profile + ".lock" for profile in PROFILES]
    if mode == "verify":
        files += ["requirements/" + LOCK_NAMES[profile] for profile in PROFILES]
    result = {}
    for name in files:
        path = contained(repo, repo / name)
        result[name] = digest(path)
    return result


def clean_environment(root):
    result = {
        "PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
        "HOME": str(root / "home"), "TMPDIR": str(root / "tmp"),
        "PIP_CONFIG_FILE": os.devnull, "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1", "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
    }
    for name in GRANT_NAMES + GITHUB_NAMES:
        if name in os.environ:
            result[name] = os.environ[name]
    for name in ("ImageOS", "ImageVersion"):
        if name in os.environ:
            result[name] = os.environ[name]
    return result


def worker_launch(root, nonce, python, kind, fields, sequence, offline=True):
    owned(root, nonce)
    job_name = str(sequence) + "-" + kind
    job_path = root / "jobs" / (job_name + ".json")
    result_path = root / "jobs" / (job_name + "-result.json")
    parent = {"ids": [os.getuid(), os.geteuid(), os.getgid()], "namespaces": network_ids()}
    job = {"format": "lnx02-worker-v1", "root": str(root), "nonce": nonce,
           "kind": kind, "result": str(result_path), "helper_sha256": digest(Path(__file__).absolute()),
           "parent": parent, "offline": offline}
    job.update(fields)
    write_new(root, job_path, json_bytes(job))
    command = [str(python), "-I", "-S", "-B", str(Path(__file__).absolute()),
               "worker", "--job", str(job_path), "--job-sha256", digest(job_path)]
    if offline:
        unshare = shutil.which("unshare")
        if unshare is None:
            raise PolicyError("rootless-unshare-unavailable")
        command = [unshare, "--user", "--map-current-user", "--net"] + command
    started = time.monotonic()
    try:
        result = execute(command, str(root), clean_environment(root))
    except subprocess.TimeoutExpired as error:
        write_new(root, root / "logs" / (job_name + ".stdout"), error.output or b"")
        write_new(root, root / "logs" / (job_name + ".stderr"), error.stderr or b"")
        raise StageError("guarded-worker-timeout", {
            "kind": kind, "helper_sha256": job["helper_sha256"], "status": "FAIL",
            "failure_code": "worker-timeout", "exit_code": None, "timed_out": True,
            "duration_seconds": round(time.monotonic() - started, 3),
        }) from None
    write_new(root, root / "logs" / (job_name + ".stdout"), result.stdout)
    write_new(root, root / "logs" / (job_name + ".stderr"), result.stderr)
    if network_ids() != parent["namespaces"]:
        raise PolicyError("controller-namespace-changed")
    if not result_path.exists():
        raise StageError("guarded-worker-did-not-produce-result", {
            "kind": kind, "helper_sha256": job["helper_sha256"], "status": "FAIL",
            "failure_code": "worker-result-unavailable", "exit_code": result.returncode,
            "guard_evidence": "NOT AVAILABLE",
            "duration_seconds": round(time.monotonic() - started, 3),
            "parent_namespace_unchanged": True,
        })
    outcome = read_json(result_path)
    if outcome.get("kind") != kind or outcome.get("helper_sha256") != job["helper_sha256"]:
        raise PolicyError("worker-evidence-identity-mismatch")
    outcome["exit_code"] = result.returncode
    outcome["duration_seconds"] = round(time.monotonic() - started, 3)
    outcome["parent_namespace_unchanged"] = True
    if result.returncode or outcome.get("status") != "PASS":
        raise StageError("guarded-" + kind + "-failed", outcome)
    return outcome


class StageError(PolicyError):
    def __init__(self, code, outcome):
        super().__init__(code)
        self.outcome = outcome


class PipOperationError(PolicyError):
    def __init__(self, code, operation):
        super().__init__(code)
        self.operation = operation


def pip_load(root, bootstrap=False):
    flags()
    if sys.prefix == sys.base_prefix:
        raise PolicyError("pip-worker-requires-fresh-venv")
    for key in ("purelib", "platlib"):
        path = sysconfig.get_path(key)
        if path not in sys.path:
            contained(root, Path(path))
            real_components(Path(path), directory=True)
            sys.path.append(path)
    if bootstrap:
        wheel = root / "bootstrap" / PIP_FILENAME
        if digest(wheel) != PIP_SHA256:
            raise PolicyError("bootstrap-pip-wheel-hash-mismatch")
        sys.path.insert(0, str(wheel))
    import pip
    if pip.__version__ != PIP_VERSION:
        raise PolicyError("unexpected-pip-version")
    origin = Path(pip.__file__)
    if bootstrap:
        if not str(origin).startswith(str(root / "bootstrap" / PIP_FILENAME) + os.sep):
            raise PolicyError("pip-not-loaded-from-verified-wheel")
    else:
        contained(root, origin)
        regular(origin)
    # Pip's normal user-agent probes lsb_release/uname/rustc in child processes.
    # Fixed pip + this offline/setup-only header avoids these unrelated probes.
    # No pip source file, resolver logic or package artifact is changed.
    import pip._internal.network.session as session
    session.user_agent = lambda: "pip/26.2.1 lnx02-guarded"
    return {"version": pip.__version__, "verified_bootstrap_sha256": PIP_SHA256,
            "fixed_user_agent_without_child_probes": True}


def pip_run(arguments, setup=False):
    sys.argv = ["pip", "--isolated", "--no-cache-dir", "--disable-pip-version-check"] + arguments
    previous = GUARD.phase
    GUARD.phase = "setup" if setup else "offline"
    code = 0
    escaped = None
    try:
        try:
            runpy.run_module("pip", run_name="__main__")
        except SystemExit as result:
            code = result.code if isinstance(result.code, int) else 1
        except BaseException as error:
            code = None
            escaped = type(error).__name__
    finally:
        GUARD.phase = previous
    operation = {"exit_code": code, "pid": os.getpid(), "runpy_same_pid": True}
    if escaped is not None:
        operation["exception_type"] = escaped
        raise PipOperationError("pip-unhandled-exception", operation)
    if GUARD.unexpected:
        raise PipOperationError("unexpected-audit-denial-during-pip", operation)
    if code:
        raise PipOperationError("pip-exit-" + str(code), operation)
    return operation


def installed_inventory():
    records = {}
    for distribution in importlib.metadata.distributions():
        name = canonical(distribution.metadata["Name"])
        if name in records:
            raise PolicyError("duplicate-installed-distribution")
        records[name] = distribution.version
    return dict(sorted(records.items()))


def native_smoke(root, profile, role):
    import numpy as np
    import pandas as pd
    import talib
    import openpyxl
    if (np.__version__ != "2.3.4" or pd.__version__ != "2.3.3"
            or talib.__version__ != "0.6.8" or openpyxl.__version__ != "3.1.5"):
        raise PolicyError("native-import-version-drift")
    np.testing.assert_allclose(np.arange(6, dtype=np.float64).sum(), 15.0)
    values = np.asarray([1, 2, 3, 4, 5], dtype=np.float64)
    actual = talib.SMA(values, timeperiod=3)
    np.testing.assert_allclose(actual, np.asarray([np.nan, np.nan, 2, 3, 4]), equal_nan=True)
    fixture_root = contained(root, root / "fixtures" / (profile + "-" + role))
    fixture_root.mkdir(mode=0o700)
    frame = pd.DataFrame({"ticker": ["AAA", "BBB", "VVV"],
                          "close": [1.25, np.nan, 3.5], "volume": [2, 0, 7]})
    csv_path, xlsx_path = fixture_root / "fixture.csv", fixture_root / "fixture.xlsx"
    for path in (csv_path, xlsx_path):
        contained(root, path)
        if path.exists() or path.is_symlink():
            raise PolicyError("synthetic-output-not-fresh")
    frame.to_csv(csv_path, index=False, encoding="utf-8")
    csv = pd.read_csv(csv_path, dtype={"ticker": str, "volume": "int64"})
    pd.testing.assert_frame_equal(csv, frame)
    frame.to_excel(xlsx_path, index=False, engine="openpyxl")
    xlsx = pd.read_excel(xlsx_path, engine="openpyxl", dtype={"ticker": str, "volume": "int64"})
    pd.testing.assert_frame_equal(xlsx, frame)
    native_version = talib.__ta_version__
    if isinstance(native_version, bytes):
        native_version = native_version.decode("ascii")
    return {"numpy": np.__version__, "pandas": pd.__version__, "ta_lib": talib.__version__,
            "ta_native": native_version.split()[0], "numpy_synthetic": "PASS", "ta_sma_synthetic": "PASS",
            "csv_synthetic": "PASS", "xlsx_synthetic": "PASS", "project_modules_imported": []}


def job_file(root, job, key, directory=False):
    path = contained(root, Path(job[key]))
    if directory:
        real_components(path, directory=True)
        if not path.is_dir():
            raise PolicyError("job-directory-missing")
    else:
        regular(path)
    return path


def worker(args):
    flags()
    path = real_components(Path(args.job).absolute())
    if digest(path) != args.job_sha256:
        raise PolicyError("worker-job-digest-mismatch")
    job = read_json(path)
    root = owned(Path(job["root"]), job["nonce"])
    contained(root, path)
    if job.get("format") != "lnx02-worker-v1" or digest(Path(__file__).absolute()) != job.get("helper_sha256"):
        raise PolicyError("worker-source-identity-mismatch")
    result_path = contained(root, Path(job["result"]))
    outcome = {"kind": job.get("kind"), "helper_sha256": job["helper_sha256"], "status": "FAIL"}
    if "profile" in job:
        outcome["profile"] = job["profile"]
    if "role" in job:
        outcome["role"] = job["role"]
    code = 1
    try:
        outcome["identity"] = identity()
        if job.get("offline") is True:
            outcome["namespace"] = namespace_proof(job["parent"])
        elif job.get("offline") is not False:
            raise PolicyError("worker-network-phase-unclassified")
        if job["kind"] == "preflight":
            outcome["negative_probes"] = negative_probes()
        else:
            validate_grant(os.environ, job["mode"], job["sha"], job["base"])
            profile = job["profile"]
            if profile not in PROFILES:
                raise PolicyError("unknown-profile")
            outcome["pip"] = pip_load(root, bootstrap=job["kind"] == "bootstrap")
            outcome["negative_probes"] = negative_probes(use_requests=True)
            if job["kind"] == "bootstrap":
                outcome["pip_operation"] = pip_run([
                    "install", "--ignore-installed", "--no-deps", "--no-index", "--require-hashes",
                    "--only-binary=:all:", "--no-compile", "--find-links", str(root / "bootstrap"),
                    "-r", str(job_file(root, job, "bootstrap_lock")),
                ])
            elif job["kind"] == "resolve":
                if installed_inventory() != {"pip": PIP_VERSION}:
                    raise PolicyError("resolve-venv-is-not-fresh")
                report_path = contained(root, Path(job["report"]))
                if report_path.exists():
                    raise PolicyError("resolve-report-not-fresh")
                arguments = [
                    "install", "--dry-run", "--ignore-installed", "--only-binary=:all:",
                    "--no-compile", "--retries", "0", "--timeout", "60",
                    "--index-url", "https://pypi.org/simple", "--report", str(report_path),
                    "-r", str(job_file(root, job, "direct_input")),
                ]
                for constraint in job["constraints"]:
                    path = contained(root, Path(constraint))
                    regular(path)
                    arguments += ["-c", str(path)]
                outcome["pip_operation"] = pip_run(arguments, setup=True)
                regular(report_path)
            elif job["kind"] in {"install", "verify-setup"}:
                if installed_inventory() != {"pip": PIP_VERSION}:
                    raise PolicyError("install-venv-is-not-fresh")
                report_path = contained(root, Path(job["report"]))
                if report_path.exists():
                    raise PolicyError("install-report-not-fresh")
                arguments = [
                    "install", "--only-binary=:all:", "--no-compile", "--require-hashes",
                    "--retries", "0", "--timeout", "60", "--report", str(report_path),
                    "-r", str(job_file(root, job, "lock")),
                ]
                if job["kind"] == "verify-setup":
                    arguments += ["--index-url", "https://pypi.org/simple"]
                else:
                    arguments += ["--no-index", "--find-links", str(job_file(root, job, "wheelhouse", True))]
                outcome["pip_operation"] = pip_run(arguments, setup=job["kind"] == "verify-setup")
            elif job["kind"] == "check":
                outcome["pip_operation"] = pip_run(["check"])
                inventory = installed_inventory()
                expected = dict(job["versions"])
                expected["pip"] = PIP_VERSION
                if inventory != expected:
                    raise PolicyError("installed-inventory-does-not-match-lock")
                outcome["inventory"] = inventory
                if job.get("role") not in {"resolve", "replay"}:
                    raise PolicyError("invalid-synthetic-role")
                outcome["synthetic"] = native_smoke(root, profile, job["role"])
            else:
                raise PolicyError("unknown-worker-operation")
        if GUARD.unexpected:
            raise PolicyError("worker-caught-unexpected-audit-denial")
        outcome["status"] = "PASS"
        code = 0
    except BaseException as error:
        if isinstance(error, PipOperationError):
            outcome["pip_operation"] = error.operation
        outcome["failure_code"] = str(error) if isinstance(error, PolicyError) else "worker-" + type(error).__name__
    outcome["guard"] = GUARD.evidence()
    write_new(root, result_path, json_bytes(outcome))
    return code


def prepare_output(path):
    path = real_components(Path(path).absolute(), directory=True)
    temp_root = real_components(Path(os.environ.get("RUNNER_TEMP", tempfile.gettempdir())).absolute(), directory=True)
    contained(temp_root, path)
    if path.exists() or path.is_symlink():
        raise PolicyError("artifact-output-must-be-fresh")
    if not path.parent.is_dir():
        raise PolicyError("artifact-output-parent-missing")
    if hasattr(os, "getuid"):
        parent = path.parent.lstat()
        if parent.st_uid != os.getuid() or stat.S_IMODE(parent.st_mode) != 0o700:
            raise PolicyError("artifact-output-parent-not-private-owned")
    path.mkdir(mode=0o700)
    return path


def sanitized(value):
    if isinstance(value, dict):
        for key, item in value.items():
            if key.lower() in {"password", "token", "private_key", "secret", "credentials", "authorization"}:
                raise PolicyError("credential-shaped-evidence-key")
            sanitized(item)
    elif isinstance(value, list):
        for item in value:
            sanitized(item)
    elif isinstance(value, str):
        if value.startswith("https://"):
            public_url(value)
            return
        if re.search(r"(?:^|\s)/[A-Za-z0-9_.-]", value):
            raise PolicyError("private-absolute-path-in-evidence")
        if re.search(r"[A-Za-z]:[\\/]", value) or "file://" in value or "Bearer " in value:
            raise PolicyError("private-path-or-token-in-evidence")


def finalize_output(output, report, locks=None):
    if report["status"] == "PASS":
        if locks is None or set(locks) != set(PROFILES):
            raise PolicyError("successful-bundle-has-incomplete-locks")
        for profile in PROFILES:
            parse_lock(locks[profile].decode("utf-8"))
    elif locks is not None:
        raise PolicyError("failed-bundle-cannot-contain-locks")
    sanitized(report)
    write_new(output, output / "report.json", json_bytes(report))
    if locks is not None:
        for profile in PROFILES:
            write_new(output, output / LOCK_NAMES[profile], locks[profile])
    manifest = {path.name: digest(path) for path in sorted(output.iterdir())}
    write_new(output, output / "manifest.json", json_bytes(manifest))


def audit_output(args):
    flags()
    output = real_components(Path(args.output).absolute(), directory=True)
    expected_names = {"report.json", "manifest.json"}
    if args.success == "true":
        expected_names.update(LOCK_NAMES.values())
    if not output.is_dir() or {path.name for path in output.iterdir()} != expected_names:
        raise PolicyError("artifact-bundle-is-not-closed")
    if hasattr(os, "getuid") and (output.lstat().st_uid != os.getuid() or stat.S_IMODE(output.lstat().st_mode) != 0o700):
        raise PolicyError("artifact-root-not-private-owned")
    for path in output.iterdir():
        regular(path)
    manifest = read_json(output / "manifest.json")
    expected = {name: digest(output / name) for name in expected_names - {"manifest.json"}}
    if manifest != expected:
        raise PolicyError("artifact-digest-manifest-mismatch")
    report = read_json(output / "report.json")
    sanitized(report)
    if (report.get("status") != ("PASS" if args.success == "true" else "FAIL")
            or report.get("source_sha") != args.expected_sha or report.get("base_sha") != args.expected_base
            or report.get("mode") != args.mode or report.get("schema_version") != 1):
        raise PolicyError("artifact-report-identity-mismatch")
    if args.success == "true":
        profiles = {}
        for profile in PROFILES:
            profiles[profile] = parse_lock((output / LOCK_NAMES[profile]).read_text(encoding="utf-8"))
            if report.get("profiles", {}).get(profile, {}).get("lock_sha256") != digest(output / LOCK_NAMES[profile]):
                raise PolicyError("artifact-profile-lock-digest-mismatch")
        for profile in ("test", "dev"):
            if any(profiles[profile].get(name) != value for name, value in profiles["runtime"].items()):
                raise PolicyError("artifact-runtime-lock-divergence")
        if any(profiles["dev"].get(name) != value for name, value in profiles["test"].items()):
            raise PolicyError("artifact-test-lock-divergence")
        validate_success_evidence(report, profiles)
    return {"status": "PASS", "artifact_count": len(expected_names), "source_sha": args.expected_sha}


def validate_native_identity_evidence(value, helper_sha, executable_sha=None):
    target = {"os": "Ubuntu", "os_version": "24.04", "architecture": "x86_64",
              "python": "3.14.8", "implementation": "CPython", "glibc": "glibc 2.39",
              "soabi": "cpython-314-x86_64-linux-gnu",
              "provisioning_boundary": "Trusted pre-existing toolcache; source archive bytes not revalidated"}
    if not isinstance(value, dict) or any(value.get(key) != expected for key, expected in target.items()):
        raise PolicyError("artifact-native-target-identity-invalid")
    if (value.get("helper_sha256") != helper_sha or value.get("gil_enabled") is not True
            or "py_gil_disabled" not in value
            or not (value["py_gil_disabled"] is None
                    or type(value["py_gil_disabled"]) is int and value["py_gil_disabled"] == 0)):
        raise PolicyError("artifact-helper-or-gil-identity-invalid")
    build = value.get("python_build")
    if not isinstance(build, str) or not build.startswith("3.14.8 ") or len(build) > 512:
        raise PolicyError("artifact-python-build-identity-invalid")
    if value.get("python_executable_name") not in {"python", "python3", "python3.14"}:
        raise PolicyError("artifact-python-executable-name-invalid")
    actual_sha = value.get("python_executable_sha256")
    if not isinstance(actual_sha, str) or not HEX.fullmatch(actual_sha):
        raise PolicyError("artifact-python-executable-digest-invalid")
    if executable_sha is not None and actual_sha != executable_sha:
        raise PolicyError("artifact-worker-python-bytes-differ-from-controller")
    if (value.get("runner_image_os") != "ubuntu24"
            or not isinstance(value.get("runner_image_version"), str)
            or value["runner_image_version"] in {"", "NOT AVAILABLE"}):
        raise PolicyError("artifact-runner-image-identity-missing")
    return actual_sha


def validate_success_evidence(report, profiles):
    if set(report.get("profiles", {})) != set(PROFILES) or report.get("owned_scratch_cleanup") != "PASS":
        raise PolicyError("incomplete-profile-or-cleanup-evidence")
    helper_sha = digest(Path(__file__).absolute())
    executable_sha = validate_native_identity_evidence(report.get("identity"), helper_sha)
    mode = report.get("mode")
    if mode not in {"generate", "verify"}:
        raise PolicyError("artifact-operation-mode-invalid")
    expected_sources = {".github/lnx02/verify_env.py"}
    expected_sources.update("requirements/" + profile + ".in" for profile in PROFILES)
    expected_sources.update("requirements/windows-cp314-" + profile + ".lock" for profile in PROFILES)
    if mode == "verify":
        expected_sources.update("requirements/" + LOCK_NAMES[profile] for profile in PROFILES)
    sources = report.get("source_files_sha256")
    if (not isinstance(sources, dict) or set(sources) != expected_sources
            or any(not isinstance(value, str) or not HEX.fullmatch(value) for value in sources.values())
            or sources[".github/lnx02/verify_env.py"] != helper_sha):
        raise PolicyError("artifact-source-digest-manifest-invalid")
    bootstrap = report.get("bootstrap")
    if (not isinstance(bootstrap, dict) or bootstrap.get("name") != "pip"
            or bootstrap.get("version") != PIP_VERSION or bootstrap.get("filename") != PIP_FILENAME
            or bootstrap.get("sha256") != PIP_SHA256):
        raise PolicyError("artifact-bootstrap-pip-identity-invalid")
    bootstrap_url = public_url(bootstrap.get("source_url"))
    if (bootstrap_url.hostname != "files.pythonhosted.org"
            or unquote(bootstrap_url.path.rsplit("/", 1)[-1]) != PIP_FILENAME):
        raise PolicyError("artifact-bootstrap-pip-provenance-invalid")
    if (report.get("production") != "NOT VERIFIED"
            or report.get("linux_application_runtime") != "NOT VERIFIED"):
        raise PolicyError("artifact-overstates-runtime-verification")
    controller = report.get("controller_guard", {})
    if controller.get("registered_before_third_party") is not True or controller.get("unexpected_denials") != {}:
        raise PolicyError("artifact-controller-guard-evidence-invalid")
    stages = report.get("stages", [])
    if not stages or stages[0].get("kind") != "preflight":
        raise PolicyError("native-preflight-evidence-missing")
    expected_sequence = [("preflight", None, None)]
    for profile in PROFILES:
        expected_sequence.extend(("bootstrap", profile, role) for role in ("resolve", "replay"))
        if report["mode"] == "generate":
            expected_sequence.append(("resolve", profile, "resolve"))
        for role in ("resolve", "replay"):
            install_kind = "verify-setup" if report["mode"] == "verify" and role == "resolve" else "install"
            expected_sequence += [(install_kind, profile, role), ("check", profile, role)]
    if [(stage.get("kind"), stage.get("profile"), stage.get("role")) for stage in stages] != expected_sequence:
        raise PolicyError("artifact-profile-stage-sequence-incomplete")
    for stage in stages:
        validate_native_identity_evidence(stage.get("identity"), helper_sha, executable_sha)
        guard = stage.get("guard", {})
        probe = stage.get("negative_probes", {})
        if (stage.get("status") != "PASS" or stage.get("exit_code") != 0
                or stage.get("helper_sha256") != helper_sha
                or type(guard.get("pid")) is not int or guard["pid"] <= 0
                or guard.get("registered_before_third_party") is not True
                or guard.get("unexpected_denials") != {}
                or guard.get("expected_denials") != {"socket.getaddrinfo": 1, "subprocess.Popen": 1}
                or probe.get("pid") != guard.get("pid")
                or probe.get("dns_denied_before_resolver") is not True
                or probe.get("child_denied_before_launch") is not True
                or stage.get("parent_namespace_unchanged") is not True):
            raise PolicyError("stage-guard-or-exit-evidence-invalid")
        if stage["kind"] != "preflight":
            pip = stage.get("pip")
            if (not isinstance(pip, dict) or pip.get("version") != PIP_VERSION
                    or pip.get("verified_bootstrap_sha256") != PIP_SHA256
                    or pip.get("fixed_user_agent_without_child_probes") is not True):
                raise PolicyError("artifact-worker-pip-identity-invalid")
            if (stage.get("pip_operation", {}).get("pid") != guard.get("pid")
                    or stage["pip_operation"].get("exit_code") != 0
                    or stage["pip_operation"].get("runpy_same_pid") is not True
                    or probe.get("requests_fixture") is not True):
                raise PolicyError("actual-pip-pid-guard-evidence-invalid")
        if stage["kind"] not in {"resolve", "verify-setup"}:
            namespace = stage.get("namespace", {})
            parent_ids, child_ids = namespace.get("parent_ids"), namespace.get("child_ids")
            parent_ns = namespace.get("parent_namespace_inodes", {})
            child_ns = namespace.get("child_namespace_inodes", {})
            if (namespace.get("distinct_user_namespace") is not True
                    or namespace.get("distinct_network_namespace") is not True
                    or namespace.get("uid_preserved") is not True
                    or namespace.get("gid_preserved") is not True
                    or namespace.get("cap_eff") != 0
                    or namespace.get("interfaces") != [{"name": "lo", "up": False}]
                    or namespace.get("address_count") != 0 or namespace.get("route_count") != 0
                    or namespace.get("kernel_probe_errno") != errno.ENETUNREACH
                    or namespace.get("kernel_probe_reached_os") is not True
                    or not isinstance(parent_ids, list) or len(parent_ids) != 3
                    or any(type(value) is not int or value < 0 for value in parent_ids)
                    or child_ids != parent_ids or set(parent_ns) != {"user", "net"}
                    or set(child_ns) != {"user", "net"}
                    or any(type(parent_ns[key]) is not int or type(child_ns[key]) is not int
                           or parent_ns[key] <= 0 or child_ns[key] <= 0
                           or parent_ns[key] == child_ns[key] for key in ("user", "net"))):
                raise PolicyError("native-namespace-evidence-invalid")
    wheel_records = {}
    for record in report.get("wheels", []):
        name = record.get("name")
        if name in wheel_records or wheel_name(record.get("filename")) != (name, record.get("version")):
            raise PolicyError("artifact-wheel-record-invalid")
        if public_url(record.get("source_url")).hostname != "files.pythonhosted.org":
            raise PolicyError("artifact-wheel-source-invalid")
        wheel_records[name] = (record["version"], record.get("sha256"))
    if wheel_records != profiles["dev"]:
        raise PolicyError("artifact-wheel-inventory-does-not-match-dev-lock")
    if report.get("wheelhouse_digest_manifest") != {record["filename"]: record["sha256"] for record in report["wheels"]}:
        raise PolicyError("artifact-wheelhouse-digest-manifest-mismatch")
    for profile in PROFILES:
        evidence = report["profiles"][profile]
        if mode == "verify" and sources["requirements/" + LOCK_NAMES[profile]] != evidence.get("lock_sha256"):
            raise PolicyError("artifact-verified-source-lock-digest-mismatch")
        if evidence.get("fresh_venvs") != 2 or evidence.get("wheel_count") != len(profiles[profile]):
            raise PolicyError("artifact-profile-venv-or-wheel-count-invalid")
        for role in ("resolve", "replay"):
            matches = [stage for stage in stages if stage.get("kind") == "check"
                       and stage.get("profile") == profile and stage.get("role") == role]
            if len(matches) != 1 or evidence.get(role) != matches[0]:
                raise PolicyError("artifact-profile-check-stage-missing")
            inventory = {name: value[0] for name, value in profiles[profile].items()}
            inventory["pip"] = PIP_VERSION
            if matches[0].get("inventory") != inventory:
                raise PolicyError("artifact-installed-inventory-mismatch")
            synthetic = matches[0].get("synthetic", {})
            if any(synthetic.get(name) != "PASS" for name in ("numpy_synthetic", "ta_sma_synthetic", "csv_synthetic", "xlsx_synthetic")):
                raise PolicyError("artifact-native-synthetic-evidence-incomplete")
    if len([stage for stage in stages if stage.get("kind") == "bootstrap"]) != 6:
        raise PolicyError("artifact-six-fresh-bootstrap-evidence-missing")


def pipeline(args):
    # Repeat grants before directories, venvs, package metadata or downloads.
    validate_grant(os.environ, args.mode, args.expected_sha, args.expected_base)
    output = prepare_output(args.output)
    report = {"schema_version": 1, "status": "FAIL", "mode": args.mode,
              "source_sha": args.expected_sha, "base_sha": args.expected_base,
              "stages": [], "profiles": {}, "linux_application_runtime": "NOT VERIFIED",
              "production": "NOT VERIFIED", "network_setup_hosts": sorted(PUBLIC_HOSTS)}
    root = nonce = None
    locks = {}
    sequence = 0
    sources = None
    started = time.monotonic()
    try:
        repo = real_components(Path(args.repo_root).absolute(), directory=True)
        report["identity"] = identity()
        if sys.prefix != sys.base_prefix:
            raise PolicyError("controller-must-use-base-interpreter")
        source_identity(repo, args.expected_sha, args.expected_base)
        sources = source_manifest(repo, args.mode)
        if sources[".github/lnx02/verify_env.py"] != report["identity"]["helper_sha256"]:
            raise PolicyError("executing-helper-is-not-approved-source")
        report["source_files_sha256"] = sources
        expected = expected_profiles(repo)
        temp_root = Path(os.environ.get("RUNNER_TEMP", tempfile.gettempdir())).absolute()
        root, nonce = new_scratch(temp_root)
        report["scratch_id"] = root.name
        admitted = {"mode": args.mode, "sha": args.expected_sha, "base": args.expected_base}

        def stage(python, kind, fields=None, offline=True):
            nonlocal sequence
            sequence += 1
            fields = dict(fields or {})
            fields.update(admitted)
            try:
                outcome = worker_launch(root, nonce, python, kind, fields, sequence, offline)
            except StageError as error:
                report["stages"].append(error.outcome)
                raise
            report["stages"].append(outcome)
            return outcome

        stage(sys.executable, "preflight")
        # This is the first public package network action, after real preflight.
        pip_record = public_wheel(release_metadata("pip", PIP_VERSION), "pip", PIP_VERSION,
                                  sha256=PIP_SHA256, filename=PIP_FILENAME)
        fetch_wheel(root, root / "bootstrap", pip_record)
        bootstrap_lock = root / "bootstrap" / "pip.lock"
        write_new(root, bootstrap_lock, ("pip==" + PIP_VERSION + " --hash=sha256:" + PIP_SHA256 + "\n").encode("ascii"))
        report["bootstrap"] = pip_record
        confirm_windows_markers()
        report["inactive_windows_dependencies"] = {"colorama": "Windows-only build/pytest markers", "pywin32": "Windows-only jupyter-core marker"}
        wheel_records, resolved = {}, {}
        runtime_constraints = None
        for profile in PROFILES:
            resolve_dir = root / (profile + "-resolve")
            replay_dir = root / (profile + "-replay")
            for role, environment in (("resolve", resolve_dir), ("replay", replay_dir)):
                contained(root, environment)
                if environment.exists():
                    raise PolicyError("venv-destination-not-fresh")
                venv.EnvBuilder(with_pip=False, symlinks=False).create(environment)
                # CPython's POSIX EnvBuilder creates this directory alias even
                # when executable copies were requested. Nothing here needs it.
                lib64 = environment / "lib64"
                if lib64.is_symlink():
                    if os.readlink(lib64) != "lib" or not (environment / "lib").is_dir():
                        raise PolicyError("unexpected-venv-directory-alias")
                    lib64.unlink()
                elif lib64.exists():
                    raise PolicyError("unexpected-venv-lib64-entry")
                python = environment / "bin" / "python3.14"
                regular(python)
                if digest(python) != report["identity"]["python_executable_sha256"]:
                    raise PolicyError("venv-python-bytes-differ-from-base")
                stage(python, "bootstrap", {"profile": profile, "role": role, "bootstrap_lock": str(bootstrap_lock)})
            resolve_python = resolve_dir / "bin" / "python3.14"
            replay_python = replay_dir / "bin" / "python3.14"
            lock_path = root / "locks" / LOCK_NAMES[profile]
            setup_report = root / "jobs" / (profile + "-public-setup.json")
            if args.mode == "generate":
                seed = root / "constraints" / (profile + "-windows-versions.txt")
                pins = {name: (version, "") for name, version in expected[profile].items()}
                write_new(root, seed, constraint_text(pins).encode("ascii"))
                direct = root / "constraints" / (profile + ".in")
                direct_pins = {name: (version, "") for name, version in direct_requirements(repo, profile).items()}
                write_new(root, direct, constraint_text(direct_pins).encode("ascii"))
                constraints = [str(seed)]
                if runtime_constraints is not None:
                    constraints.append(str(runtime_constraints))
                stage(resolve_python, "resolve", {"profile": profile, "role": "resolve", "report": str(setup_report),
                                                   "direct_input": str(direct), "constraints": constraints}, offline=False)
                records, selected = parse_report(read_json(setup_report), expected[profile])
                for name, candidate in selected.items():
                    metadata = release_metadata(name, candidate["version"])
                    confirmed = public_wheel(metadata, name, candidate["version"], candidate["sha256"], candidate["filename"])
                    if confirmed["source_url"] != candidate["source_url"]:
                        raise PolicyError("pip-report-and-pypi-source-url-differ")
                    selected[name] = confirmed
                payload = lock_text(records).encode("utf-8")
            else:
                source_lock = repo / "requirements" / LOCK_NAMES[profile]
                payload = source_lock.read_bytes()
                records = parse_lock(payload.decode("utf-8"))
                validate_versions(records, expected[profile])
                selected = {}
                for name, (version, sha256) in records.items():
                    selected[name] = public_wheel(release_metadata(name, version), name, version, sha256=sha256)
            for prior in ("runtime", "test"):
                if prior in resolved and any(records.get(name) != value for name, value in resolved[prior].items()):
                    raise PolicyError("profile-runtime-or-test-wheel-divergence")
            for name, record in selected.items():
                if name in wheel_records and wheel_records[name] != record:
                    raise PolicyError("profile-wheel-artifact-divergence")
                fetch_wheel(root, root / "wheelhouse", record)
                wheel_records[name] = record
            write_new(root, lock_path, payload)
            resolved[profile] = records
            locks[profile] = payload
            expected_local = records
            for role, python in (("resolve", resolve_python), ("replay", replay_python)):
                install_report = root / "jobs" / (profile + "-" + role + "-install.json")
                install_kind = "verify-setup" if args.mode == "verify" and role == "resolve" else "install"
                stage(python, install_kind, {"profile": profile, "role": role, "lock": str(lock_path),
                                             "wheelhouse": str(root / "wheelhouse"), "report": str(install_report)},
                      offline=install_kind != "verify-setup")
                installed, installed_wheels = parse_report(read_json(install_report), expected[profile], local=install_kind == "install")
                if installed != expected_local:
                    raise PolicyError("install-report-does-not-match-lock")
                for name, item in installed_wheels.items():
                    if item["filename"] != selected[name]["filename"]:
                        raise PolicyError("installed-wheel-filename-drift")
                    if install_kind == "install":
                        raw_url = next(entry["download_info"]["url"] for entry in read_json(install_report)["install"]
                                       if canonical(entry["metadata"]["name"]) == name)
                        local_path = contained(root, Path(urllib.request.url2pathname(urlsplit(raw_url).path)))
                        if local_path.parent != root / "wheelhouse":
                            raise PolicyError("offline-replay-read-outside-wheelhouse")
                check = stage(python, "check", {"profile": profile, "role": role, "versions": expected[profile]})
                report["profiles"].setdefault(profile, {})[role] = check
            report["profiles"][profile].update({"wheel_count": len(records),
                                                "lock_sha256": hashlib.sha256(payload).hexdigest(),
                                                "fresh_venvs": 2, "dependency_environment_checks": "PASS"})
            if profile == "runtime":
                runtime_constraints = root / "constraints" / "runtime-verified-versions.txt"
                write_new(root, runtime_constraints, constraint_text(records).encode("ascii"))
        report["wheels"] = [wheel_records[name] for name in sorted(wheel_records)]
        report["wheelhouse_digest_manifest"] = {item["filename"]: item["sha256"] for item in report["wheels"]}
        actual_wheels = {path.name: digest(path) for path in (root / "wheelhouse").iterdir()}
        if actual_wheels != report["wheelhouse_digest_manifest"]:
            raise PolicyError("wheelhouse-is-not-closed-or-unchanged")
        source_identity(repo, args.expected_sha, args.expected_base)
        if source_manifest(repo, args.mode) != sources:
            raise PolicyError("source-input-bytes-changed")
        if GUARD.unexpected:
            raise PolicyError("controller-caught-unexpected-audit-denial")
        report["status"] = "PASS"
    except BaseException as error:
        report["failure_code"] = str(error) if isinstance(error, PolicyError) else "controller-" + type(error).__name__
    finally:
        if root is not None:
            try:
                cleanup(root, nonce)
                report["owned_scratch_cleanup"] = "PASS"
            except BaseException as error:
                report["status"] = "FAIL"
                report["cleanup_failure_code"] = str(error) if isinstance(error, PolicyError) else type(error).__name__
                report["owned_scratch_cleanup"] = "FAIL_PRESERVED"
        report["duration_seconds"] = round(time.monotonic() - started, 3)
        report["controller_guard"] = GUARD.evidence()
        finalize_output(output, report, locks if report["status"] == "PASS" else None)
    return report, 0 if report["status"] == "PASS" else 1


def standalone_identity(namespace_required=False):
    value = identity()
    value["status"] = "PASS"
    value["guard"] = GUARD.evidence()
    if namespace_required:
        root, nonce = new_scratch(Path(os.environ.get("RUNNER_TEMP", tempfile.gettempdir())).absolute())
        try:
            value["namespace_preflight"] = worker_launch(root, nonce, sys.executable, "preflight", {}, 1)
        finally:
            cleanup(root, nonce)
    return value


def self_test(repo):
    """Portable refusal tests. These never establish native Linux acceptance."""
    flags()
    results = []

    def check(name, function):
        try:
            function()
            results.append({"case": name, "status": "PASS"})
        except BaseException as error:
            results.append({"case": name, "status": "FAIL", "failure_code":
                            str(error) if isinstance(error, PolicyError) else type(error).__name__})

    def reject(name, function):
        def run():
            try:
                function()
            except PolicyError:
                return
            raise PolicyError("expected-refusal-did-not-occur")
        check(name, run)

    def expect(condition):
        if not condition:
            raise PolicyError("self-test-assertion-failed")

    check("ipv4-proc-empty", lambda: parse_ipv4_no_routes(""))
    ipv4_header = "Iface\tDestination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT\n"
    check("ipv4-proc-header-only", lambda: parse_ipv4_no_routes(ipv4_header))
    reject("ipv4-proc-route-record", lambda: parse_ipv4_no_routes(ipv4_header + "lo 00000000\n"))
    reject("ipv4-proc-invalid-header", lambda: parse_ipv4_no_routes("unexpected\n"))
    reject("ipv4-proc-refuse-size-limit", lambda: parse_ipv4_no_routes(" " * 513))
    null_fields = ["0" * 32, "00", "0" * 32, "00", "0" * 32,
                   "ffffffff", "00000001", "00000000", "00200200", "lo"]
    null_row = " ".join(null_fields) + "\n"
    check("ipv6-proc-empty", lambda: expect(parse_ipv6_null_routes("") == 0))
    check("ipv6-proc-null-sentinels", lambda: expect(parse_ipv6_null_routes(null_row * 2) == 2))
    for index, value in ((0, "1" + "0" * 31), (1, "80"), (2, "1" + "0" * 31),
                         (3, "80"), (4, "1" + "0" * 31), (5, "00000001"),
                         (6, "bad"), (7, "0000000G"), (8, "00000001"), (9, "eth0")):
        changed = list(null_fields)
        changed[index] = value
        reject("ipv6-proc-refuse-field-" + str(index),
               lambda changed=changed: parse_ipv6_null_routes(" ".join(changed)))
    reject("ipv6-proc-refuse-blank-row", lambda: parse_ipv6_null_routes("\n"))
    reject("ipv6-proc-refuse-extra-field", lambda: parse_ipv6_null_routes(null_row.rstrip() + " extra"))
    reject("ipv6-proc-refuse-row-limit", lambda: parse_ipv6_null_routes(null_row * 17))
    reject("ipv6-proc-refuse-size-limit", lambda: parse_ipv6_null_routes(" " * 4097))

    fixture = {"a": ("1.0", "0" * 64), "b": ("2.0.post1", "1" * 64)}
    valid = lock_text(fixture)
    check("canonical-lock-roundtrip", lambda: expect(parse_lock(valid) == fixture))
    check("version-only-constraints", lambda: expect(constraint_text(fixture) == "a==1.0\nb==2.0.post1\n"))
    for name, bad in {
        "duplicate-lock": valid + "a==1.0 --hash=sha256:" + "0" * 64 + "\n",
        "unsorted-lock": "b==1.0 --hash=sha256:" + "0" * 64 + "\na==1.0 --hash=sha256:" + "0" * 64,
        "unlocked-range": "a>=1.0",
        "missing-hash": "a==1.0",
        "bad-sha256": "a==1.0 --hash=sha256:" + "G" * 64,
        "additional-index": "--extra-index-url https://example.invalid/simple",
        "editable": "-e a==1.0",
        "included-lock": "-r another.lock",
        "marker-in-platform-lock": "a==1.0; sys_platform=='linux' --hash=sha256:" + "0" * 64,
        "multiple-hashes": "a==1.0 --hash=sha256:" + "0" * 64 + " --hash=sha256:" + "1" * 64,
        "empty-lock": "# no records\n",
    }.items():
        reject(name, lambda bad=bad: parse_lock(bad))
    for filename in ("a-1.0-py3-none-any.whl", "a-1.0-cp314-cp314-manylinux_2_28_x86_64.whl",
                     "a-1.0-cp310-abi3-manylinux2014_x86_64.whl", "a-1.0-py2.py3-none-any.whl"):
        check("target-tag-" + filename, lambda filename=filename: expect(wheel_name(filename) == ("a", "1.0")))
    for filename in ("a-1.0.tar.gz", "a-1.0-cp314-cp314-win_amd64.whl", "a-1.0-cp314t-cp314t-manylinux_2_28_x86_64.whl",
                     "a-1.0-cp314-cp314-musllinux_1_2_x86_64.whl", "a-1.0-cp314-cp314-manylinux_2_28_aarch64.whl",
                     "a-1.0-cp315-abi3-manylinux_2_28_x86_64.whl", "a-1.0-cp314-cp314-manylinux_2_40_x86_64.whl",
                     "../a-1.0-py3-none-any.whl"):
        reject("refuse-tag-" + filename.replace("/", "-"), lambda filename=filename: wheel_name(filename))
    for url in ("http://pypi.org/pypi/a/1.0/json", "https://example.invalid/a.whl", "https://pypi.org:80/pypi/a/1.0/json",
                "https://user:password@pypi.org/pypi/a/1.0/json", "https://pypi.org/a?token=fake",
                "https://pypi.org/a#fragment", "https://pypi.org:bad/a"):
        reject("refuse-public-url-" + str(len(results)), lambda url=url: public_url(url))
    check("public-pypi-https", lambda: public_url("https://pypi.org/pypi/a/1.0/json"))
    public_report = {
        "version": "1", "pip_version": PIP_VERSION,
        "environment": {"implementation_name": "cpython", "platform_python_implementation": "CPython",
                        "sys_platform": "linux", "platform_system": "Linux", "platform_machine": "x86_64",
                        "python_full_version": "3.14.8"},
        "install": [{"is_direct": False, "is_yanked": False, "metadata": {"name": "a", "version": "1.0"},
                     "download_info": {"url": "https://files.pythonhosted.org/packages/a/a-1.0-py3-none-any.whl",
                                       "archive_info": {"hashes": {"sha256": "0" * 64}}}}],
    }
    check("native-report-records", lambda: expect(parse_report(public_report, {"a": "1.0"})[0] == {"a": ("1.0", "0" * 64)}))
    variants = []
    for key, value in (("version", "2"), ("pip_version", "unknown"), ("install", [])):
        bad = copy.deepcopy(public_report)
        bad[key] = value
        variants.append(("report-" + key, bad))
    bad = copy.deepcopy(public_report)
    bad["environment"]["sys_platform"] = "win32"
    variants.append(("report-windows-platform", bad))
    for key, value in (("is_yanked", True), ("is_direct", True)):
        bad = copy.deepcopy(public_report)
        bad["install"][0][key] = value
        variants.append(("report-" + key, bad))
    bad = copy.deepcopy(public_report)
    bad["install"].append(copy.deepcopy(bad["install"][0]))
    variants.append(("report-duplicate-package", bad))
    bad = copy.deepcopy(public_report)
    bad["install"][0]["download_info"]["archive_info"] = {}
    variants.append(("report-missing-sha", bad))
    for name, bad in variants:
        reject(name, lambda bad=bad: parse_report(bad, {"a": "1.0"}))
    reject("unexpected-linux-transitive-package-is-blocker", lambda: parse_report(public_report, {"a": "1.0", "b": "2.0"}))

    sha, base = "0" * 40, "1" * 40
    env = {"MDS_LNX02_SETUP_APPROVED": "true", "MDS_LNX02_APPROVED_SHA": sha,
           "MDS_LNX02_APPROVED_BASE": base, "MDS_LNX02_OPERATION": "generate",
           "GITHUB_EVENT_NAME": "push", "GITHUB_REPOSITORY": "foykes/moex-dataset",
           "GITHUB_REF": "refs/heads/codex/lnx-02", "GITHUB_SHA": sha,
           "GITHUB_ACTOR": "foykes", "GITHUB_TRIGGERING_ACTOR": "foykes",
           "TASK_EVENT": "push", "TASK_EVENT_SHA": sha, "TASK_EVENT_REF": "refs/heads/codex/lnx-02",
           "TASK_REPOSITORY": "foykes/moex-dataset", "TASK_ACTOR": "foykes",
           "TASK_TRIGGERING_ACTOR": "foykes", "TASK_RUNNER_ENVIRONMENT": "github-hosted", "TASK_DELETED": "false"}
    check("exact-push-grant", lambda: validate_grant(env, "generate", sha, base))
    for key in GRANT_NAMES:
        bad = dict(env)
        bad.pop(key)
        reject("missing-" + key, lambda bad=bad: validate_grant(bad, "generate", sha, base))
    for key, value in (("GITHUB_EVENT_NAME", "pull_request"), ("GITHUB_REPOSITORY", "fork/moex-dataset"),
                       ("GITHUB_REF", "refs/heads/main"), ("GITHUB_ACTOR", "other"), ("GITHUB_TRIGGERING_ACTOR", "other"),
                       ("GITHUB_SHA", "2" * 40), ("TASK_RUNNER_ENVIRONMENT", "self-hosted"), ("TASK_DELETED", "true"),
                       ("MDS_LNX02_SETUP_APPROVED", "TRUE"), ("MDS_LNX02_APPROVED_BASE", "2" * 40)):
        bad = dict(env)
        bad[key] = value
        reject("refused-grant-" + key, lambda bad=bad: validate_grant(bad, "generate", sha, base))
    dispatch = dict(env)
    dispatch.update({"MDS_LNX02_APPROVED_BASE": sha, "MDS_LNX02_OPERATION": "verify",
                     "GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_REF": "refs/heads/main",
                     "TASK_EVENT": "workflow_dispatch", "TASK_EVENT_REF": "refs/heads/main",
                     "TASK_DISPATCH_ALLOW": "true", "TASK_DISPATCH_SHA": sha})
    check("exact-current-main-dispatch", lambda: validate_grant(dispatch, "verify", sha, sha))
    for key, value in (("TASK_DISPATCH_ALLOW", "false"), ("TASK_DISPATCH_SHA", "2" * 40),
                       ("GITHUB_REF", "refs/heads/codex/lnx-02"), ("MDS_LNX02_OPERATION", "generate")):
        bad = dict(dispatch)
        bad[key] = value
        reject("refused-dispatch-" + key, lambda bad=bad: validate_grant(bad, "verify", sha, sha))

    def guard_sticky():
        guard = AuditGuard()
        for event, arguments in (("socket.getaddrinfo", ("other.invalid", 443)), ("subprocess.Popen", ("python", ["python"], None, {}))):
            try:
                guard(event, arguments)
            except GuardDenied:
                pass
        expect(guard.unexpected == {"socket.getaddrinfo": 1, "subprocess.Popen": 1})
    check("caught-denials-remain-sticky", guard_sticky)

    def https_only():
        guard = AuditGuard()
        guard.phase = "setup"
        guard.public_addresses.add("151.101.0.223")
        guard("socket.connect", (None, ("151.101.0.223", 443)))
        try:
            guard("socket.connect", (None, ("151.101.0.223", 80)))
        except GuardDenied:
            expect(guard.unexpected == {"socket.connect": 1})
            return
        raise PolicyError("http-port-was-not-denied")
    check("setup-public-ip-still-requires-https-port443", https_only)
    check("actual-same-process-network-and-child-denial", negative_probes)
    check("urllib3-import-ipv6-probe-disabled", lambda: expect(socket.has_ipv6 is False))
    check("existing-profile-inputs-and-windows-closures", lambda: expect(
        [len(expected_profiles(repo)[profile]) for profile in PROFILES] == [42, 46, 82]))
    reject("sanitizer-private-posix-path", lambda: sanitized({"value": "/private/example"}))
    reject("sanitizer-private-windows-path", lambda: sanitized({"value": "C:\\private\\example"}))
    reject("sanitizer-credential-shaped-field", lambda: sanitized({"token": "fake"}))

    root, nonce = new_scratch(Path(tempfile.gettempdir()).absolute())
    try:
        sentinel = root / "sentinel.bin"
        write_new(root, sentinel, b"unchanged sentinel")
        reject("refuse-replacing-owned-file", lambda: write_new(root, sentinel, b"changed"))
        reject("refuse-output-root-traversal", lambda: contained(root, root / ".." / "foreign"))
        reject("refuse-output-outside-root", lambda: contained(root, root.parent / "foreign"))
        hardlink = root / "hardlink.bin"
        os.link(sentinel, hardlink)
        try:
            reject("refuse-hardlinked-read", lambda: regular(hardlink))
            reject("refuse-hardlinked-write", lambda: write_new(root, hardlink, b"changed"))
            check("hardlink-sentinel-preserved", lambda: expect(sentinel.read_bytes() == b"unchanged sentinel"))
        finally:
            hardlink.unlink()
        alias = root / "alias"
        try:
            os.symlink(root / "fixtures", alias, target_is_directory=True)
        except OSError:
            results.append({"case": "real-directory-symlink-refusal", "status": "NOT AVAILABLE"})
        else:
            try:
                reject("real-directory-symlink-refusal", lambda: write_new(root, alias / "foreign.txt", b"changed"))
            finally:
                alias.unlink()
        check("sentinel-still-preserved", lambda: expect(digest(sentinel) == hashlib.sha256(b"unchanged sentinel").hexdigest()))
        wheel = root / "a-1.0-py3-none-any.whl"
        with zipfile.ZipFile(wheel, "w") as archive:
            archive.writestr("a-1.0.dist-info/METADATA", "Name: a\nVersion: 1.0\n")
            archive.writestr("a.py", "value = 1\n")
        record = {"name": "a", "version": "1.0", "filename": wheel.name, "sha256": digest(wheel)}
        check("actual-wheel-bytes-and-embedded-metadata", lambda: validate_wheel(wheel, record))
        with wheel.open("ab") as handle:
            handle.write(b"tamper")
        reject("wheel-byte-tampering-refused", lambda: validate_wheel(wheel, record))
        bad_wheel = root / "b-1.0-py3-none-any.whl"
        with zipfile.ZipFile(bad_wheel, "w") as archive:
            archive.writestr("../foreign.py", "changed")
            archive.writestr("b-1.0.dist-info/METADATA", "Name: b\nVersion: 1.0\n")
        bad_record = {"name": "b", "version": "1.0", "filename": bad_wheel.name, "sha256": digest(bad_wheel)}
        reject("wheel-zip-traversal-refused", lambda: validate_wheel(bad_wheel, bad_record))
        # A complete synthetic evidence bundle exercises the success audit.
        # These are fabricated fixtures, never native execution evidence.
        profile_versions = expected_profiles(repo)
        profile_locks = {profile: {name: (version, "0" * 64) for name, version in versions.items()}
                         for profile, versions in profile_versions.items()}
        helper_sha = digest(Path(__file__).absolute())
        namespace_fixture = {"distinct_user_namespace": True, "distinct_network_namespace": True,
                             "uid_preserved": True, "gid_preserved": True, "cap_eff": 0,
                             "parent_ids": [1001, 1001, 1001], "child_ids": [1001, 1001, 1001],
                             "parent_namespace_inodes": {"user": 10, "net": 20},
                             "child_namespace_inodes": {"user": 11, "net": 21},
                             "interfaces": [{"name": "lo", "up": False}], "address_count": 0,
                             "route_count": 0, "kernel_probe_errno": errno.ENETUNREACH,
                             "kernel_probe_reached_os": True}

        identity_fixture = {"os": "Ubuntu", "os_version": "24.04", "architecture": "x86_64",
                            "python": "3.14.8", "implementation": "CPython", "gil_enabled": True,
                            "glibc": "glibc 2.39", "python_build": "3.14.8 (fixture build) [GCC 13.3.0]",
                            "soabi": "cpython-314-x86_64-linux-gnu", "py_gil_disabled": 0,
                            "runner_image_os": "ubuntu24", "runner_image_version": "20261012.1",
                            "python_executable_name": "python3.14", "python_executable_sha256": "2" * 64,
                            "helper_sha256": helper_sha,
                            "provisioning_boundary": "Trusted pre-existing toolcache; source archive bytes not revalidated"}
        bootstrap_fixture = {"name": "pip", "version": PIP_VERSION, "filename": PIP_FILENAME,
                             "sha256": PIP_SHA256,
                             "source_url": "https://files.pythonhosted.org/packages/fixture/" + PIP_FILENAME}

        def stage_fixture(kind, profile=None, role=None):
            stage = {"kind": kind, "helper_sha256": helper_sha, "status": "PASS", "exit_code": 0,
                     "identity": copy.deepcopy(identity_fixture),
                     "parent_namespace_unchanged": True,
                     "guard": {"pid": 100, "registered_before_third_party": True, "unexpected_denials": {},
                               "expected_denials": {"socket.getaddrinfo": 1, "subprocess.Popen": 1}},
                     "negative_probes": {"pid": 100, "dns_denied_before_resolver": True,
                                         "child_denied_before_launch": True, "requests_fixture": kind != "preflight"}}
            if profile is not None:
                stage.update({"profile": profile, "role": role,
                              "pip": {"version": PIP_VERSION, "verified_bootstrap_sha256": PIP_SHA256,
                                      "fixed_user_agent_without_child_probes": True},
                              "pip_operation": {"pid": 100, "exit_code": 0, "runpy_same_pid": True}})
            if kind not in {"resolve", "verify-setup"}:
                stage["namespace"] = copy.deepcopy(namespace_fixture)
            if kind == "check":
                stage["inventory"] = {**profile_versions[profile], "pip": PIP_VERSION}
                stage["synthetic"] = {name: "PASS" for name in
                                      ("numpy_synthetic", "ta_sma_synthetic", "csv_synthetic", "xlsx_synthetic")}
            return stage

        def success_fixture(mode):
            report = {"schema_version": 1, "status": "PASS", "source_sha": sha, "base_sha": base,
                      "mode": mode, "identity": copy.deepcopy(identity_fixture), "owned_scratch_cleanup": "PASS",
                      "bootstrap": copy.deepcopy(bootstrap_fixture),
                      "source_files_sha256": source_manifest(repo, "generate"),
                      "production": "NOT VERIFIED", "linux_application_runtime": "NOT VERIFIED",
                      "controller_guard": {"registered_before_third_party": True, "unexpected_denials": {}},
                      "profiles": {}, "stages": [stage_fixture("preflight")], "wheels": []}
            for profile in PROFILES:
                report["stages"] += [stage_fixture("bootstrap", profile, role) for role in ("resolve", "replay")]
                if mode == "generate":
                    report["stages"].append(stage_fixture("resolve", profile, "resolve"))
                evidence = {"fresh_venvs": 2, "wheel_count": len(profile_locks[profile]),
                            "lock_sha256": hashlib.sha256(lock_text(profile_locks[profile]).encode("utf-8")).hexdigest()}
                for role in ("resolve", "replay"):
                    kind = "verify-setup" if mode == "verify" and role == "resolve" else "install"
                    report["stages"].append(stage_fixture(kind, profile, role))
                    stage = stage_fixture("check", profile, role)
                    report["stages"].append(stage)
                    evidence[role] = stage
                report["profiles"][profile] = evidence
                if mode == "verify":
                    report["source_files_sha256"]["requirements/" + LOCK_NAMES[profile]] = evidence["lock_sha256"]
            for name, version in profile_versions["dev"].items():
                filename = name.replace("-", "_") + "-" + version + "-py3-none-any.whl"
                report["wheels"].append({"name": name, "version": version, "filename": filename,
                                         "sha256": "0" * 64,
                                         "source_url": "https://files.pythonhosted.org/packages/fixture/" + filename})
            report["wheelhouse_digest_manifest"] = {item["filename"]: item["sha256"] for item in report["wheels"]}
            return report

        for mode in ("generate", "verify"):
            bundle = root / (mode + "-success-artifact")
            bundle.mkdir(mode=0o700)
            report = success_fixture(mode)
            payloads = {profile: lock_text(profile_locks[profile]).encode("utf-8") for profile in PROFILES}
            finalize_output(bundle, report, payloads)
            success_args = argparse.Namespace(output=str(bundle), expected_sha=sha, expected_base=base,
                                              mode=mode, success="true")
            check("complete-" + mode + "-five-file-success-fixture-accepted",
                  lambda success_args=success_args: expect(audit_output(success_args)["artifact_count"] == 5))
        success_report = success_fixture("generate")
        for field in identity_fixture:
            variant = copy.deepcopy(success_report)
            variant["identity"].pop(field)
            reject("success-audit-refuses-missing-controller-identity-" + field,
                   lambda variant=variant: validate_success_evidence(variant, profile_locks))
        for field in bootstrap_fixture:
            variant = copy.deepcopy(success_report)
            variant["bootstrap"].pop(field)
            reject("success-audit-refuses-missing-bootstrap-" + field,
                   lambda variant=variant: validate_success_evidence(variant, profile_locks))
        for field in success_report["stages"][5]["pip"]:
            variant = copy.deepcopy(success_report)
            variant["stages"][5]["pip"].pop(field)
            reject("success-audit-refuses-missing-worker-pip-" + field,
                   lambda variant=variant: validate_success_evidence(variant, profile_locks))
        for name, mutate in (
            ("missing-controller-identity", lambda item: item.pop("identity")),
            ("wrong-controller-python", lambda item: item["identity"].update({"python": "3.14.7"})),
            ("wrong-controller-os", lambda item: item["identity"].update({"os_version": "22.04"})),
            ("free-threaded-controller", lambda item: item["identity"].update({"py_gil_disabled": 1})),
            ("malformed-controller-python-digest", lambda item: item["identity"].update({"python_executable_sha256": "bad"})),
            ("missing-worker-identity", lambda item: item["stages"][5].pop("identity")),
            ("wrong-worker-python-bytes", lambda item: item["stages"][5]["identity"].update({"python_executable_sha256": "3" * 64})),
            ("missing-source-digest-manifest", lambda item: item.pop("source_files_sha256")),
            ("missing-source-input-digest", lambda item: item["source_files_sha256"].pop("requirements/runtime.in")),
            ("malformed-source-input-digest", lambda item: item["source_files_sha256"].update({"requirements/runtime.in": "bad"})),
            ("unapproved-source-key", lambda item: item["source_files_sha256"].update({"requirements/extra.in": "3" * 64})),
            ("wrong-source-helper-digest", lambda item: item["source_files_sha256"].update({".github/lnx02/verify_env.py": "3" * 64})),
            ("missing-bootstrap-record", lambda item: item.pop("bootstrap")),
            ("wrong-bootstrap-pip-hash", lambda item: item["bootstrap"].update({"sha256": "3" * 64})),
            ("wrong-bootstrap-pip-version", lambda item: item["bootstrap"].update({"version": "26.2.0"})),
            ("wrong-bootstrap-source-file", lambda item: item["bootstrap"].update({"source_url": "https://files.pythonhosted.org/packages/fixture/other.whl"})),
            ("unapproved-bootstrap-host", lambda item: item["bootstrap"].update({"source_url": "https://example.invalid/" + PIP_FILENAME})),
            ("missing-worker-pip", lambda item: item["stages"][5].pop("pip")),
            ("wrong-worker-pip-version", lambda item: item["stages"][5]["pip"].update({"version": "26.2.0"})),
            ("wrong-worker-bootstrap-pip-hash", lambda item: item["stages"][5]["pip"].update({"verified_bootstrap_sha256": "3" * 64})),
            ("runpy-same-pid-unconfirmed", lambda item: item["stages"][5]["pip_operation"].update({"runpy_same_pid": False})),
            ("missing-profile-on-check", lambda item: item["stages"][5].pop("profile")),
            ("missing-role-on-check", lambda item: item["stages"][5].pop("role")),
            ("caught-unexpected-denial", lambda item: item["stages"][5]["guard"]["unexpected_denials"].update({"socket.connect": 1})),
            ("unguarded-actual-pip-pid", lambda item: item["stages"][5]["pip_operation"].update({"pid": 101})),
            ("missing-actual-process-pid", lambda item: item["stages"][5]["guard"].pop("pid")),
            ("requests-fixture-probe-missing", lambda item: item["stages"][5]["negative_probes"].update({"requests_fixture": False})),
            ("skipped-check-exit", lambda item: item["stages"][5].update({"exit_code": None})),
            ("kernel-probe-audit-denial-not-reach-os", lambda item: item["stages"][0]["namespace"].update({"kernel_probe_reached_os": False})),
            ("offline-network-interface-up", lambda item: item["stages"][0]["namespace"].update({"interfaces": [{"name": "lo", "up": True}]})),
            ("namespace-child-uid-changed", lambda item: item["stages"][0]["namespace"].update({"child_ids": [0, 0, 0]})),
            ("namespace-reused-parent-netns", lambda item: item["stages"][0]["namespace"]["child_namespace_inodes"].update({"net": 20})),
            ("missing-sixth-fresh-bootstrap", lambda item: item["stages"].pop(16)),
            ("missing-wheelhouse-digest", lambda item: item.pop("wheelhouse_digest_manifest")),
            ("invented-runtime-verification", lambda item: item.update({"production": "VERIFIED"})),
        ):
            variant = copy.deepcopy(success_report)
            mutate(variant)
            reject("success-audit-refuses-" + name,
                   lambda variant=variant: validate_success_evidence(variant, profile_locks))
        verify_variant = success_fixture("verify")
        verify_variant["source_files_sha256"]["requirements/" + LOCK_NAMES["runtime"]] = "3" * 64
        reject("success-audit-refuses-verified-source-lock-digest-mismatch",
               lambda: validate_success_evidence(verify_variant, profile_locks))
        fail_output = root / "failure-artifact"
        fail_output.mkdir(mode=0o700)
        fail_report = {"schema_version": 1, "status": "FAIL", "source_sha": sha,
                       "base_sha": base, "mode": "generate", "failure_code": "self-test-fixture"}
        finalize_output(fail_output, fail_report)
        arguments = argparse.Namespace(output=str(fail_output), expected_sha=sha, expected_base=base,
                                       mode="generate", success="false")
        check("closed-failure-bundle-with-no-partial-locks", lambda: expect(audit_output(arguments)["artifact_count"] == 2))
        reject("failure-cannot-publish-partial-lock", lambda: finalize_output(root, fail_report, {"runtime": valid.encode("utf-8")}))
        write_new(root, fail_output / "unexpected.bin", b"extra")
        reject("extra-evidence-member-refused", lambda: audit_output(arguments))
        (fail_output / "unexpected.bin").unlink()
        (fail_output / "report.json").write_bytes(json_bytes({**fail_report, "failure_code": "tampered"}))
        reject("evidence-byte-tampering-refused", lambda: audit_output(arguments))
        reject("owned-root-nonce-required-for-cleanup", lambda: owned(root, "wrong"))
    finally:
        cleanup(root, nonce)
    failed = [item for item in results if item["status"] == "FAIL"]
    return {"status": "FAIL" if failed else "PASS", "scope": "portable-static-refusal-tests",
            "native_linux_environment": "NOT VERIFIED", "cases": results,
            "passed": sum(item["status"] == "PASS" for item in results),
            "not_available": sum(item["status"] == "NOT AVAILABLE" for item in results),
            "failed": len(failed), "guard": GUARD.evidence()}, 1 if failed else 0


class Parser(argparse.ArgumentParser):
    def error(self, message):
        raise PolicyError("invalid-cli")


def main():
    parser = Parser(prog="verify_env.py", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    target = commands.add_parser("identity")
    target.add_argument("--namespace-required", action="store_true")
    tests = commands.add_parser("self-test")
    tests.add_argument("--repo-root", default=str(Path(__file__).absolute().parents[2]))
    run = commands.add_parser("run")
    run.add_argument("--mode", choices=("generate", "verify"), required=True)
    run.add_argument("--repo-root", required=True)
    run.add_argument("--output", required=True)
    run.add_argument("--expected-sha", required=True)
    run.add_argument("--expected-base", required=True)
    internal = commands.add_parser("worker", help=argparse.SUPPRESS)
    internal.add_argument("--job", required=True)
    internal.add_argument("--job-sha256", required=True)
    audit = commands.add_parser("audit-output")
    audit.add_argument("--output", required=True)
    audit.add_argument("--expected-sha", required=True)
    audit.add_argument("--expected-base", required=True)
    audit.add_argument("--mode", choices=("generate", "verify"), required=True)
    audit.add_argument("--success", choices=("true", "false"), required=True)
    try:
        args = parser.parse_args()
        if args.command == "identity":
            result, code = standalone_identity(args.namespace_required), 0
        elif args.command == "self-test":
            result, code = self_test(real_components(Path(args.repo_root).absolute(), directory=True))
        elif args.command == "run":
            result, code = pipeline(args)
            result = {"status": result["status"], "source_sha": result["source_sha"],
                      "mode": result["mode"], "failure_code": result.get("failure_code"),
                      "production": "NOT VERIFIED"}
        elif args.command == "worker":
            return worker(args)
        else:
            result, code = audit_output(args), 0
    except SystemExit as error:
        # argparse's normal --help exit is successful; retain its real code.
        return error.code if isinstance(error.code, int) else 1
    except BaseException as error:
        result = {"status": "FAIL", "failure_code": str(error) if isinstance(error, PolicyError) else type(error).__name__}
        code = 1
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
