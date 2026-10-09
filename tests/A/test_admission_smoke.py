"""Infrastructure admission only; no domain imports or data operations."""

import sys


def test_admission_smoke():
    metadata = sys.modules['mds_offline_guard'].report_metadata()
    assert metadata['lane'] == 'f3'
    assert metadata['mode'] == 'run'
    assert metadata['role'] in {'entry', 'command'}
    assert len(metadata['run_id']) == 32
    assert len(metadata['tested_sha']) == 40
    assert len(metadata['source_digest']) == 64
