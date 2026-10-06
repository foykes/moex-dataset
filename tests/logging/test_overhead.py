import json
import statistics
import time
import uuid

from _probe import ROOT


def test_report_bounded_delivered_overhead(log, record_property, capsys):
    payload = 'i' * 1024
    baselines = []
    observed = []
    for repeat in range(3):
        before = time.perf_counter()
        total = 0
        for i in range(2000):
            total += len(payload) + i % 2
        baselines.append((time.perf_counter() - before) * 1000)
        session = log.configure_logging(ROOT, run_id='bench-' + uuid.uuid4().hex,
            environment='offline', producer_sha='c4f2210')
        before = time.perf_counter()
        for i in range(2000):
            log.emit_event(session, 'INFO', 'benchmark', payload, {'page': i})
            if (i + 1) % 250 == 0:
                assert log.flush_logging(session)['confirmed']
        report = log.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)
        observed.append(report['run_duration_ms'])
        assert report['exit_status'] == 0
        assert report['event_counters']['dropped'] == 0
        rows = [json.loads(line) for line in (session['_run_root'] / 'events.jsonl').read_text(encoding='utf-8').splitlines()]
        assert sum(row['event'] == 'benchmark' for row in rows) == 2000
    result = dict(events=2000, payload_bytes=1024, repeats=3, batch=250,
        baseline_median_ms=statistics.median(baselines), logging_median_ms=statistics.median(observed),
        samples_ms=observed, measured_through='delivery_shutdown_and_closed_inventory_before_summary',
        threshold='none', console='pytest capture INFO', file_level='DEBUG')
    record_property('overhead', result)
    (ROOT / '.f-log/evidence/overhead.json').write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
