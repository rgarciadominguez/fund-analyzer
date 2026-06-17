"""Audit fix #3 (2026-06-17): el status del run se deriva del exit code AUTORITATIVO
del bat (0/5/10), no se colapsa todo ≠0 a 'failed'. Un run exit 5 es PUBLICABLE.

Single (watch_run) y cola (_classify_run_result) comparten _status_from_exit_code.
"""
from tools.web_server import _status_from_exit_code as s


def test_exit_0_done():
    assert s(0) == "done"


def test_exit_5_is_publishable_warnings():
    # El caso del bug: exit 5 (publicable con avisos) NO debe ser 'failed'.
    assert s(5) == "completed_with_warnings"


def test_exit_10_failed():
    assert s(10) == "failed"


def test_unexpected_exit_without_output_is_failed():
    assert s(2, "ZZ0000000000") == "failed"
    assert s(-1) == "failed"
