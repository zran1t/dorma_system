# infra_py/nats/test_js_auditor.py

from __future__ import annotations

import pytest
from datetime import timedelta

import infra_py.nats.js_auditor as mod


class _SConfig:
    def __init__(self, *, subjects, storage):
        self.subjects = subjects
        self.storage = storage


class _CConfig:
    def __init__(self, *, filter_subject, deliver_subject, ack_policy, ack_wait):
        self.filter_subject = filter_subject
        self.deliver_subject = deliver_subject
        self.ack_policy = ack_policy
        self.ack_wait = ack_wait


class _StreamInfo:
    def __init__(self, cfg):
        self.config = cfg


class _ConsumerInfo:
    def __init__(self, cfg):
        self.config = cfg


class _FakeJS:
    def __init__(self):
        self.streams = {}     # name -> _SConfig
        self.consumers = {}   # (stream, durable) -> _CConfig

    async def stream_info(self, name: str):
        if name not in self.streams:
            raise Exception("stream not found")
        return _StreamInfo(self.streams[name])

    async def consumer_info(self, stream: str, durable: str):
        key = (stream, durable)
        if key not in self.consumers:
            raise Exception("consumer not found")
        return _ConsumerInfo(self.consumers[key])


class _FakeNC:
    def __init__(self, js: _FakeJS):
        self._js = js

    def jetstream(self):
        return self._js


@pytest.mark.asyncio
async def test_audit_ok_when_match():
    js = _FakeJS()
    js.streams["S1"] = _SConfig(subjects=["a.*"], storage=mod.StorageType.FILE)
    js.consumers[("S1", "D1")] = _CConfig(
        filter_subject="a.1",
        deliver_subject="a.1.deliver",
        ack_policy=mod.AckPolicy.EXPLICIT,
        ack_wait=30.0,  # seconds float is acceptable in your code path
    )

    nc = _FakeNC(js)

    cfg = {
        "jetstream": {
            "streams": [{"name": "S1", "subjects": ["a.*"], "storage": "file"}],
            "consumers": [
                {
                    "stream": "S1",
                    "durable_name": "D1",
                    "filter_subject": "a.1",
                    "deliver_subject": "a.1.deliver",
                    "ack_policy": "explicit",
                    "ack_wait": "30s",
                }
            ],
        }
    }

    report = await mod.audit_channels_topology(cfg, nc)
    assert report["ok"] is True
    assert report["missing"]["streams"] == []
    assert report["missing"]["consumers"] == []
    assert report["mismatched"]["streams"] == {}
    assert report["mismatched"]["consumers"] == {}


@pytest.mark.asyncio
async def test_audit_reports_missing_stream_and_consumer():
    js = _FakeJS()
    nc = _FakeNC(js)

    cfg = {
        "jetstream": {
            "streams": [{"name": "S_missing", "subjects": ["a.*"], "storage": "file"}],
            "consumers": [
                {
                    "stream": "S_missing",
                    "durable_name": "D_missing",
                    "filter_subject": "a.1",
                    "deliver_subject": "a.1.deliver",
                    "ack_policy": "explicit",
                    "ack_wait": "30s",
                }
            ],
        }
    }

    report = await mod.audit_channels_topology(cfg, nc)
    assert report["ok"] is False
    assert "S_missing" in report["missing"]["streams"]
    assert "D_missing@S_missing" in report["missing"]["consumers"]


@pytest.mark.asyncio
async def test_audit_reports_stream_mismatch_subjects_and_storage():
    js = _FakeJS()
    js.streams["S1"] = _SConfig(subjects=["b.*"], storage=mod.StorageType.MEMORY)
    nc = _FakeNC(js)

    cfg = {"jetstream": {"streams": [{"name": "S1", "subjects": ["a.*"], "storage": "file"}], "consumers": []}}

    report = await mod.audit_channels_topology(cfg, nc)
    assert report["ok"] is False
    assert "subjects" in report["mismatched"]["streams"]["S1"]
    assert "storage" in report["mismatched"]["streams"]["S1"]


@pytest.mark.asyncio
async def test_audit_reports_consumer_field_mismatch():
    js = _FakeJS()
    js.streams["S1"] = _SConfig(subjects=["a.*"], storage=mod.StorageType.FILE)
    js.consumers[("S1", "D1")] = _CConfig(
        filter_subject="a.2",
        deliver_subject="a.2.deliver",
        ack_policy=mod.AckPolicy.NONE,
        ack_wait=30.0,
    )
    nc = _FakeNC(js)

    cfg = {
        "jetstream": {
            "streams": [{"name": "S1", "subjects": ["a.*"], "storage": "file"}],
            "consumers": [
                {
                    "stream": "S1",
                    "durable_name": "D1",
                    "filter_subject": "a.1",
                    "deliver_subject": "a.1.deliver",
                    "ack_policy": "explicit",
                    "ack_wait": "30s",
                }
            ],
        }
    }

    report = await mod.audit_channels_topology(cfg, nc)
    assert report["ok"] is False
    key = "D1@S1"
    assert report["mismatched"]["consumers"][key]["filter_subject"]["expected"] == "a.1"
    assert report["mismatched"]["consumers"][key]["deliver_subject"]["expected"] == "a.1.deliver"
    assert report["mismatched"]["consumers"][key]["ack_policy"]["expected"] == "explicit"


@pytest.mark.asyncio
async def test_audit_ack_wait_tolerance_plus_minus_one_second():
    js = _FakeJS()
    js.streams["S1"] = _SConfig(subjects=["a.*"], storage=mod.StorageType.FILE)

    # actual 30.9s should still pass against "30s" with 1s tolerance
    js.consumers[("S1", "D1")] = _CConfig(
        filter_subject="a.1",
        deliver_subject="a.1.deliver",
        ack_policy=mod.AckPolicy.EXPLICIT,
        ack_wait=30.9,
    )
    nc = _FakeNC(js)

    cfg = {
        "jetstream": {
            "streams": [{"name": "S1", "subjects": ["a.*"], "storage": "file"}],
            "consumers": [
                {
                    "stream": "S1",
                    "durable_name": "D1",
                    "filter_subject": "a.1",
                    "deliver_subject": "a.1.deliver",
                    "ack_policy": "explicit",
                    "ack_wait": "30s",
                }
            ],
        }
    }

    report = await mod.audit_channels_topology(cfg, nc)
    assert report["ok"] is True

    # actual 32.1s should fail against "30s"
    js.consumers[("S1", "D1")].ack_wait = 32.1
    report2 = await mod.audit_channels_topology(cfg, nc)
    assert report2["ok"] is False
    assert "ack_wait" in report2["mismatched"]["consumers"]["D1@S1"]
