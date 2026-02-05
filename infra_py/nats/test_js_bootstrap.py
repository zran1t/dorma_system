# infra_py/nats/test_js_bootstrap.py

from __future__ import annotations

import pytest

import infra_py.nats.js_bootstrap as mod


class _FakeStreamInfo:
    def __init__(self, config):
        self.config = config


class _FakeConsumerInfo:
    def __init__(self, config):
        self.config = config


class _FakeJS:
    """
    Fake JetStream context:
        - add_stream / update_stream / stream_info
        - add_consumer / update_consumer / consumer_info
    """

    def __init__(self):
        self._streams = {}
        self._consumers = {}  # key: (stream, durable)

        self.add_stream_calls = []
        self.update_stream_calls = []
        self.add_consumer_calls = []
        self.update_consumer_calls = []

    async def add_stream(self, sc):
        self.add_stream_calls.append(sc)
        if sc.name in self._streams:
            raise Exception("stream exists")
        self._streams[sc.name] = sc

    async def update_stream(self, sc):
        self.update_stream_calls.append(sc)
        self._streams[sc.name] = sc

    async def stream_info(self, name: str):
        if name not in self._streams:
            raise Exception("stream not found")
        return _FakeStreamInfo(self._streams[name])

    async def add_consumer(self, stream: str, cc):
        self.add_consumer_calls.append((stream, cc))
        key = (stream, cc.durable_name)
        if key in self._consumers:
            raise Exception("consumer exists")
        self._consumers[key] = cc

    async def update_consumer(self, stream: str, cc):
        self.update_consumer_calls.append((stream, cc))
        key = (stream, cc.durable_name)
        self._consumers[key] = cc

    async def consumer_info(self, stream: str, durable: str):
        key = (stream, durable)
        if key not in self._consumers:
            raise Exception("consumer not found")
        return _FakeConsumerInfo(self._consumers[key])


class _FakeNC:
    def __init__(self, js: _FakeJS):
        self._js = js
        self.connect_calls = []

    async def connect(self, *, servers):
        self.connect_calls.append(list(servers))

    def jetstream(self):
        return self._js


@pytest.mark.parametrize(
    "text,default,expected",
    [
        (None, 30.0, 30.0),
        (60, 30.0, 60.0),
        (1.5, 30.0, 1.5),
        ("30s", 30.0, 30.0),
        ("1500ms", 30.0, 1.5),
        ("2m", 30.0, 120.0),
        ("1h", 30.0, 3600.0),
        ("  10  ", 30.0, 10.0),
        ("10x", 30.0, 10.0),  # unknown unit -> treat as seconds in current impl
        ("junk", 30.0, 30.0),  # parse fail -> default
    ],
)
def test_parse_duration_seconds(text, default, expected):
    got = mod._parse_duration_seconds(text, default)
    assert got == expected


@pytest.mark.asyncio
async def test_bootstrap_creates_when_missing(monkeypatch):
    js = _FakeJS()
    nc = _FakeNC(js)

    class _FakeNATSFactory:
        def __call__(self):
            return nc

    monkeypatch.setattr(mod, "NATS", _FakeNATSFactory())

    cfg = {
        "nats": {"servers": ["nats://127.0.0.1:4222"]},
        "jetstream": {
            "streams": [
                {"name": "S1", "subjects": ["a.*"], "storage": "file", "duplicates": "30s"},
            ],
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
        },
    }

    _nc, _js = await mod.bootstrap_nats_and_js(cfg)

    assert _nc is nc
    assert _js is js
    assert nc.connect_calls == [["nats://127.0.0.1:4222"]]
    assert len(js.add_stream_calls) == 1
    assert len(js.add_consumer_calls) == 1
    assert len(js.update_stream_calls) == 0
    assert len(js.update_consumer_calls) == 0


@pytest.mark.asyncio
async def test_bootstrap_updates_only_when_different(monkeypatch):
    js = _FakeJS()

    # pre-seed existing stream/consumer
    # We store the actual StreamConfig/ConsumerConfig objects; easiest is to call add_* once.
    # But add_stream/add_consumer will "create". We'll directly set for clarity.
    existing_sc = mod.StreamConfig(
        name="S1",
        subjects=["a.*"],
        storage=mod.StorageType.FILE,
        retention=mod.RetentionPolicy.LIMITS,
        discard=mod.DiscardPolicy.OLD,
        duplicate_window=30.0,
    )
    js._streams["S1"] = existing_sc

    existing_cc = mod.ConsumerConfig(
        durable_name="D1",
        filter_subject="a.1",
        deliver_subject="a.1.deliver",
        ack_policy=mod.AckPolicy.EXPLICIT,
        ack_wait=30.0,
    )
    js._consumers[("S1", "D1")] = existing_cc

    nc = _FakeNC(js)

    class _FakeNATSFactory:
        def __call__(self):
            return nc

    monkeypatch.setattr(mod, "NATS", _FakeNATSFactory())

    # change duplicate_window + ack_wait to force updates
    cfg = {
        "nats": {"servers": ["nats://127.0.0.1:4222"]},
        "jetstream": {
            "streams": [
                {"name": "S1", "subjects": ["a.*"], "storage": "file", "duplicates": "2m"},
            ],
            "consumers": [
                {
                    "stream": "S1",
                    "durable_name": "D1",
                    "filter_subject": "a.1",
                    "deliver_subject": "a.1.deliver",
                    "ack_policy": "explicit",
                    "ack_wait": "45s",
                }
            ],
        },
    }

    await mod.bootstrap_nats_and_js(cfg)

    # add_* should be attempted then fallback to update
    assert len(js.add_stream_calls) == 1
    assert len(js.update_stream_calls) == 1
    assert len(js.add_consumer_calls) == 1
    assert len(js.update_consumer_calls) == 1


@pytest.mark.asyncio
async def test_bootstrap_no_update_when_same(monkeypatch):
    js = _FakeJS()

    js._streams["S1"] = mod.StreamConfig(
        name="S1",
        subjects=["a.*"],
        storage=mod.StorageType.FILE,
        retention=mod.RetentionPolicy.LIMITS,
        discard=mod.DiscardPolicy.OLD,
        duplicate_window=30.0,
    )

    js._consumers[("S1", "D1")] = mod.ConsumerConfig(
        durable_name="D1",
        filter_subject="a.1",
        deliver_subject="a.1.deliver",
        ack_policy=mod.AckPolicy.EXPLICIT,
        ack_wait=30.0,
    )

    nc = _FakeNC(js)

    class _FakeNATSFactory:
        def __call__(self):
            return nc

    monkeypatch.setattr(mod, "NATS", _FakeNATSFactory())

    cfg = {
        "nats": {"servers": ["nats://127.0.0.1:4222"]},
        "jetstream": {
            "streams": [
                {"name": "S1", "subjects": ["a.*"], "storage": "file", "duplicates": "30s"},
            ],
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
        },
    }

    await mod.bootstrap_nats_and_js(cfg)

    # add attempted; since exists it falls back to stream_info/consumer_info but no updates
    assert len(js.add_stream_calls) == 1
    assert len(js.update_stream_calls) == 0
    assert len(js.add_consumer_calls) == 1
    assert len(js.update_consumer_calls) == 0
