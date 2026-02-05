import pytest
from pathlib import Path

from infra_py.nats.config_loader import load_config


def _write(tmp_path: Path, name: str, content: str) -> Path:
    p = tmp_path / name
    p.write_text(content, encoding="utf-8")
    return p


def test_load_config_rejects_empty_path():
    with pytest.raises(ValueError):
        load_config("")


def test_load_config_missing_file(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "nope.yaml")


def test_load_config_yaml_parse_error(tmp_path: Path):
    p = _write(tmp_path, "bad.yaml", "nats: [unclosed")
    with pytest.raises(ValueError):
        load_config(p)


def test_load_config_root_must_be_mapping(tmp_path: Path):
    p = _write(tmp_path, "root.yaml", "- a\n- b\n")
    with pytest.raises(ValueError, match="yaml root"):
        load_config(p)


def test_load_config_ok_minimal(tmp_path: Path):
    p = _write(
        tmp_path,
        "ok.yaml",
        """
nats:
  servers: ["nats://127.0.0.1:4222"]
subjects:
  namespace: "inter"
  departments: [data, state]
jetstream:
  streams:
    - name: "S"
      subjects: ["a.*"]
  consumers:
    - stream: "S"
      name: "C"
      durable_name: "D"
      filter_subject: "a.1"
      deliver_subject: "a.1.deliver"
      ack_policy: explicit
""",
    )
    cfg = load_config(p)
    assert cfg["subjects"]["namespace"] == "inter"


def test_load_config_allows_empty_kols_lists(tmp_path: Path):
    p = _write(
        tmp_path,
        "intra.yaml",
        """
nats:
  servers: ["nats://127.0.0.1:4222"]
subjects:
  namespace: "intra"
  departments: [data, state]
  kols:
    data: ["market_data_kol"]
    state: []
jetstream:
  streams:
    - name: "INTRA_SYS_STREAM"
      subjects: ["intra.dpt.*.kol.*.sys.in"]
  consumers: []
""",
    )
    cfg = load_config(p)
    assert cfg["subjects"]["kols"]["state"] == []


def test_load_config_stream_subjects_must_be_non_empty(tmp_path: Path):
    p = _write(
        tmp_path,
        "bad_stream.yaml",
        """
nats:
  servers: ["nats://127.0.0.1:4222"]
subjects:
  namespace: "x"
jetstream:
  streams:
    - name: "S"
      subjects: []
  consumers: []
""",
    )
    with pytest.raises(ValueError, match="subjects"):
        load_config(p)


def test_load_config_consumer_requires_core_fields(tmp_path: Path):
    p = _write(
        tmp_path,
        "bad_consumer.yaml",
        """
nats:
  servers: ["nats://127.0.0.1:4222"]
subjects:
  namespace: "x"
jetstream:
  streams:
    - name: "S"
      subjects: ["a.*"]
  consumers:
    - stream: "S"
      durable_name: "D"
      filter_subject: "a.1"
      deliver_subject: "a.1.deliver"
      ack_policy: explicit
""",
    )
    # missing 'name'
    with pytest.raises(ValueError, match=r"consumers\[0\]\.name"):
        load_config(p)
