"""Unit tests for pure helper functions — no API key or S3 required."""

import base64
import re

import yaml

from metaflow_extensions.phala.plugins.phala_cli import (
    _build_compose_yaml,
    _build_step_bash_script,
    _make_cvm_name,
    _parse_s3_sysroot,
    _sentinel_py_b64,
)


def test_make_cvm_name_valid():
    name = _make_cvm_name("MyFlow", "start", "1", 0)
    assert 5 <= len(name) <= 63
    assert re.match(r"^[a-z][a-z0-9-]*$", name)


def test_make_cvm_name_long_inputs():
    name = _make_cvm_name("A" * 100, "B" * 100, "99", 3)
    assert len(name) <= 63
    assert re.match(r"^[a-z][a-z0-9-]*$", name)


def test_make_cvm_name_deterministic():
    a = _make_cvm_name("MyFlow", "start", "1", 0)
    b = _make_cvm_name("MyFlow", "start", "1", 0)
    assert a == b


def test_make_cvm_name_unique_per_task():
    a = _make_cvm_name("MyFlow", "start", "1", 0)
    b = _make_cvm_name("MyFlow", "start", "2", 0)
    assert a != b


def test_parse_s3_sysroot_basic():
    bucket, prefix = _parse_s3_sysroot("s3://my-bucket/metaflow")
    assert bucket == "my-bucket"
    assert prefix == "metaflow"


def test_parse_s3_sysroot_nested():
    bucket, prefix = _parse_s3_sysroot("s3://my-bucket/path/to/metaflow")
    assert bucket == "my-bucket"
    assert prefix == "path/to/metaflow"


def test_parse_s3_sysroot_trailing_slash():
    _bucket, prefix = _parse_s3_sysroot("s3://my-bucket/metaflow/")
    assert prefix == "metaflow"


def test_sentinel_py_b64_is_valid_base64():
    s = _sentinel_py_b64()
    decoded = base64.b64decode(s).decode()
    assert "boto3" in decoded
    assert "urllib" in decoded


def test_build_step_bash_script_structure():
    script = _build_step_bash_script(["pip install metaflow"], "python flow.py step start")
    assert "#!/bin/bash" in script
    assert "MFLOG_STDOUT=/dev/stdout" in script
    assert "MFLOG_STDERR=/dev/stderr" in script
    assert "trap" in script
    assert "set -e" in script
    assert "set +e" in script
    assert "pip install metaflow" in script
    assert "python flow.py step start" in script
    assert "STEP_EXIT=$?" in script


def test_build_compose_yaml_structure():
    env = {"FOO": "bar", "METAFLOW_DEFAULT_DATASTORE": "s3"}
    result = _build_compose_yaml("python:3.11-slim", env)
    parsed = yaml.safe_load(result)
    svc = parsed["services"]["metaflow-step"]
    assert svc["image"] == "python:3.11-slim"
    assert svc["restart"] == "no"
    assert isinstance(svc["command"], list)
    assert svc["command"][0] == "python3"
    assert svc["environment"]["FOO"] == "bar"


def test_build_compose_yaml_special_chars_in_env():
    env = {
        "URL": "https://s3.example.com/key?foo=bar&baz=qux",
        "TOKEN": "abc+def/ghi==",
    }
    result = _build_compose_yaml("python:3.11-slim", env)
    parsed = yaml.safe_load(result)
    env_out = parsed["services"]["metaflow-step"]["environment"]
    assert env_out["URL"] == "https://s3.example.com/key?foo=bar&baz=qux"
    assert env_out["TOKEN"] == "abc+def/ghi=="
