# metaflow-phala

> **Work in progress** — not yet production-ready. Integration tests pending.

[![CI](https://github.com/npow/metaflow-phala/actions/workflows/ci.yml/badge.svg)](https://github.com/npow/metaflow-phala/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/metaflow-phala)](https://pypi.org/project/metaflow-phala/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)

Run any Metaflow step inside a hardware-attested confidential VM with one decorator.

## The problem

You need to process sensitive data — PII, health records, financial models — in the cloud, but you can't fully trust the infrastructure. Remote workers like `@batch` and `@kubernetes` run in plain VMs where cloud providers or insiders can read your memory. Phala Cloud's TEE CVMs provide hardware-enforced memory encryption and remote attestation, but wiring them into a Metaflow pipeline requires rewriting your execution layer from scratch.

## Quick start

```bash
pip install metaflow-phala
export PHALA_API_KEY=your-api-key
export METAFLOW_DEFAULT_DATASTORE=s3
export METAFLOW_DATASTORE_SYSROOT_S3=s3://your-bucket/metaflow
```

```python
from metaflow import FlowSpec, step
from metaflow_extensions.phala.plugins.phala_decorator import PhalaDecorator as phala

class MyFlow(FlowSpec):
    @phala(cpu=2, memory=4096)
    @step
    def train(self):
        self.result = 42
        self.next(self.end)

    @step
    def end(self):
        print(self.result)

if __name__ == "__main__":
    MyFlow()
```

```bash
python flow.py run
```

## Install

```bash
pip install metaflow-phala
```

From source:

```bash
git clone https://github.com/npow/metaflow-phala
cd metaflow-phala
pip install -e .
```

## Usage

### Basic step with custom resources

```python
@phala(cpu=4, memory=8192, disk=40)
@step
def train(self):
    import sklearn
    ...
```

### Pass extra environment variables

```python
@phala(cpu=2, memory=4096, env={"HF_TOKEN": "hf_...", "MY_SECRET": "..."})
@step
def inference(self):
    ...
```

### Set a step timeout

```python
@phala(cpu=2, memory=4096, timeout=1800)  # 30-minute cap
@step
def preprocess(self):
    ...
```

### Available parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `image` | `python:3.11-slim` | Docker image |
| `cpu` | `2` | vCPUs |
| `memory` | `2048` | Memory in MB |
| `disk` | `20` | Disk size in GB |
| `timeout` | `3600` | Max seconds to wait |
| `env` | `{}` | Extra environment variables |

All defaults can be overridden via environment variables: `METAFLOW_PHALA_IMAGE`, `METAFLOW_PHALA_CPU`, `METAFLOW_PHALA_MEMORY`, `METAFLOW_PHALA_DISK`, `METAFLOW_PHALA_TIMEOUT`.

## How it works

When Metaflow runs a `@phala`-decorated step, instead of running locally it:

1. Uploads the code package to S3 (once per flow run)
2. Provisions a Phala Cloud CVM via the REST API
3. Starts the CVM with a docker-compose spec that embeds all env vars and a base64-encoded bootstrap script
4. The container downloads the code package, installs dependencies, and runs the Metaflow step
5. On completion, the exit code is written to an S3 sentinel key
6. The local process polls the sentinel, retrieves the result, and deletes the CVM

Artifacts are stored in your S3 datastore as usual — identical to `@batch` or `@kubernetes`.

## Development

```bash
git clone https://github.com/npow/metaflow-phala
cd metaflow-phala
pip install -e ".[dev]"
pytest -v  # unit tests
```

Integration tests require a Phala API key and S3:

```bash
PHALA_API_KEY=... METAFLOW_DEFAULT_DATASTORE=s3 METAFLOW_DATASTORE_SYSROOT_S3=s3://... \
pytest tests/ -m integration -s
```

## License

[Apache 2.0](LICENSE)
