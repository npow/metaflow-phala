"""Simple @phala integration test.

Run with:
    PHALA_API_KEY=... METAFLOW_DEFAULT_DATASTORE=s3 METAFLOW_DATASTORE_SYSROOT_S3=s3://... \
    pytest tests/test_hello_flow.py -m integration -s

The test runs a minimal Metaflow flow with @phala, checks that the artifact
round-trips correctly through S3.
"""
import os

import pytest


@pytest.mark.integration
def test_phala_hello_flow():
    """Run a minimal @phala flow and verify artifact round-trip."""
    # Verify required environment
    api_key = os.environ.get("PHALA_API_KEY") or os.environ.get("METAFLOW_PHALA_API_KEY")
    assert api_key, "PHALA_API_KEY must be set"
    assert os.environ.get("METAFLOW_DEFAULT_DATASTORE") == "s3", (
        "METAFLOW_DEFAULT_DATASTORE must be s3"
    )
    assert os.environ.get("METAFLOW_DATASTORE_SYSROOT_S3"), (
        "METAFLOW_DATASTORE_SYSROOT_S3 must be set"
    )

    import subprocess
    import sys
    import tempfile
    import textwrap

    flow_code = textwrap.dedent("""\
        from metaflow import FlowSpec, step
        from metaflow_extensions.phala.plugins.phala_decorator import PhalaDecorator as phala

        class HelloPhalaFlow(FlowSpec):
            @phala(cpu=2, memory=2048, timeout=600)
            @step
            def start(self):
                self.message = "hello from phala"
                self.next(self.end)

            @step
            def end(self):
                print(f"Got: {self.message}")
                assert self.message == "hello from phala"

        if __name__ == "__main__":
            HelloPhalaFlow()
    """)

    with tempfile.TemporaryDirectory() as tmpdir:
        flow_path = os.path.join(tmpdir, "hello_phala_flow.py")
        with open(flow_path, "w") as f:
            f.write(flow_code)

        result = subprocess.run(
            [sys.executable, flow_path, "--datastore=s3", "run"],
            capture_output=True,
            text=True,
            timeout=900,
        )
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
        assert result.returncode == 0, (
            f"Flow failed with exit code {result.returncode}\n"
            f"STDOUT: {result.stdout}\nSTEDRR: {result.stderr}"
        )
