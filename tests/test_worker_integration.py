import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest
from celery import Celery

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("CELERY_UPTIME_RUN_INTEGRATION") != "1",
        reason="set CELERY_UPTIME_RUN_INTEGRATION=1 to run worker subprocess tests",
    ),
]


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def get_json(url: str, timeout: float = 1) -> dict:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.load(response)


def wait_for_ready(port: int, process: subprocess.Popen, timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError(f"worker exited with code {process.returncode}")
        try:
            if get_json(f"http://127.0.0.1:{port}/ready")["status"] == "ok":
                return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.1)
    raise AssertionError("worker health server did not become ready")


def celery_client(broker_folder: Path, result_folder: Path) -> Celery:
    app = Celery("integration-client", broker="filesystem://", backend=f"file://{result_folder}")
    app.conf.broker_transport_options = {
        "data_folder_in": str(broker_folder),
        "data_folder_out": str(broker_folder),
        "control_folder": str(broker_folder),
    }
    return app


@pytest.mark.parametrize(("pool", "concurrency"), [("gevent", 4), ("prefork", 2), ("solo", 1)])
def test_worker_pool_remains_responsive_and_releases_health_port(tmp_path, pool, concurrency):
    broker_folder = tmp_path / "broker"
    result_folder = tmp_path / "results"
    broker_folder.mkdir()
    result_folder.mkdir()
    port = free_port()
    hostname = f"test-{pool}@integration"
    log_path = tmp_path / "worker.log"
    env = os.environ | {
        "CELERY_UPTIME_HOST": "127.0.0.1",
        "CELERY_UPTIME_PORT": str(port),
        "CELERY_UPTIME_CHECK_INTERVAL": "0.1",
        "CELERY_UPTIME_TEST_BROKER_FOLDER": str(broker_folder),
        "CELERY_UPTIME_TEST_RESULT_FOLDER": str(result_folder),
        "PYTHONPATH": os.pathsep.join([str(Path.cwd() / "src"), str(Path.cwd() / "tests")]),
    }
    command = [
        sys.executable,
        "-m",
        "celery",
        "-A",
        "integration_fixture_app:celery_app",
        "worker",
        f"--pool={pool}",
        f"--concurrency={concurrency}",
        f"--hostname={hostname}",
        "--loglevel=INFO",
        "--without-gossip",
        "--without-mingle",
    ]

    with log_path.open("w+") as log:
        worker = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            wait_for_ready(port, worker)
            assert get_json(f"http://127.0.0.1:{port}/health")["status"] == "ok"

            ping = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "celery",
                    "-A",
                    "integration_fixture_app:celery_app",
                    "inspect",
                    "ping",
                    "--destination",
                    hostname,
                    "--timeout=10",
                ],
                env=env,
                capture_output=True,
                text=True,
                timeout=15,
            )
            assert ping.returncode == 0, ping.stdout + ping.stderr
            assert "pong" in ping.stdout

            client = celery_client(broker_folder, result_folder)
            result = client.send_task("celery_uptime.integration.sleep", args=[0.01])
            assert result.get(timeout=15)["seconds"] == 0.01

            results = [client.send_task("celery_uptime.integration.sleep", args=[0.25]) for _ in range(concurrency)]
            timings = [result.get(timeout=15) for result in results]
            if concurrency > 1:
                assert max(timing["started_at"] for timing in timings) < min(
                    timing["finished_at"] for timing in timings
                )

            client.control.shutdown(destination=[hostname])
            worker.wait(timeout=15)
            assert worker.returncode == 0
        finally:
            if worker.poll() is None:
                os.killpg(worker.pid, signal.SIGTERM)
                worker.wait(timeout=10)

        log.seek(0)
        worker_log = log.read()

    assert "address already in use" not in worker_log.lower()
    with socket.socket() as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", port))