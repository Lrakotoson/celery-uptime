import os
import time

from celery import Celery

from celery_uptime import CheckResult, monitor

broker_folder = os.environ["CELERY_UPTIME_TEST_BROKER_FOLDER"]
result_folder = os.environ["CELERY_UPTIME_TEST_RESULT_FOLDER"]

celery_app = Celery(
    "celery_uptime_integration",
    broker="filesystem://",
    backend=f"file://{result_folder}",
)
celery_app.conf.update(
    broker_transport_options={
        "data_folder_in": broker_folder,
        "data_folder_out": broker_folder,
        "control_folder": broker_folder,
    },
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
)


@celery_app.task(name="celery_uptime.integration.sleep")
def sleep_task(seconds: float) -> dict[str, float]:
    started_at = time.time()
    time.sleep(seconds)
    return {"seconds": seconds, "started_at": started_at, "finished_at": time.time()}


def process_check() -> CheckResult:
    return CheckResult(name="fixture", ok=True, detail="ok")


process_check.__celery_uptime_name__ = "fixture"
monitor(celery_app, checks=[process_check], include_auto_checks=False)