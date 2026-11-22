"""Batch side of the pipeline. Streaming runs permanently and is not orchestrated here."""

import datetime as dt

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

# No start_date on purpose: a fresh install waits for the next cron slot instead of
# immediately running yesterday's job against an empty lake.
COMMON = {
    "catchup": False,
    "max_active_runs": 1,
    "tags": ["weather"],
    "default_args": {
        "retries": 1,
        "retry_delay": dt.timedelta(minutes=2),
        "execution_timeout": dt.timedelta(minutes=40),
    },
}


def submit(task_id: str, module: str) -> BashOperator:
    return BashOperator(task_id=task_id, bash_command=f"/opt/app/bin/submit {module} ")


with DAG("weather_forecast", schedule="10,40 * * * *", **COMMON):
    submit("forecast", "weather_pipeline.ml.forecast")

with DAG("weather_gold_daily", schedule="15 18 * * *", **COMMON):
    submit("build_gold", "weather_pipeline.batch.gold") >> submit("quality_gate", "weather_pipeline.batch.checks")

with DAG("weather_train_weekly", schedule="30 19 * * 0", **COMMON):
    submit("train", "weather_pipeline.ml.train") >> submit("forecast", "weather_pipeline.ml.forecast")
