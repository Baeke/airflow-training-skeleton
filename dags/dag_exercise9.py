import json
import pathlib
import posixpath
import datetime
import requests

import airflow
from hooks.launch_hook import LaunchHook
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from operators.http_to_gcs_operator import HttpToGcsOperator

args = {
    "owner": "godatadriven",
    "start_date": datetime.datetime(2018, 1, 1)
}

dag = DAG(
    dag_id="real_estate",
    default_args=args,
    description="DAG downloading rocket launches from Launch Library.",
    schedule_interval="0 0 * * *"
)


def _download_rocket_launches(ds, tomorrow_ds, **context):
    query = f"https://launchlibrary.net/1.4/launch?startdate={ds}&enddate={tomorrow_ds}"
    result_path = f"/tmp/rocket_launches/ds={ds}"
    pathlib.Path(result_path).mkdir(parents=True, exist_ok=True)
    response = requests.get(query)
    print(f"response was {response}")

    with open(posixpath.join(result_path, "launches.json"), "w") as f:
        print(f"Writing to file {f.name}")
        f.write(response.text)

def _print_stats(ds, **context):
    with open(f"/tmp/rocket_launches/ds={ds}/launches.json") as f:
        data = json.load(f)
        rockets_launched = [launch["name"] for launch in data["launches"]]
        rockets_str = ""

        if rockets_launched:
            rockets_str = f" ({' & '.join(rockets_launched)})"
            print(f"{len(rockets_launched)} rocket launch(es) on {ds}{rockets_str}.")
        else:
            print(f"No rockets found in {f.name}")


# download_rocket_launches = LaunchHook(
#     task_id="download_rocket_launches",
#     query='',
#     destination='',
#     provide_context=True,
#     dag=dag
# )

get_from_api_to_gcs = HttpToGcsOperator(
    task_id="get_from_api_to_gcs",
    endpoint=f"https://api.exchangeratesapi.io/history?start_at=/{{ ds }}&end_at=/{{ ds }}&symbols=EUR&base=GBP",
    gcs_path='{{ ds }}',
    gcs_bucket='gdd_bucket',
    dag=dag
)


print_stats = PythonOperator(
    task_id="print_stats",
    python_callable=_print_stats,
    provide_context=True,
    dag=dag
)

get_from_api_to_gcs >> print_stats
