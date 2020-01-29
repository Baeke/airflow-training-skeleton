import json
import datetime
import airflow
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.contrib.operators.postgres_to_gcs_operator import (
    PostgresToGoogleCloudStorageOperator,
)
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataProcPySparkOperator,
    DataprocClusterDeleteOperator,
)
from airflow.models import DAG
from operators.http_to_gcs_operator import HttpToGcsOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

# project_id="airflowbolcom-jan2829-99875f84"
# analytics_bucket_name="europe-west1-training-airfl-840ef3d9-bucket"

bucket_name="gdd_bucket"
currency="EUR"

args = {
    "owner": "godatadriven",
    "start_date": datetime.datetime(2019, 11, 24),
}

dag = DAG(
    dag_id="real_estate",
    default_args=args,
    description="DAG downloading rocket launches from Launch Library.",
    schedule_interval="0 0 * * *"
)


def check_date(execution_date, **context):
    return execution_date <= datetime.datetime(2019, 11, 28)


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


check_date = ShortCircuitOperator(
        task_id="check_if_before_end_of_last_year",
        python_callable=check_date,
        provide_context=True,
    )

# # use of f voor format dan {{{{ gebruiken om {{ 2 over te houden
# get_from_api_to_gcs = HttpToGcsOperator(
#     task_id="get_from_api_to_gcs",
#     endpoint = f"/history?start_at={{{{ ds }}}}&end_at={{{{ tomorrow_ds }}}}&base=GBP&symbols={currency}",
#     http_conn_id = "currency-http",
#     gcs_conn_id = "google_cloud_storage_default",
#     gcs_path = f"usecase/currency/{{{{ ds }}}}-{currency}.json",
#     gcs_bucket = f"{bucket_name}",
#     dag=dag
# )
#
#
print_stats = PythonOperator(
    task_id="print_stats",
    python_callable=_print_stats,
    provide_context=True,
    dag=dag
)

check_date >> print_stats
# >> get_from_api_to_gcs >> print_stats
