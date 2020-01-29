# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Example DAG demonstrating the usage of the BashOperator."""

from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow.hooks.postgres_hook import PostgresHook

sql= "SELECT * FROM land_registry_price_paid_uk limit 10"

def _get_data():

    hook = PostgresHook(
        postgres_conn_id='postgres_cursus_db'
    )

    hook.get_records(
        sql
    )

    hook.run(sql)


def _print_exec_date(execution_date, **context):
    print(execution_date)


def _get_weekday(execution_date, **context):
    return execution_date.strftime("%a")


args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

with DAG(
    dag_id='dag4_postgres_hook',
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60)
) as dag:

    print_data = PythonOperator(
        task_id='print_data',
        python_callable= _get_data,
    )

    copy_data_to_gcs = PostgresToGoogleCloudStorageOperator(
        task_id='copy_data_to_gcs',
        sql=sql,
        bucket='gdd_bucket',
        filename='gdd_data.csv',
        postgres_conn_id='postgres_cursus_db'
    )

    final_task = DummyOperator(
        task_id='final_task',
        bash_command="sleep 1"
    )

    print_data >> copy_data_to_gcs >> final_task

