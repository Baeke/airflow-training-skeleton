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
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator


def _print_exec_date(execution_date, **context):
    print(execution_date)


def _get_weekday(execution_date, **context):
    return execution_date.strftime("%a")


args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

with DAG(
    dag_id='dag3_branching',
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60)
) as dag:

    print_weekday = PythonOperator(
        task_id='print_weekday',
        python_callable= _get_weekday,
        provide_context=True
    )

    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=_get_weekday,
        provide_context=True,
        trigger_rule='dummy'
        )

    final_task = DummyOperator(
        task_id='final_task'
    )

    print_weekday >> branching
    names = ["email_joe", "email_bob", "email_alice"]
    for name in names:
        branching >> DummyOperator(task_id=name, dag=dag) >> final_task

