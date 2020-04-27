#!/usr/bin/env python
# -*- coding:utf-8 -*-

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import time
import boto3
from pprint import pprint

args = {'owner': 'psyoblade',
        'start_date': days_ago(n=1)}

dag = DAG(dag_id='list-s3-bucket',
          default_args=args,
          schedule_interval='@daily')


def list_s3(name, **kwargs):
    s3 = boto3.resource(name)
    for bucket in s3.buckets.all():
        print(bucket.name)

t1 = PythonOperator(task_id='task_1',
                    provide_context=True,
                    python_callable=list_s3,
                    op_kwargs={'name': 's3'},
                    dag=dag)
t1
