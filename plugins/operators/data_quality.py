import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries


class DataQualityOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_check="",
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.dq_check=dq_check

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
                
        for test in self.dq_check:
            records = redshift_hook.get_records(test.get('check_sql'))
            if records[0][0] != test.get('expected_result'):
                raise ValueError(f"Data quality check failed. {records[0][0]} doesn't equal {test.get('expected_result')}") 
            else:   
                self.log.info(f"""Data quality check passed with {records[0][0]} records""")