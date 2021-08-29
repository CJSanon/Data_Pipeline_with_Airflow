from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    sql_insert_template = """
        INSERT INTO {}
        {};    
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 append_insert=False,
                 primary_key="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql_query=sql_query
        self.append_insert=append_insert
        self.primary_key=primary_key

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id) 
        
        if self.append_insert:
            self.log.info("Delete-load functionality selected")
            insert_sql = f"""
                DELETE FROM {self.table};
                
                INSERT INTO {self.table}
                {self.sql_query}
            """
        else:
            self.log.info("Append-only functionality selected")
            insert_sql = f"""
                INSERT INTO {self.table}
                {self.sql_query}
            """

        redshift.run(insert_sql)
        self.log.info(f"Load into {self.table} table complete")
