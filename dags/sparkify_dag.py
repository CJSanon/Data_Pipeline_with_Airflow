from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators import (
    StageToRedshiftOperator, 
    LoadFactOperator, 
    LoadDimensionOperator, 
    DataQualityOperator
)

from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1), #Set retry back to 5
    'catchup': False,
    'email_on_retry': False
}


dag = DAG('etl_s3_to_redshift_dw_dag',
         default_args=default_args, 
         description='Load and transform data in Redshift with Airflow', 
         schedule_interval='0 * * * *',
         max_active_runs=1
)

params = {'users_dim_table_name': 'users', 
          'songs_dim_table_name': 'songs',
          'artists_dim_table_name': 'artists',
          'time_dim_table_name': 'time',
          'songplays_fact_table_name': 'songplays'
         }
    
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credentials='aws_credentials',
    redshift_conn_id='redshift',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    copy_option="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_credentials='aws_credentials',
    redshift_conn_id='redshift',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    copy_option='auto'
)

load_songplays_fact_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='songplays',
    sql_query=SqlQueries.songplay_table_insert
)

load_users_dimension_table = LoadDimensionOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table=params.get('users_dim_table_name'),
    sql_query=SqlQueries.user_table_insert,
    append_insert=True
)


load_songs_dimension_table = LoadDimensionOperator(
    task_id='Load_songs_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table=params.get('songs_dim_table_name'),
    sql_query=SqlQueries.song_table_insert,
    append_insert=True
)

load_artists_dimension_table = LoadDimensionOperator(
    task_id='Load_artists_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table=params.get('artists_dim_table_name'),
    sql_query=SqlQueries.artist_table_insert,
    append_insert=True,
    primary_key="artistid"
)


load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table=params.get('time_dim_table_name'),
    sql_query=SqlQueries.time_table_insert,
    append_insert=True
)

data_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    dq_check = [
        {'check_sql': f"SELECT COUNT(*) FROM {params.get('users_dim_table_name')};", 'expected_result': 104}, 
        {'check_sql': f"SELECT COUNT(*) FROM {params.get('songs_dim_table_name')};", 'expected_result': 14896}, 
        {'check_sql': f"SELECT COUNT(*) FROM {params.get('artists_dim_table_name')};", 'expected_result': 10025}, 
        {'check_sql': f"SELECT COUNT(*) FROM {params.get('time_dim_table_name')};", 'expected_result': 54560}, 
        {'check_sql': f"SELECT COUNT(*) FROM {params.get('songplays_fact_table_name')};", 'expected_result': 54560}]
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_fact_table >> [load_users_dimension_table, load_songs_dimension_table, load_artists_dimension_table, load_time_dimension_table] >> data_quality_checks >> end_operator

