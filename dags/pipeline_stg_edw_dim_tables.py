import os
import sys
HOME = os.getenv('AIRFLOW_HOME')
TEMPLATE_ROOT_PATH = os.path.join(HOME, 'dags', 'resources', 'sql_template')
sys.path.append(HOME)

import json
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from dags.resources.business.dim.l2_dim_staging import staging_layer
from dags.resources.business.dim.l3_dim_edw import edw_layer
from lib.utils import get_rundate as _get_rundate

_default_args = {
    'owner': 'tungnt',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=15),
    'start_date': datetime.today()
}

def create_dag(_dag_id, _schedule, **kwargs):

    @dag(
        dag_id=_dag_id,
        default_args=_default_args,
        schedule=_schedule,
        tags=['dim_stg_edw_pipelines', kwargs.get('table_name')],
        catchup=False
    )
    def get_dag():

        @task(provide_context=True) 
        def get_rundate(**context):
            run_dt = _get_rundate()
            print(f">> Rundate: {run_dt}")
            context['ti'].xcom_push(key="rundate", value=run_dt)
        
        stg_layer = staging_layer(**kwargs)

        dw_layer = edw_layer(**kwargs)

        get_rundate() >> stg_layer >> dw_layer

    return get_dag()

config_path = os.path.join(HOME, 'config', 'pipeline_config.json')

with open(config_path, 'r') as inp:
    config_content = inp.read()
    print('Config_content: ', config_content)
    pipelines = json.loads(config_content)['dim_table']
    db_env = json.loads(config_content)['db_environment']

_project = db_env.get('project')
_landing_dataset = db_env.get('landing_dataset')
_staging_dataset = db_env.get('staging_dataset')
_dw_dataset = db_env.get('dw_dataset')

for pipeline in pipelines:
    _table_name = pipeline.get('table_name')
    _dag_id = f'{_table_name}_dag'
    _schedule_interval = pipeline.get('schedule_interval')

    _dim_type = pipeline.get('dim_type')
    _columns_detail_old = pipeline.get('columns_detail_old')
    _columns_detail_new = pipeline.get('columns_detail_new')
    _columns_nk = pipeline.get('columns_nk')
    _columns_nk_new = pipeline.get('columns_nk_new')

    cmn_config = {
        "gcp_conn_id": 'gcp',
        "project": _project,
        "landing_dataset": _landing_dataset,
        "staging_dataset": _staging_dataset,
        "dw_dataset": _dw_dataset,

        "template_root_path": os.path.join(TEMPLATE_ROOT_PATH),
        "table_name": _table_name,
        "dim_type": _dim_type,
        "columns_nk": _columns_nk,
        "columns_nk_new": _columns_nk_new,
        "columns_detail_old": _columns_detail_old,
        "columns_detail_new": _columns_detail_new
    }

    globals()[_dag_id] = create_dag(_dag_id, _schedule_interval, **cmn_config)

