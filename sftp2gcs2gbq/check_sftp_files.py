from airflow                   import configuration, DAG
from airflow.operators.python  import BranchPythonOperator
from airflow.operators.dummy   import DummyOperator
from airflow.models            import Variable
from airflow.utils.task_group  import TaskGroup
from utils.dag_notification    import *

from airflow.providers.ssh.hooks.ssh    import *

import datetime as dt
import logging

######### VARIABLES ###########

log       = logging.getLogger(__name__)
path      = configuration.get('core','dags_folder')
MAIN_PATH = path + "/../data"

MAIN_FOLDER = ["ODP", "B2S"]
SUB_FOLDER  = ["POS"]

###############################

def _list_file(ti, hookname, mainfolder, subfolder, tablename):
    SSH_HOOK  = SSHHook(
        ssh_conn_id = hookname,
        banner_timeout = 120,
        conn_timeout   = 120,
        keepalive_interval = 15
    )

    with SSH_HOOK.get_conn() as ssh_client:
        SFTP_HOOK = ssh_client.open_sftp()
        file_list = SFTP_HOOK.listdir(f"/{subfolder}/outbound/{tablename}/")
        SFTP_HOOK.close()

    if file_list == [ 'archive' ]:
        return f"no_alert_{mainfolder}_{subfolder}_{tablename}"
    else:
        file_list.remove('archive')
        ti.xcom_push(key='stuck_files', value=file_list)
        ti.xcom_push(key='sftp_path', value=f"/{subfolder}/outbound/{tablename}/")
        return f"send_alert_{mainfolder}_{subfolder}_{tablename}"

with DAG(
    dag_id="check_sftp_files",
    # schedule_interval=None,
    schedule_interval="59 23 * * *",
    start_date=dt.datetime(2022, 7, 7),
    catchup=True,
    max_active_runs=1,
    tags=['convz', 'production', 'mario', 'alert', 'sftp'],
    description='Alert for stuck files on SFTP server',
    render_template_as_native_obj=True,
    default_args={
        'on_failure_callback': ofm_task_fail_slack_alert,
        'retries': 0
    }
) as dag:

    start_task = DummyOperator(task_id = "start_task")
    end_task   = DummyOperator(task_id = "end_task", trigger_rule='none_failed')

    iterable_sources_list = Variable.get(
        key=f'sftp_alert',
        deserialize_json=True
    )
    # iterable_sources_list = {
    #     "ODP_JDA": ["BCH_JDA_DataPlatform_APADDR"],
    #     "B2S_JDA": ["BCH_JDA_DataPlatform_APADDR"],
    #     "ODP_POS": ["POS_DataPlatform_Txn_Translator"],
    #     "B2S_POS": ["POS_DataPlatform_Txn_Translator"]
    # }

    with TaskGroup(
        f'load_{SUB_FOLDER[0]}_tasks_group',
        prefix_group_id=False,
    ) as load_source_tasks_group:

        for source in MAIN_FOLDER:

            if source == "ODP": source = "OFM"
            HOOK_NAME   = f"sftp-{source.lower()}-connection-id"
            if source == "OFM": source = "ODP"

            for subsource in SUB_FOLDER:

                start_source = DummyOperator(task_id = f"start_{source}_{subsource}")

                with TaskGroup(
                    f'load_{source}_{subsource}_tasks_group',
                    prefix_group_id=False,
                ) as load_tables_tasks_group:

                    for table in iterable_sources_list.get(f"{source}_{subsource}"):

                        TABLE_ID = f'{table}'

                        list_file = BranchPythonOperator(
                            task_id=f'list_file_{source}_{subsource}_{table}',
                            python_callable=_list_file,
                            pool='sftp_connect_pool',
                            op_kwargs = {
                                'hookname'  : HOOK_NAME,
                                'mainfolder': source,
                                'subfolder' : subsource,
                                'tablename' : table
                            }
                        )

                        send_alert = DummyOperator(
                            task_id =f"send_alert_{source}_{subsource}_{table}",
                            on_success_callback = ofm_stuck_sftp_file_slack_alert                        
                        )

                        no_alert = DummyOperator(task_id =f"no_alert_{source}_{subsource}_{table}")

                        list_file >> [ send_alert, no_alert ]

                start_source >> load_tables_tasks_group

    start_task >> load_source_tasks_group >> end_task