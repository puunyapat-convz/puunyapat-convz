from airflow.hooks.base_hook import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

SLACK_OFM_ALERT_CONN = 'slack_ofm_alert_conn'

def ofm_task_fail_slack_alert(context):
    slack_webhook_token  = BaseHook.get_connection(SLACK_OFM_ALERT_CONN).password
    ti = context.get('task_instance')
    
    slack_msg = """
            OFM alert: Task Failed. 
            Task: {task}  
            Dag: {dag} 
            Execution Time: {exec_date}  
            Log Url: {log_url} 
            """.format(
                task = ti.task_id,
                dag  = ti.dag_id,
                log_url   = ti.log_url,
                exec_date = context.get('ts')
            )

    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id=SLACK_OFM_ALERT_CONN,
        webhook_token=slack_webhook_token,
        message=slack_msg
    )
    return failed_alert.execute(context=context)

def ofm_missing_daily_file_slack_alert(context):
    slack_webhook_token  = BaseHook.get_connection(SLACK_OFM_ALERT_CONN).password
    ti = context.get('task_instance')
    ts = context.get('ts')
    table_name = '_'.join(ti.task_id.split('_')[2:])
    gcs_list = ti.xcom_pull(key='gcs_uri', task_ids=f'check_list_{table_name}').split('/')

    slack_msg = """
            OFM alert: Data file on [ {file_date} ] does not exist on GCS
            GCS URI: {gcs_path}
            Task: {task}
            Dag: {dag}
            Execution Time: {exec_date}
            """.format(
                type = type,
                task = ti.task_id,
                dag  = ti.dag_id,
                exec_date = ts,
                file_date = gcs_list[-1],
                gcs_path = '/'.join(gcs_list[0:-1]),
            )

    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id=SLACK_OFM_ALERT_CONN,
        webhook_token=slack_webhook_token,
        message=slack_msg
    )
    return failed_alert.execute(context=context)

def ofm_missing_daily_ctrl_slack_alert(context):
    slack_webhook_token  = BaseHook.get_connection(SLACK_OFM_ALERT_CONN).password

    ti = context.get('task_instance')
    ts = context.get('ts')
    table_name = '_'.join(ti.task_id.split('_')[2:])

    gcs_list   = ti.xcom_pull(key='gcs_uri', task_ids=f'check_list_{table_name}')
    gcs_prefix = gcs_list[0].split('/')[-1]
    filename   = [ f"{gcs_prefix}_{name}" for name in gcs_list[1] ]

    slack_msg = """
            OFM alert: Control file with prefix [ {filename} ] do not exist on GCS
            GCS URI: {gcs_path}
            Task: {task}
            Dag: {dag}
            Execution Time: {exec_date}
            """.format(
                gcs_path = '/'.join(gcs_list[0].split('/')[0:-1]),
                filename = ' and '.join(filename),
                task = ti.task_id,
                dag  = ti.dag_id,
                exec_date = ts,
            )

    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id=SLACK_OFM_ALERT_CONN,
        webhook_token=slack_webhook_token,
        message=slack_msg
    )
    return failed_alert.execute(context=context)

def ofm_missing_intraday_file_slack_alert(context):
    slack_webhook_token  = BaseHook.get_connection(SLACK_OFM_ALERT_CONN).password
    ti = context.get('task_instance')
    ts = context.get('ts')
    # table_name = ti.task_id.split('_')[-1]
    # gcs_list = ti.xcom_pull(key='gcs_uri', task_ids=f'check_tm1_list_{table_name}').split('/')

    slack_msg = """
            OFM alert: Task Failed. 
            Task: {task}  
            Dag: {dag} 
            Execution Time: {exec_date}  
            Log Url: {log_url} 
            """.format(
                task = ti.task_id,
                dag  = ti.dag_id,
                log_url   = ti.log_url,
                exec_date = ts
            )

    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id=SLACK_OFM_ALERT_CONN,
        webhook_token=slack_webhook_token,
        message=slack_msg
    )
    return failed_alert.execute(context=context)
