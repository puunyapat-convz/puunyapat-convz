from airflow.hooks.base_hook import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

SLACK_OFM_ALERT_CONN = 'slack_ofm_alert_conn'


def ofm_task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(
        SLACK_OFM_ALERT_CONN).password
    slack_msg = """
            OFM alert: Task Failed. 
            Task: {task}  
            Dag: {dag} 
            Execution Time: {exec_date}  
            Log Url: {log_url} 
            """.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        exec_date=context.get('execution_date'),
        log_url=context.get('task_instance').log_url
    )
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id=SLACK_OFM_ALERT_CONN,
        webhook_token=slack_webhook_token,
        message=slack_msg)
    return failed_alert.execute(context=context)