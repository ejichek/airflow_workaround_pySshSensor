from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.python import PythonSensor
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

bash_cmd1 = "/usr/bin/bash /home/userName1/dir1/dir2/file1.sh "
bash_cmd2 = "/usr/bin/bash /home/userName1/dir1/dir2/file2.sh "

# Питоном отправляем ssh запросы 
def pyBashExecutor(bash_command):
    ssh_hook = SSHHook(ssh_conn_id='ssh_08')
    with ssh_hook.get_conn() as ssh_client:
        stdin, stdout, stderr = ssh_client.exec_command(bash_command)
        output = stdout.read().decode()   
    if len(output) == 0:
        #sys.exit('Принудительно роняем')
        #'Принудительно роняем' 
        return False
    if len(output) > 0:
        return True

# Питоном отправляем ssh запросы 
# def pyBashExecutor(bash_command):
#     ssh_hook = SSHHook(ssh_conn_id='ssh_08')
#     with ssh_hook.get_conn() as ssh_client:
#         stdout = ssh_client.exec_command(bash_command)
#         exit_status = stdout.channel.recv_exit_status()
#     if exit_status == 0:
#         #sys.exit('Принудительно роняем')
#         #'Принудительно роняем' 
#         return True
#     else:
#         return False

with DAG(dag_id='Dag_schedule',
         tags=["ETL на расписании"],
         default_args=default_args,
         schedule_interval='01 18 * * *',    # каждый день в 18:01 утра, но тк UTC +3 часа, поэтому в 21:01 утра
         start_date=days_ago(1)
         ) as dag:
         
    get_some1 = PythonSensor(
        task_id='get_checkIncrementStatisticsLogs',
        python_callable=pyBashExecutor,
        poke_interval=10 * 60,  # каждые 10 минут, Через какое время перезапускаться (10 * 60)
        timeout=12 * 60 * 60,  # время до принудительного падения - 9 часов. До 6 утра.
        # retries=720,
        soft_fail=False,
        mode="reschedule",
        dag=dag,
        op_kwargs={
            'bash_command': bash_cmd1}
    )

    task1 = SSHOperator(
        task_id='task1',
        ssh_conn_id="ssh_conn",
        command=bash_cmd1,
        dag=dag
    )

    
get_some1 >> task1
