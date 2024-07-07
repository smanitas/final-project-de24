import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.operators.email import EmailOperator
from similarity_processor import SimilarityProcessor
from config import engine, s3, BUCKET_NAME, INPUT_PREFIX, SIMILARITIES_PREFIX, FINGERPRINTS_PREFIX


def send_failure_notification(context):
    email = EmailOperator(
        task_id='send_failure_email',
        to='anita@example.com',
        subject='Airflow Task Failed',
        html_content=f"Task {context['task_instance'].task_id} failed. <br> Dag: {context['task_instance'].dag_id}",
    )
    return email.execute(context=context)

with DAG(
    dag_id="monthly_similarity_processing_dag",
    start_date=pendulum.datetime(2024, 6, 18, tz="UTC"),
    schedule_interval="0 0 1 * *",  # Run on the first day of each month
    catchup=False,
    tags=["similarity_processing"],
    default_args={
        'on_failure_callback': send_failure_notification
    }
) as dag:

    start_op = EmptyOperator(task_id="start")

    check_for_new_monthly_file_op = S3KeySensor(
        task_id='check_for_new_monthly_file',
        bucket_name=BUCKET_NAME,
        bucket_key=f"{INPUT_PREFIX}{{{{ execution_date.strftime('%Y-%m') }}}}.csv",
        aws_conn_id='aws_default',
        timeout=18*60*60,
        poke_interval=60*60,
        mode='poke'
    )

    compute_similarity_op = PythonOperator(
        task_id="compute_similarity",
        python_callable=compute_similarity,
    )

    process_similarity_op = PythonOperator(
        task_id="process_similarity",
        python_callable=process_and_store_similarity,
    )

    finish_op = EmptyOperator(task_id="finish")

    start_op >> check_for_new_monthly_file_op >> compute_similarity_op >> process_similarity_op >> finish_op
