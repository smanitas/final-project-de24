FROM apache/airflow:2.6.1

ENV PYTHONPATH="/opt/airflow/dags/scripts:${PYTHONPATH}"

COPY requirements.txt .
RUN pip install apache-airflow[amazon,postgres]==${AIRFLOW_VERSION} -r requirements.txt

COPY dags /opt/airflow/dags
