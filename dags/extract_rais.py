import airflow
from airflow.providers.ftp.operators.ftp import FTPFileTransmitOperator
from airflow import DAG
from datetime import timedelta,datetime
from ftplib import FTP


# FTP_SERVER='ftp.mtps.gov.br'

# ftp=FTP(FTP_SERVER)
# ftp.login()
# ftp.cwd('/pdet/microdados/RAIS/2020/')

# list_files=ftp.nlst()
# print(type(list_files))

# USER="anonymous"
# PASS="anonymous@"



default_args={
        "owner": "Stefano",
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
       
    }


with DAG(
    "extract_rais_from_FTP",
       
    description="A simple tutorial DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2023, 8, 19),
    catchup=False,
    tags=["rais"],
    default_args=default_args
) as dag:

    # get files from FTP 
    get_file = FTPFileTransmitOperator(
    task_id="test_ftp",
    ftp_conn_id="ftp_default",
    local_filepath="./data/RAIS_VINC_PUB_SUL.7z",
    remote_filepath="/pdet/microdados/RAIS/2020/RAIS_VINC_PUB_SUL.7z",
    operation="get",
    
    dag=dag
)