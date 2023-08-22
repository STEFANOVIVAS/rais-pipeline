
from airflow.providers.ftp.operators.ftp import FTPFileTransmitOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import timedelta,datetime
from ftplib import FTP
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor


FTP_SERVER='ftp.mtps.gov.br'

ftp=FTP(FTP_SERVER)
ftp.login()
ftp.cwd('/pdet/microdados/RAIS/2020/')

list_files=ftp.nlst()


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
    dag_id="extract_rais_from_FTP",
       
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

    is_ftp_file_available = FileSensor(
        task_id="is_ftp_file_available",
        fs_conn_id="ftp_path",
        filepath="RAIS_VINC_PUB_SUL.7z",
        poke_interval=5,
        timeout=20
    )
    def extract_files():
        import py7zr
        archive = py7zr.SevenZipFile('./data/RAIS_VINC_PUB_SUL.7z', mode='r')
        archive.extractall(path="./data")
        archive.close()

    extract_files=PythonOperator(
        task_id="extract_7zip_files",
        python_callable=extract_files

    )

get_file >> is_ftp_file_available >> extract_files