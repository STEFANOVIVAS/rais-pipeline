
from airflow.providers.ftp.operators.ftp import FTPFileTransmitOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import timedelta,datetime
from ftplib import FTP
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator


# FTP_SERVER='ftp.mtps.gov.br'

# ftp=FTP(FTP_SERVER)
# ftp.login()
# ftp.cwd('/pdet/microdados/RAIS/2020/')

# list_files=ftp.nlst()


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
    task_id="Get_file_from_FTP_server",
    ftp_conn_id="ftp_default",
    local_filepath="./data/RAIS_VINC_PUB_SUL.7z",
    remote_filepath="/pdet/microdados/RAIS/2020/RAIS_VINC_PUB_SUL.7z",
    operation="get",
    dag=dag
    
)

    is_ftp_file_available = FileSensor(
        task_id="is_ftp_file_available_locally",
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
        task_id="Extract_7zip_files",
        python_callable=extract_files

    )

    is_txt_file_extracted = FileSensor(
        task_id="is_csv_file_extracted",
        fs_conn_id="ftp_path",
        filepath="RAIS_VINC_PUB_SUL.txt",
        poke_interval=5,
        timeout=20
    )

    upload_from_local_to_adls=LocalFilesystemToWasbOperator(
        task_id="Upload_csv_file_to_blob",
        file_path='./data/RAIS_VINC_PUB_SUL.txt',
        container_name="rais",
        blob_name="vinculos.csv",
        wasb_conn_id='azure_adls_conn'

    )

    delete_data=BashOperator(
        task_id="Delete_from_data_folder",
        bash_command=""" 
        
                    rm /opt/airflow/data/RAIS_VINC_PUB_SUL.7z ;
                    rm /opt/airflow/data/RAIS_VINC_PUB_SUL.txt;
        """
    )



get_file >> is_ftp_file_available >> extract_files >> is_txt_file_extracted >> upload_from_local_to_adls
upload_from_local_to_adls >> delete_data


