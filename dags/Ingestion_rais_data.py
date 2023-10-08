from airflow.providers.ftp.operators.ftp import FTPFileTransmitOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import timedelta,datetime
from ftplib import FTP
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.google.cloud.transfers.azure_blob_to_gcs import AzureBlobStorageToGCSOperator
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator
from airflow.providers.microsoft.azure.transfers.local_to_adls import LocalFilesystemToADLSOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook,AzureDataLakeHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from azure.storage.filedatalake import DataLakeFileClient,DataLakeDirectoryClient 
import os



FTP_SERVER='ftp.mtps.gov.br'

ftp=FTP(FTP_SERVER)
ftp.login()
ftp.cwd('/pdet/microdados/RAIS/2020/')
arquivos=[]

list_files=ftp.nlst()
for file in list_files:
    if file.startswith("RAIS_VINC_PUB"):
        arquivos.append(file.split(".")[0])


# USER="anonymous"
# PASS="anonymous@"

def extract_7zip_files(file):
    import py7zr
    archive = py7zr.SevenZipFile(f'./data/{file}.7z', mode='r')
    archive.extractall(path="./data")
    archive.close()




default_args={
        "owner": "Stefano",
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "max_active_runs":2
        
       
    }


with DAG(
    dag_id="Ingestion_rais_data",
       
    description="RAIS files DAG",
    schedule=None,
    start_date=datetime(2023, 8, 19),
    catchup=False,
    tags=["rais"],
    default_args=default_args
) as dag:
    
    with TaskGroup(f'Ingestion_RAIS_layout') as Ingestion_RAIS_layout:
    
        get_rais_layout_file = FTPFileTransmitOperator(
                    task_id=f"Get_rais_layout_file_from_FTP_server",
                    ftp_conn_id="ftp_default",
                    local_filepath=f"./data/Rais_vinculos_layout2020.xls",
                    remote_filepath=f"/pdet/microdados/RAIS/Layouts/vínculos/Rais_vinculos_layout2020.xls",
                    operation="get")
        
        upload_from_local_to_blob_=LocalFilesystemToWasbOperator(
                        task_id=f"Upload_rais_layout_file_to_blob",
                        file_path='./data/Rais_vinculos_layout2020.xls',
                        container_name="bronze",
                        blob_name ='Rais_vinculos_layout2020.xls',
                        wasb_conn_id='azure_adls_conn'

                    )

        get_rais_layout_file >> upload_from_local_to_blob_
    
    with TaskGroup(f'Ingestion_RAIS_vinculos') as Ingestion_RAIS_vinculos:
    
        # get files from FTP 
        with TaskGroup(f'Download_ftp_files') as Download_files:
            
            for file in arquivos:

        
                get_rais_vinculos_file = FTPFileTransmitOperator(
                task_id=f"Get_{file}_from_FTP_server",
                ftp_conn_id="ftp_default",
                local_filepath=f"./data/{file}.7z",
                remote_filepath=f"/pdet/microdados/RAIS/2020/{file}.7z",
                operation="get",
                
                
            )
            
        with TaskGroup(f'Is_ftp_files_available') as Available_7zip_files:    
            
            for file in arquivos:
                is_ftp_file_available = FileSensor(
                    task_id=f"is_ftp_{file}_available_locally",
                    fs_conn_id="ftp_path",
                    filepath=f"{file}.7z",
                    poke_interval=10,
                    timeout=120
                )


        with TaskGroup(f'Extract_7zip_files') as Extract_files:
            
            for file in arquivos:
                extract_files=PythonOperator(
                    task_id=f"Extract_7zip_{file}",
                    python_callable=extract_7zip_files,
                    op_kwargs={'file': file},
                )

        with TaskGroup(f'Is_csv_files_available') as Available_csv:
            
            for file in arquivos:
            
                is_csv_file_extracted = FileSensor(
                    task_id=f"is_csv_{file}_extracted",
                    fs_conn_id="ftp_path",
                    filepath=f"{file}.txt",
                    poke_interval=10,
                    timeout=40
                )
        with TaskGroup(f'Zip_files_in_gzip_format') as Zip_files:
            
            for file in arquivos:
                
                zip_file=BashOperator(
                    task_id=f"Zip_{file}_from_data_folder",
                    bash_command=f""" 
                    
                                gzip /opt/airflow/data/{file}.txt;
                                
                    """
                )
        with TaskGroup(f'Delete_local_files') as Delete_files:
            
            for file in arquivos:
            
                delete_data=BashOperator(
                    task_id=f"Delete_{file}_from_data_folder",
                    bash_command=f""" 
                    
                                rm /opt/airflow/data/{file}.7z ;
                                
                    """
                )


        with TaskGroup(f'Upload_blob') as Upload_blob:
            
            for file in arquivos:
                
                 
                upload_from_local_to_wasb=LocalFilesystemToWasbOperator(
                    task_id=f"Upload_rais_vinculos_csv_{file}_to_blob",
                    file_path=f'./data/{file}.txt.gz',
                    container_name="bronze",
                    blob_name =f'{file}.txt.gz',
                    wasb_conn_id='azure_adls_conn'

                )
       
    Download_files >> Available_7zip_files >>Extract_files >> Available_csv
    Available_csv >> Zip_files >> Delete_files >> Upload_blob
       
    with TaskGroup(f'Is_files_available_blob') as Available_blob:
        
        for file in arquivos:
        
            wait_for_vinculos_objects_in_storage=WasbBlobSensor(
                task_id=f"wait_for_blob_{file}_in_storage",
                container_name="bronze",
                blob_name=f"{file}.txt.gz",
                wasb_conn_id='azure_adls_conn'
            )
        wait_for_layout_in_storage=WasbBlobSensor(
                task_id=f"wait_for_layout_in_storage",
                container_name="bronze",
                blob_name="Rais_vinculos_layout2020.xls",
                wasb_conn_id='azure_adls_conn'
            )
    

    databricks_load_fact_vinculos = DatabricksRunNowOperator(
    task_id = 'Ingestion_RAIS_data_blob',
    databricks_conn_id = 'databricks_default',
    job_id = "923938036859559"
  )
    databricks_load_dim_tables = DatabricksRunNowOperator(
    task_id = 'Ingestion_dim_municipios_ocupacao_atividade',
    databricks_conn_id = 'databricks_default',
    job_id = "230388160279966"
  )
   
    databricks_transform_data = DatabricksRunNowOperator(
    task_id = 'Join_vinculos_table_with_municipios_ocupacao_atividade',
    databricks_conn_id = 'databricks_default',
    job_id = "968113886196840"
  )



    [Ingestion_RAIS_vinculos,Ingestion_RAIS_layout] >> Available_blob >> databricks_load_fact_vinculos >> databricks_load_dim_tables >> databricks_transform_data


    

   
    


    
    
    # def upload_to_adls_gen2():
    #     azure_hook=AzureDataLakeStorageV2Hook('adls_conn')
    #     azure_hook.upload_file(
    #         file_system_name='rais-2020',
    #         file_name='RAIS_VINC_PUB_CENTRO_OESTE.txt.gz',
    #         file_path='./data/RAIS_VINC_PUB_CENTRO_OESTE.txt.gz',
    #     )

        

    # upload_from_local_to_adls=PythonOperator(
    #                 task_id=f"Upload_csv_to_blob",
    #                 python_callable=upload_to_adls_gen2,
                   

    #             )