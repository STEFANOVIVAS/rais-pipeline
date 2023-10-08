from azure.storage.filedatalake import DataLakeFileClient,DataLakeDirectoryClient,DataLakeServiceClient
import os

data = './data/RAIS_VINC_PUB_CENTRO_OESTE.txt.gz'
directory_client= DataLakeDirectoryClient.from_connection_string("BlobEndpoint=https://raispipeline.blob.core.windows.net/;QueueEndpoint=https://raispipeline.queue.core.windows.net/;FileEndpoint=https://raispipeline.file.core.windows.net/;TableEndpoint=https://raispipeline.table.core.windows.net/;SharedAccessSignature=sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-10-08T00:54:47Z&st=2023-09-29T16:54:47Z&spr=https&sig=aO1uNtXjAfd1uQgwUC8lu8QvmDSIF9HYi3KW83Dj%2Bsw%3D",
                                                 file_system_name="rais-2020", directory_name="silver")

print(directory_client.exists())

def upload_file_to_directory(directory_client: DataLakeDirectoryClient, local_path: str, file_name: str):
    file_client = directory_client.get_file_client(file_name)

    with open(file=os.path.join(local_path, file_name), mode="rb") as data:
        file_client.upload_data(data, overwrite=True)

upload_file_to_directory(directory_client,'./data','PNADC_2021_visita5_com_dividendos.csv')

