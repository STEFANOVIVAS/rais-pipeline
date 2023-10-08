import os, uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient,BlobBlock



def upload_blobs_container(storage_account, container_name,local_path,file_name):

    try:
        print("Azure Blob Storage Python quickstart sample")
        account_url = f"https://{storage_account}.blob.core.windows.net"
        default_credential = DefaultAzureCredential()

        # Create the BlobServiceClient object
        blob_service_client = BlobServiceClient(account_url, credential=default_credential,)

        # container_name="rais-2020"
        
        # Create the container
        # container_client = blob_service_client.create_container(container_name)
        # Create a local directory to hold blob data
        # local_path = "./data"
        
        # Create a file in the local data directory to upload and download
        # file_name = "RAIS_VINC_PUB_CENTRO_OESTE.7z"
        upload_file_path = os.path.join(local_path, file_name)
        
        
        # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)

        print("\nUploading to Azure Storage as blob:\n\t" + file_name)
        
        # block_list=[]
        # chunk_size=1024*1024*64
        # with open(upload_file_path,'rb') as f:
        
        #     while True:
        #             read_data = f.read(chunk_size)
        #             if not read_data:
        #                 break # done
        #             blk_id = str(uuid.uuid4())
        #             blob_client.stage_block(block_id=blk_id,data=read_data) 
        #             block_list.append(BlobBlock(block_id=blk_id))
        

        # blob_client.commit_block_list(block_list)

      
        # Upload the created file
        with open(file=upload_file_path, mode="rb") as data:
            blob_client.upload_blob(data=data,blob_type="BlockBlob")

        # Quickstart code goes here

    except BaseException as err:
        print('Upload file error')
        print(err)

upload_blobs_container('rais2020','silver',"./data",'RAIS_VINC_PUB_SP.txt.gz')