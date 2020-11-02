from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os

def import_from_blob(container_name,local_file_name, downloadfilepathandname):
    try:
        # Retrieve the connection string for use with the application. The storage
        # connection string is stored in an environment variable on the machine
        # running the application called AZURE_STORAGE_CONNECTION_STRING. If the environment variable is
        # created after the application is launched in a console or with Visual Studio,
        # the shell or application needs to be closed and reloaded to take the
        # environment variable into account.
        
        connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        
        # Create the BlobServiceClient object which will be used to connect a container client
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        # Define where the file will be downloaded
        #downloadfilepathandname = 'appended_output'

        #get the container location
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)

        print("\Downloading historic NR daily log data from Azure Storage as blob:\t" + local_file_name)
        #down the file with a context handler
        with open(downloadfilepathandname, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())

    except Exception as ex:
        print('Exception:')
        print(ex)


def export_to_blob(source_path,source_file_name,container_name):
    try:
        
        # Retrieve the connection string for use with the application. The storage
        # connection string is stored in an environment variable on the machine
        # running the application called AZURE_STORAGE_CONNECTION_STRING. If the environment variable is
        # created after the application is launched in a console or with Visual Studio,
        # the shell or application needs to be closed and reloaded to take the
        # environment variable into account.
        connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

        # Create the BlobServiceClient object which will be used to connect a container client
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        # Define the container
        container_client = blob_service_client.get_container_client(container_name)

        # Create a file in local data directory to upload and download
        local_path = source_path
        local_file_name = source_file_name
        upload_file_path = os.path.join(local_path, local_file_name)

        # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)

        print("\nUploading NR daily log data to Azure Storage as blob:\t" + local_file_name)

        # Upload the created file
        with open(upload_file_path, "rb") as data:
            blob_client.upload_blob(data,overwrite=True)

    except Exception as ex:
        print('Exception:')
        print(ex)
