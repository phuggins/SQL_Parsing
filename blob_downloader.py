# -----------------------------
###  Run this file to download the files in blob to a local path
###  Required because we cannot read the files directly from blob storage
# -----------------------------

# # download_blobs_parallel.py
# # Python program to bulk download blobs from azure storage
# # Uses latest python SDK() for Azure blob storage
# # Requires python 3.6 or above
# # Base Code: https://www.quickprogrammingtips.com/azure/how-to-download-blobs-from-azure-storage-using-python.html
 
import os
import yaml
import shutil
from multiprocessing.pool import ThreadPool
from azure.storage.blob import BlobServiceClient
#from azure.storage.blob import ContentSettings, ContainerClient, BlobClient

# Load config file
def load_config():
    try:
        dir_root = os.path.dirname(os.path.abspath(__file__))
        with open(dir_root + "/config.yaml", "r") as yamlfile:
            print ("Config file loaded successfully")
            return yaml.load(yamlfile, Loader = yaml.FullLoader)
    except:
            print ("Config file error")

config = load_config()

# Delete folder if exists
mydir = config['local_path']
if os.path.exists(mydir) and os.path.isdir(mydir):
    shutil.rmtree(mydir)
    print ("Deleted folder")
else:
    print ("Folder does not exist yet")

# set variables to be used in the loader - replace with vars from config file
connection_string = config["azure_storage_connectionstring"]
top_level_container = config['container']
target_subfolder = config['target_subfolder']
local_save_path = config['local_path']

# Downloader
class AzureBlobFileDownloader:
  def __init__(self):
    print("Intializing AzureBlobFileDownloader")
 
    # Initialize the connection to Azure storage account
    self.blob_service_client =  BlobServiceClient.from_connection_string(connection_string)
    self.my_container = self.blob_service_client.get_container_client(top_level_container)
 
  def download_all_blobs_in_container(self):
    # get a list of blobs
    my_blobs = self.my_container.list_blobs(target_subfolder)
    result = self.run(my_blobs)
    print(result)
 
  def run(self,blobs):
    # Download 10 files at a time via threading
    with ThreadPool(processes=int(10)) as pool:
     return pool.map(self.save_blob_locally, blobs)
 
  def save_blob_locally(self,blob):
    file_name = blob.name
    print(file_name)
    bytes = self.my_container.get_blob_client(blob).download_blob().readall()

    # Get full path to the file
    download_file_path = os.path.join(local_save_path, file_name)

    # for nested blobs, create local path as well
    os.makedirs(os.path.dirname(download_file_path), exist_ok=True)

    with open(download_file_path, "wb") as file:
      file.write(bytes)
    return file_name

# # Initialize class and download DDLs
def run_downloader():
    try:
        azure_blob_file_downloader = AzureBlobFileDownloader()
        azure_blob_file_downloader.download_all_blobs_in_container()
        print ("Folder successfully created")
        print ("Files downloaded successfully")
    except PermissionError: # Files already exist but we're overwriting them so this passes.
        print ("Files downloaded successfully")
    except FileExistsError:
      print("Cannot create file, please delete folder manually and wait a few seconds before running again")

run_downloader()
