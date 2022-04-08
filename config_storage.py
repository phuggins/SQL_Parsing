
# -----------------------------
###  Run this file to test the connection to blob storage
# -----------------------------


import os
import yaml
from azure.storage.blob import BlobServiceClient

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

# Connect to Azure Storage Explorer and create service client
def storage_connect():
    try:
        blob_service_client = BlobServiceClient.from_connection_string(config['azure_storage_connectionstring'])
        container_client = blob_service_client.get_container_client(config['container'])
        print ("Successfully connected to Azure Storage Explorer")
        return container_client
    except:
        print ("Connection unsuccessful. Check connection string and container in config.yaml")

storage_connect()