import os
import yaml
from azure.storage.blob import ContainerClient

def load_config():
    dir_root = os.path.dir(os.path.abspath(__file__))
    with open(dir_root + "/config.yaml", "r") as yamlfile:
        return yaml.load(yamlfile, Loader = yaml.FullLoader)

config = load_config()
print(* config)