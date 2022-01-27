import pandas as pd
from importlib.util import spec_from_file_location, module_from_spec
from google.cloud import storage
print("google storage installed!!!!!!!!!!!!!!!!!")
# import sys
import os

def load_module(root, file_name):
    """Load a file_name from root folder
    Args:
        root (str): absolute path to the filder where the file is located
        file_name (str): name of file in root folder which
            needs to be loaded. Exclude .py file extension.
    Returns:
        (module) file loaded as a module
    """
    module_path = os.path.join(root, f"{file_name}.py")
    spec = spec_from_file_location(file_name, module_path)
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

from azure.storage.blob import ContainerClient, BlobClient, BlobServiceClient
import logging
from google.api_core.exceptions import (
    NotFound,
    Forbidden,
    BadRequest,
)
from azure.core.exceptions import (
    ResourceNotFoundError,
    HttpResponseError,
)


UTIL_PATH = os.path.abspath(os.path.join(__file__, "..", "..", "cloud"))
FileConfigParser = load_module(UTIL_PATH, "config_parser1").FileConfigParser
AzureImplementation = load_module(UTIL_PATH + '/Azure', "Azure").AzureImplementation
GcpImplementation = load_module(UTIL_PATH + '/GCP', "GCP").GcpImplementation
Validate = load_module(UTIL_PATH, "validate_file").Validate


# from project.cloud.GCP.GCP import GcpImplementation
# from project.cloud.Azure.Azure import AzureImplementation
# from project.cloud.validate_file import Validate
# from project.cloud.config_parser1 import FileConfigParser

CONFIG = FileConfigParser()

f = open("unixmen.log", "w")
f.close()
logging.basicConfig(filename="unixmen.log", level=logging.DEBUG)

logger = logging.getLogger()

logger.info(
    "-------------Welcome to Cloud Storage Factory!!!!-------------------------------"
)

"""
    CloudStorageFactory is an abstract factory which consists of unimplementated getter functions as follows:

    1) def __init__(self):
    2) def getFromBucketUsingPandas(self): 
    3) def getFromBucketUsingOutputStream(self):
    4) def getUploadToBucket(self):
    5) def getprovider(self,path): returns cloud provider based on the path url

    which are further implemented by GCPProvider i.e Google Cloud Storage Factory and AzureProvider i.e Azure Blob Storage Factory

"""


class CloudStorageFactory:
    def __init__(self):
        pass

    def read_from_bucket_using_pandas(self):
        pass

    def read_from_bucket_using_output_stream(self):
        pass

    def upload_to_bucket(self):
        pass

    def read_validate_move_file_from_s2d_bucket(self):
        pass

    def get_provider(self):

        ProviderType = CONFIG.get('cloud_provider')
        # Setting defaults environment for CGP
        if ProviderType == None:
            return GCPProvider()
        elif ProviderType == "gcp":
            return GCPProvider()
        elif ProviderType == "azure":
            return AzureProvider()
        else:
            raise Exception("Unknown Provider Requested")


""" 
    GCPProvider is a Google Cloud Storage Factory which implements CloudStorageFactory which is an abstract factory :
    It consists of get methods that will return an instance of GcpImplementation class to get the actual implementation.

    The Following are the get methods of GCPProvider class:

    1) def __init__(self):
    This function implements the method of CloudStorageFactory. It gets the connection to the Google Cloud Storage Bucket
    from GcpImplementation().getconnection() and stores it in storage_client variable.
    It also creates a bucket variable by using storage_client.get_bucket(bucket_name) function which is then furthur
    passed as an input 

    eter to implementation Functions in GCPImplementation class.

    2) def getFromBucketUsingPandas(self):
    This function returns a call to the method i.e read_from_bucket_using_pandas in GCPImplementation class 
    and gets the Implementation of read_from_bucket_using_pandas function 

    3) def getFromBucketUsingOutputStream(self):
    This function returns a call to the method i.e read_from_output_stream in GCPImplementation class 
    and gets the Implementation of read_from_output_stream function 

    4) def getUploadToBucket(self):
    This function returns a call to the method i.e upload_to_bucket in GCPImplementation class 
    and gets the Implementation of upload_to_bucket function 


"""


class GCPProvider(CloudStorageFactory):
    def __init__(self):
        try:
            logger.info(
                "-------------Welcome to Google Cloud Storage!!!!--------------------"
            )
            self.storage_client = GcpImplementation().getconnection()

        except IndexError:
            logger.error(
                f"ERROR: Bucket name is empty: string index out of range"
            )
            exit()
        except BadRequest:
            logger.error(
                f"ERROR: Bucket names must be at least 3 characters in length"
            )
            exit()
        except Forbidden:
            logger.error(
                f"ERROR: Request is prohibited by organization's policy"
            )
            exit()

    def read_from_bucket_using_pandas(self):
        try:

            bucket_name = CONFIG.get('source_bucket_name')
            blob_name = CONFIG.get('file_name')
            li1 = []
            li1 = blob_name.split(".")
            file_format = li1[-1]
            bucket = self.storage_client.get_bucket(bucket_name)
            filename = list(bucket.list_blobs(prefix=""))
            logger.info("Listing Blobs in the bucket:")
            for name in filename:
                logger.info(name.name)
            return GcpImplementation().read_from_bucket_using_pandas(
                bucket, blob_name, file_format
            )
        except NotFound:
            logger.error(f"ERROR: Bucket {bucket_name} not found")
            exit()
        except IndexError:
            logger.error(
                f"ERROR: Bucket name is empty: string index out of range"
            )
            exit()
        except BadRequest:
            logger.error(
                f"ERROR: Bucket names must be at least 3 characters in length"
            )
            exit()
        except Forbidden:
            logger.error(
                f"ERROR: Request is prohibited by organization's policy"
            )
            exit()
        except AttributeError:
            logger.error(
                f"ERROR: 'GCPProvider' object has no attribute 'bucket'"
            )

    def read_from_bucket_using_output_stream(self):
        try:
            bucket_name = CONFIG.get('source_bucket_name')
            blob_name = CONFIG.get('file_name')
            bucket = self.storage_client.get_bucket(bucket_name)
            return GcpImplementation().read_from_output_stream(
                bucket, blob_name
            )
        except AttributeError:
            logger.error(
                f"ERROR: 'GCPProvider' object has no attribute 'bucket'"
            )

    def upload_to_bucket(self):
        try:
            bucket_name = CONFIG.get('destination_bucket_name')
            blob_name = CONFIG.get('file_name')
            path_to_file = CONFIG.get('local_file_path')
            sheet_name = CONFIG.get('sheet_name')
            bucket = self.storage_client.get_bucket(bucket_name)
            filename = list(bucket.list_blobs(prefix=""))
            logger.info("Listing Blobs in the bucket:")
            for name in filename:
                logger.info(name.name)
            return GcpImplementation().upload_to_bucket(
                bucket, blob_name, path_to_file, sheet_name
            )
        except NotFound:
            logger.error(f"ERROR: Bucket {bucket} not found")
            exit()
        except IndexError:
            logger.error(
                f"ERROR: Bucket name is empty: string index out of range"
            )
            exit()
        except BadRequest:
            logger.error(
                f"ERROR: Bucket names must be at least 3 characters in length"
            )
            exit()
        except Forbidden:
            logger.error(
                f"ERROR: Request is prohibited by organization's policy"
            )
            exit()

        except AttributeError:
            logger.error(
                f"ERROR: 'GCPProvider' object has no attribute 'bucket'"
            )

    def read_validate_move_file_from_s2d_bucket(self):
        try:

            source_bucket_name = CONFIG.get('source_bucket_name')
            destination_bucket_name = CONFIG.get('destination_bucket_name')
            blob_name = CONFIG.get('file_name')
            source_bucket = self.storage_client.get_bucket(source_bucket_name)
            destination_bucket = self.storage_client.get_bucket(destination_bucket_name)
            source_blob = source_bucket.blob(blob_name)

            # step1: read
            li1 = []
            li1 = blob_name.split(".")
            file_format = li1[-1]
            df = GcpImplementation().read_from_bucket_using_pandas(source_bucket, blob_name, file_format)

            # step2: validate
            result = Validate().validate_json(df)
            if result == 'pass':
                # step3: move file from one bucket to another
                return GcpImplementation().move_file_from_source_to_destination_bucket(
                    source_bucket, source_blob, destination_bucket, blob_name)
            else:
                logger.error("ERROR: File validation failed. Recheck the file!")
                raise Exception("Only .csv and .json File validation are supported")

        except Exception as e:
            logger.error(e, exc_info=True)
            raise Exception(
                "Function failure: Failed to move_file_from_source_to_destination_bucket in Google Cloud Storage"
            )


""" 
    AzureProvider is a Azure Blob Storage Factory which implements CloudStorageFactory which is an abstract factory :
    It consists of get methods that will return an instance of AzureImplementation class to get the actual implementation.

    The Following are the get methods of AzureProvider class:

    1) def __init__(self):
    This function implements the method of CloudStorageFactory. It gets the connection to the a Azure Blob Storage Container
    from AzureImplementation().getconnection() and stores it in connection_string variable.
    It also creates a container client and blob storage client variable which is then furthur passed as an input parameter 
    to implementation Functions in AzureImplementation class.

    2) def getFromBucketUsingPandas(self):
    This function returns a call to the method i.e read_from_bucket_using_pandas in AzureImplementation class 
    and gets the Implementation of read_from_bucket_using_pandas function 

    3) def getFromBucketUsingOutputStream(self):
    This function returns a call to the method i.e read_from_output_stream in AzureImplementation class 
    and gets the Implementation of read_from_output_stream function 

    4) def getUploadToBucket(self):
    This function returns a call to the method i.e upload_to_bucket in AzureImplementation class 
    and gets the Implementation of upload_to_bucket function 


"""


class AzureProvider(CloudStorageFactory):
    def __init__(self):
        try:
            logger.info(
                "-------------Welcome to Azure Blob Storage!!!!--------------------"
            )
            self.connection_string = AzureImplementation().getconnection()

        except ValueError:
            logger.error(f"Error: Please specify a container name")
            exit()

        except ResourceNotFoundError:
            logger.error(f"ERROR: Container not found")
            exit()

        except HttpResponseError:
            logger.error(
                f"Error: The specified resource name length is not within the permissible limits"
            )
            exit()

    def read_from_bucket_using_pandas(self):
        try:
            # li = []
            # li = path.split("https://")[1].split("/", 1)
            # path = li[1]
            # bucket_name, blob_name = path.split("/", 1)
            container_name = CONFIG.get('source_bucket_name')
            blob_name = CONFIG.get('file_name')
            #sheet_name = CONFIG.get('sheet_name')
            li1 = []
            li1 = blob_name.split(".")
            file_format = li1[-1]
            self.container = ContainerClient.from_connection_string(
                conn_str=self.connection_string,
                container_name=container_name,
            )
            logger.info("Listing Blobs in the Container:")
            for blob in self.container.list_blobs():
                logger.info(blob.name)
            logger.info(
                "Reading blob {!s} using Pandas df".format(blob_name)
            )
            blob_client = BlobClient.from_connection_string(
                conn_str=self.connection_string,
                container_name=container_name,
                blob_name=blob_name,
            )
            print('blob_client',blob_client)

            return AzureImplementation().read_from_bucket_using_pandas(
                blob_client, file_format
            )
        except AttributeError:
            logger.error(
                f"ERROR:'AzureProvider' object has no attribute 'container'"
            )

    def read_from_bucket_using_output_stream(self):
        try:
            container_name = CONFIG.get('source_bucket_name')
            blob_name = CONFIG.get('file_name')
            logger.info(
                "Reading blob {!s} using Output Stream".format(blob_name)
            )
            blob_client = BlobClient.from_connection_string(
                conn_str=self.connection_string,
                container_name=container_name,
                blob_name=blob_name,
            )
            return AzureImplementation().read_from_output_stream(blob_client)
        except AttributeError:
            logger.error(
                f"ERROR:'AzureProvider' object has no attribute 'container'"
            )

    def upload_to_bucket(self):
        try:
            blob_name = CONFIG.get('file_name')
            container_name = CONFIG.get('destination_bucket_name')
            path_to_file = CONFIG.get('local_file_path')
            self.container = ContainerClient.from_connection_string(
                conn_str=self.connection_string,
                container_name=container_name,
            )
            logger.info("Listing Blobs in the Container:")
            for blob in self.container.list_blobs():
                logger.info(blob.name)
            logger.info(
                "Uploading blob {!s} to Azure Blob Storage Container".format(
                    blob_name
                )
            )
            blob_client = BlobClient.from_connection_string(
                conn_str=self.connection_string,
                container_name=container_name,
                blob_name=blob_name,
            )
            return AzureImplementation().upload_to_bucket(
                blob_client, path_to_file)


        except ValueError:
            logger.error(f"Error: Please specify a container name")
            exit()

        except ResourceNotFoundError:
            logger.error(f"ERROR: Container not found")
            exit()

        except HttpResponseError:
            logger.error(
                f"Error: The specified resource name length is not within the permissible limits"
            )
            exit()
        except AttributeError:
            logger.error(
                f"ERROR:'AzureProvider' object has no attribute 'container'"
            )


    def read_validate_move_file_from_s2d_bucket(self):
        try:

            source_blob = CONFIG.get('source_file_url')
            source_blob_path = CONFIG.get('source_file_path')
            dest_blob_path = CONFIG.get('destination_file_path')
            source_container_name = CONFIG.get('source_bucket_name')
            destination_container_name = CONFIG.get('destination_bucket_name')
            blob_name = CONFIG.get('file_name')
            blob_client = BlobClient.from_connection_string(
                conn_str=self.connection_string,
                container_name=source_container_name,
                blob_name=blob_name,
            )
            blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)


            # step1: read
            li1 = []
            li1 = blob_name.split(".")
            file_format = li1[-1]
            df = AzureImplementation().read_from_bucket_using_pandas(blob_client, file_format)

            # step2: validate
            result = Validate().validate_json(df)
            if result == 'pass':
                # step3: move file from one bucket to another
                return AzureImplementation().move_file_from_source_to_destination_bucket(
                    blob_service_client, source_blob, source_container_name, source_blob_path,
                    destination_container_name, dest_blob_path)
            else:
                logger.error("ERROR: File validation failed. Recheck the file!")
                raise Exception("Only .csv and .json File validation are supported")


        except Exception as e:
            logger.error(e, exc_info=True)
            raise Exception(
                "Function failure: Failed to move_file_from_source_to_destination_bucket in Azure blob Storage"
            )

#CloudStorage = CloudStorageFactory()
#CloudProvider = CloudStorage.get_provider()
#CloudProvider.read_validate_move_file_from_s2d_bucket()

