import os
import io
import pandas as pd
import logging
from azure.core.exceptions import (
    ResourceNotFoundError,
    ResourceExistsError,
    ServiceRequestError,
)

from importlib.util import spec_from_file_location, module_from_spec

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

# UTIL_PATH = os.path.abspath(os.path.join(__file__, "..", "..", "cloud"))
# FileConfigParser = load_module(UTIL_PATH, "config_parser1").FileConfigParser
# #CONFIG = config_parser.FileConfigParser
# #from config_parser1 import FileConfigParser
#
# CONFIG = FileConfigParser()
#
# load_module(UTIL_PATH, "config_parser1")


# import sys
# base_path = '/'.join(os.path.realpath(__file__).split(os.path.sep)[:-2])
# print(base_path)
# sys.path.append(base_path +'/cloud')
#from project.cloud.config_parser1 import FileConfigParser

#CONFIG = FileConfigParser()

f = open("unixmen.log", "w")
f.close()
logging.basicConfig(filename="unixmen.log", level=logging.DEBUG)

logger = logging.getLogger()

logger.info(
    "-------------Welcome to Cloud Storage Factory!!!!-------------------------------"
)

""" 
    AzureImplementation is a class which consists of the actual implementation of the tasks that are need to be 
    performed on the Azure Blob Storage provider.

    It consists of following implementation methods:

    1) def getconnection(self): 
    This method sets up a connection with Azure Blob Storage provider and returns a connection string as output.
    It takes Azure Storage account's Access key to establish the connection with Azure Storage provider. 

    2) read_from_bucket_using_pandas(self,blob_service_client,container_name,container): 
    This function reads the file from Azure Blob Storage container in pandas dataframe format and returns the head of the file in pandas dataframe.
    It takes blob_service_client, container_name and container as an input parameter which are defined in AzureProvider class.

    3) read_from_output_stream(self,blob_service_client,container_name,container):
    This function reads the file from Azure Blob Storage container in output stream format and returns the file.
    It takes blob_service_client, container_name and container as an input parameter which are defined in AzureProvider class.

    4) upload_to_bucket(self,blob_service_client,container_name,container):
    This Function Uploads 1 file to the Azure Blob Storage container from your local system provided path to file as an input.
    It takes blob_service_client, container_name and container as an input parameter which are defined in AzureProvider class.

"""


class AzureImplementation:
    def getconnection(self):
        # create connection_string to connect python code to Azure Blob Storage environment
        #os.environ['AZURE_CONNECTION_STRING'] = 'DefaultEndpointsProtocol=https\;AccountName=pocairflow\;AccountKey=4XO2wBFYeqbeaD2ZuQheQtOxbj5FIXviJKdOxEYe9EyQLcM0PHLItRYRRsRrSIVuf3yo1plIgkR0qqKpSSyuTw==\;EndpointSuffix=core.windows.net'
        #connection_string = os.getenv("AZURE_CONNECTION_STRING")
        connection_string = 'DefaultEndpointsProtocol=https;AccountName=pocairflow;AccountKey=4XO2wBFYeqbeaD2ZuQheQtOxbj5FIXviJKdOxEYe9EyQLcM0PHLItRYRRsRrSIVuf3yo1plIgkR0qqKpSSyuTw==;EndpointSuffix=core.windows.net'
        if len(connection_string) == 0:
            logger.error("ERROR: Please check connection String")
            raise Exception("Trouble in establishing connection")

        return connection_string

    def read_from_bucket_using_pandas(
            self, blob_client, file_format
    ):
        try:
            logger.info(
                "---------------Read from Azure Blob Storage Container using pandas df---------------"
            )
            # get data as blob
            data = blob_client.download_blob().readall()
            # read the file in pandas df
            if file_format == "csv":
                df = pd.read_csv(io.BytesIO(data), encoding="utf-8", sep=",")
            #elif file_format == "xlsx":
                #sheet_name = CONFIG.get('sheet_name')
            #    df = pd.read_excel(io.BytesIO(data), sheet_name=sheet_name)
            elif file_format == 'json':
                df = pd.read_json(io.BytesIO(data))
            else:
                logger.error("ERROR: File format not supported")
                raise Exception("Only .xlsx and .csv File format are supported")

            return df

        except ValueError:
            logger.error("ERROR: Please specify a blob name.")

        except ResourceNotFoundError:
            logger.error(
                "ERROR: The specified blob does not exist in Azure Blob Storage Container"
            )

        except ServiceRequestError:
            logger.error(f"ERROR: Invalid Url: Please check connection string")

        except Exception as e:
            logger.error(e, exc_info=True)
            raise Exception(
                "Function failure: Failed to Read from Azure Blob Storage Container using pandas df"
            )

    def read_from_output_stream(self, blob_client):
        try:
            logger.info(
                "---------------Read from Azure Blob Storage Container using output stream-------------------"
            )
            # get data as blob
            data = blob_client.download_blob().readall()
            # read the file in output stream format
            file_obj = io.BytesIO(data)
            file_data = file_obj.getvalue()
            # print the contents of the file
            logger.info(file_data)
            # return contents of file
            return file_data, blob_client.blob_name

        except ValueError:
            logger.error("ERROR: Please specify a blob name.")

        except ServiceRequestError:
            logger.error(f"ERROR: Invalid Url: Please check connection string")

        except ResourceNotFoundError:
            logger.error(
                "ERROR: The specified blob does not exist in Azure Blob Storage Container"
            )

        except Exception as e:
            logger.error(e, exc_info=True)
            raise Exception(
                "Function failure: Failed to Read from Azure Blob Storage Container using output stream"
            )

    def upload_to_bucket(self, blob_client, path_to_file):
        try:
            logger.info(
                "---------------Upload file to Azure Blob Storage Container -------------------"
            )
            # take path of the file you want to upload as input from user
            #path_to_file = CONFIG.get('local_file_path')
            # upload Blob to container
            with open(path_to_file, "rb") as data:
                blob_client.upload_blob(data)
            # check blob has been uploaded Successfully or not
            assert blob_client.exists()
            logger.info("File Uploaded Successfully!!!!!!")

        except ValueError:
            logger.error("ERROR: Please specify a blob name.")

        except ServiceRequestError:
            logger.error(f"ERROR: Invalid Url: Please check connection string")

        except ResourceExistsError:
            logger.error(
                "ERROR: The specified blob already exist in Azure Blob Storage Container"
            )

        except AssertionError:
            logger.error(f"ERROR: File Upload Failed!!!!!!")

        except OSError:
            logger.error(
                f"ERROR: Unable to fetch file, Invalid argument: {path_to_file} "
            )

        except Exception as e:
            logger.error(e, exc_info=True)
            raise Exception(
                "Function failure: Failed to Upload file to Azure Blob Storage Container"
            )


    def move_file_from_source_to_destination_bucket(self, blob_service_client,source_blob,source_container_name,source_blob_path,destination_container_name,dest_blob_path):
        try:
            # Target

            copied_blob = blob_service_client.get_blob_client(destination_container_name, dest_blob_path)
            copied_blob.start_copy_from_url(source_blob)

            # If you would like to delete the source file
            #remove_blob = blob_service_client.get_blob_client(source_container_name, source_blob_path)
            #remove_blob.delete_blob()

        except Exception as e:
            logger.error(e, exc_info=True)
            raise Exception(
                "Function failure: Failed to move_file_from_source_to_destination_bucket in Azure Blob Storage Container"
            )