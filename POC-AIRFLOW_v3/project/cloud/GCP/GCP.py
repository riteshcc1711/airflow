from google.cloud import storage
import os
import io
import pandas as pd
import logging
from google.api_core.exceptions import (
    BadRequest,
)

# import sys
# base_path = '/'.join(os.path.realpath(__file__).split(os.path.sep)[:-2])
# print(base_path)
# sys.path.append(base_path +'/cloud')
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

#CONFIG = FileConfigParser()

f = open("unixmen.log", "w")
f.close()
logging.basicConfig(filename="unixmen.log", level=logging.DEBUG)

logger = logging.getLogger()

logger.info(
    "-------------Welcome to Cloud Storage Factory!!!!-------------------------------"
)

""" 
    GCPImplementation is a class which consists of the actual implementation of the tasks that are need to be 
    performed on the Google cloud storage provider.

    It consists of following implementation methods:

    1) def read_from_bucket_using_pandas(self,bucket):
    This method sets up a connection with Google cloud storage provider and returns a storage client as output.
    It takes a application_default_credentials.json file as an input which consists of all the credentials of the Google cloud storage provider.
    It takes gcp bucket as an input parameter which is defined in the GCPProvider class.

    2) def read_from_output_stream(self,bucket):
    This function reads the files from Google Cloud Storage bucket in pandas dataframe format and returns the head of the file in pandas dataframe.
    It takes gcp bucket as an input parameter which is defined in the GCPProvider class.

    3) def read_from_output_stream(self,bucket):
    This function reads the files from Google Cloud Storage bucket in output stream format and returns the file.
    It takes gcp bucket as an input parameter which is defined in the GCPProvider class.

    4) def upload_to_bucket(self,bucket):
    This Function Uploads 1 file to the Google Cloud Storage bucket from your local system provided path to file as an input.
    It takes gcp bucket as an input parameter which is defined in the GCPProvider class.

"""

class GcpImplementation:
    def getconnection(self):
        # create storage client to connect python code to Google cloud storage environment
        path_to_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if len(path_to_json) == 0:
            logger.error(
                "ERROR: Please check GOOGLE_APPLICATION_CREDENTIALS String"
            )
            raise Exception("Trouble in establishing connection")
        storage_client = storage.Client(path_to_json)
        # print(storage_client)
        # return storage client for connection
        return storage_client

    def read_from_bucket_using_pandas(
            self, bucket, blob_name, file_format
    ):
        try:
            logger.info(
                "---------------Read from Google Cloud Storage bucket using pandas df---------------"
            )
            # take blob name as input from user
            logger.info("Reading blob {!s} using Pandas df".format(blob_name))
            # get data as blob
            blob = bucket.blob(blob_name)
            assert blob.exists()
            data = blob.download_as_string()

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
            # read the file in pandas df

            # print the contents of the file
            # logger.info(df.head())
            # logger.info(df.columns)
            return df

        except ValueError:
            logger.error(f"ERROR: Cannot determine path without a blob name.")

        except AssertionError:
            logger.error(
                f"ERROR: The specified blob does not exist in GCP bucket: {blob_name}"
            )

        except Exception as e:
            logger.error(e, exc_info=True)
            raise Exception(
                "Function failure: Failed to Read from Google Cloud Storage bucket using pandas df"
            )

    def read_from_output_stream(self, bucket, blob_name):
        try:
            logger.info(
                "---------------Read from Google Cloud Storage bucket using output stream-------------------"
            )

            # take blob name as input from user
            logger.info(
                "Reading blob {!s} using Output Stream".format(blob_name)
            )
            # get data as blob
            blob = bucket.blob(blob_name)
            assert blob.exists()
            # read file in Output Stream format
            file_obj = io.BytesIO()
            blob.download_to_file(file_obj)
            file_data = file_obj.getvalue()
            # print the contents of the file
            logger.info(file_data)
            # return contents of file
            return file_data, blob_name

        except ValueError:
            logger.error(f"ERROR: Cannot determine path without a blob name.")

        except AssertionError:
            logger.error(
                f"ERROR: The specified blob does not exist in GCP bucket: {blob_name}"
            )

        except Exception as e:
            logger.error(e, exc_info=True)
            raise Exception(
                "Function failure: Failed to Read from Google Cloud Storage bucket using output stream"
            )

    def upload_to_bucket(self, bucket, blob_name, path_to_file):
        try:
            logger.info(
                "---------------Upload file to Google Cloud Storage bucket -------------------"
            )
            # take blob name as input from user

            logger.info(
                "Uploading blob {!s} in Google Cloud Storage bucket".format(
                    blob_name
                )
            )
            # take path of the file you want to upload as input from user
            #path_to_file = CONFIG.get('local_file_path')
            # get data as blob
            blob = bucket.blob(blob_name)
            filename = list(bucket.list_blobs(prefix=""))
            for name in filename:
                if name.name == blob_name:
                    logger.warning(
                        f"WARNING: File {blob_name} Already Exists in GCP Bucket"
                    )
                    return
            # upload Blob to bucket
            blob.upload_from_filename(path_to_file)
            # check blob has been uploaded Successfully or not
            assert blob.exists()
            logger.info("File Uploaded Successfully!!!")
            return blob.public_url

        except BadRequest:
            logger.error(f"ERROR: Please specify blob name")
        except AssertionError:
            logger.error(f"ERROR: File Upload Failed!!!!!!")
        except OSError:
            logger.error(
                f"ERROR: Unable to fetch file, Invalid argument: {path_to_file} "
            )
        except Exception as e:
            logger.error(e, exc_info=True)
            raise Exception(
                "Function failure: Failed to Upload file to Google Cloud Storage bucket"
            )

    def move_file_from_source_to_destination_bucket(self,source_bucket,source_blob,destination_bucket,blob_name):
        try:

            new_blob = source_bucket.copy_blob(source_blob, destination_bucket, blob_name)
            source_blob.delete()

        except Exception as e:
            logger.error(e, exc_info=True)
            raise Exception(
                "Function failure: Failed to move_file_from_source_to_destination_bucket in Google Cloud Storage"
            )
