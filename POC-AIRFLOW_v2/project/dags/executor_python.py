import sys
from pprint import pprint
pprint(sys.path)
import os
#base_path = '/'.join(os.path.realpath(__file__).split(os.path.sep)[:-1])
#print(base_path)
#sys.path.append(base_path +'/cloud')
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

UTIL_PATH = os.path.abspath(os.path.join(__file__, "..", "..", "cloud"))
#CloudStorageFactory = load_module(UTIL_PATH, "CloudStorage").CloudStorageFactory
#config_parser = load_module(UTIL_PATH, "CloudStorage")
#DEFAULTS = config_parser.DEFAULTS
#CloudProvider = CloudStorageFactory.CloudProvider
CloudStorageFactory = load_module(UTIL_PATH, "CloudStorage").CloudStorageFactory

#from project.cloud.CloudStorage import CloudStorageFactory

def func():
    #CloudStorage = CloudStorageFactory()
    CloudProvider = CloudStorageFactory().get_provider()
    CloudProvider.upload_to_bucket()
    CloudProvider.read_validate_move_file_from_s2d_bucket()

#func()