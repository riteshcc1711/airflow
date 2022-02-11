import os
import yaml
import gcsfs
from google import auth
import logging
logger = logging.getLogger()
# Any change made to the below classes here should be copied into
# /deploy/util/config_parser.py for respective classes
class YamlJoin(yaml.YAMLObject):
    """ Class to add a `!join` tag in yaml files
    The tag should function as a string concatenator
    Example:
        ---
        foo: &VAR bar
        val: !join [*VAR, buzz]
        ---
        This yaml should return:
        foo: bar
        vl: barbuzz
    """
    yaml_loader = yaml.SafeLoader
    yaml_tag = '!join'
    @classmethod
    def from_yaml(cls, loader, node):
        return ''.join([str(i) for i in loader.construct_sequence(node)])
class Readers:
    """ Class to hold different readers
    """
    class GcsConfigReader:
        """ Reads file from GCS bucket
        Args:
            root_dir (str): absolute uri to the root directory
                of config files
        """
        def __init__(self, root_dir):
            self._root = root_dir
            self._project_id = auth.default()[1]
        def _join(self, *args):
            """ Creates complete path to objects.
            Args:
                Accepts positional args
            Returns:
                (str): complete path to gcs object
            """
            return "/".join([self._root, *args])
        def load_yaml(self, *args):
            """ Reads a yaml file
            Args:
                Accepts positional args
            Returns:
                (dict) loaded config
            """
            path = self._join(*args)
            gcs = gcsfs.GCSFileSystem(project=self._project_id)
            try:
                with gcs.open(path) as fp:
                    conf = yaml.safe_load(fp)
            except Exception:
                logger.debug(f"Exception while loading file: {path}")
                raise
            return conf
        def read(self, *args):
            """ Reads a text file
            Args:
                Accepts positional args
            Returns:
                (str) contents of the file
            """
            path = self._join(*args)
            gcs = gcsfs.GCSFileSystem(project=self._project_id)
            try:
                with gcs.open(path, "r") as fp:
                    conf = fp.read()
            except Exception:
                logger.debug(f"Exception while loading file: {path}")
                raise
            return conf
    class OsConfigReader:
        """ Reads file from OS file system
        Args:
            root_dir (str): absolute path to the root directory
                of config files
        """
        def __init__(self, root_dir):
            self._root = root_dir
        def _join(self, *args):
            """ Creates complete path to objects.
            Args:
                Accepts positional args
            Returns:
                (str): complete path to os object
            """
            return os.path.join(self._root, *args)
        def load_yaml(self, *args):
            """ Reads a yaml file
            Args:
                Accepts positional args
            Returns:
                (dict) loaded config
            """
            path = self._join(*args)
            try:
                with open(path) as fp:
                    conf = yaml.safe_load(fp)
            except Exception:
                logger.error(f"Exception while loading file: {path}")
                raise
            return conf
        def read(self, *args):
            """ Reads a text file
            Args:
                Accepts positional args
            Returns:
                (str) contents of the file
            """
            path = self._join(*args)
            try:
                with open(path) as fp:
                    conf = fp.read()
            except Exception:
                logger.debug(f"Exception while loading file: {path}")
                raise
            return conf
class BaseConfigParser:
    """ Base config parser
    Args:
        root_dir (str): absolute uri/path to the root
            directory of config files
    """
    def __init__(self, root_dir):
        if not root_dir:
            raise ValueError("No value passed for config ROOT_DIR")
        self._reader = self._get_reader(root_dir)
    @staticmethod
    def _get_reader(root):
        """Assign the correct reader.
        """
        if root.startswith("gs://"):
            return Readers.GcsConfigReader(root)
        else:
            return Readers.OsConfigReader(root)
class FileConfigParser(BaseConfigParser):
    """ Read config from a file within root directory
    Args:
        root_dir (str): absolute uri/path to the root
            directory of config files
        file_name (str): file name within root directory
    """
    def __init__(self, root_dir, file_name):
        super().__init__(root_dir)
        self._conf = self._reader.load_yaml(file_name)
    def get(self, section):
        """ Return a section or nested key of config
        Example: a = {'a': {'b': {'c': 2}}}
        get('a') = {'b': {'c': 2}}
        get('a.b') = {'c': 2}
        get('a.b.c') = 2
        Args:
            section (str): section name
        Returns:
            (Any | NoneType) section config or None if not found
        Raises:
            AttributeError if intermediate section is missing
        """
        conf = self._conf
        tokens = section.split(".")
        if len(tokens) == 1:
            return conf.get(tokens[0])
        out = conf.get(tokens[0])
        try:
            for key in tokens[1:]:
                out = out.get(key)
        except AttributeError:
            logger.error(f"Section not found: {section}")
            raise
        return out
class DirConfigParser(BaseConfigParser):
    """Read config from a file within a subdirectory of
        root folder
    Args:
        root_dir (str): absolute uri/path to the root
            directory of config files
        parent_dir (str): subdirectory in which config files lie
    """
    def __init__(self, root_dir, parent_dir):
        super().__init__(root_dir)
        self._dir = parent_dir
    def get(self, file_name):
        """ Reads a file content/config from subdirectory
        Args:
            file_name (str): file name
        Returns:
            (dict | str) content of config/file
        """
        reader = self._reader
        if file_name.endswith(".yaml"):
            conf = reader.load_yaml(self._dir, file_name)
        elif file_name.endswith(".sql") or file_name.endswith(".j2"):
            conf = reader.read(self._dir, file_name)
        else:
            raise Exception(
                "Unexpected config file type. Expected: .yaml, .j2, or .sql"
            )
        return conf