import os
import yaml
from google import auth
import logging
logger = logging.getLogger()
class FileConfigParser:

    def __init__(self):
        #with open('C:/Users/vaishnavi.ruikar/PycharmProjects/Multicloud/project/Config/Config.yml') as fp:
        #    self._conf = yaml.safe_load(fp)
        with open('/opt/airflow/dags/repo/POC-AIRFLOW/project/cloud/Config/Config.yml') as fp:
            self._conf = yaml.safe_load(fp)

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


