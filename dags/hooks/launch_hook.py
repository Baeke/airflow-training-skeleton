# hook gets lauches from an api

import os
import pathlib
import subprocess
import re
import time
import posixpath
import requests
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from airflow.utils import apply_defaults
from airflow.utils.log.logging_mixin import LoggingMixin


class LaunchHook(BaseHook, LoggingMixin):
    """

    """
    template_fields = ('_query', '_destination')

    @apply_defaults
    def __init__(self,
                 query='',
                 destination=''):

        self._query = query
        self._destination = destination
        if self._query or self._destination is None:
            raise RuntimeError(
                "query and destination should be specified!")

    def _download_rocket_launches(self, ds, tomorrow_ds, **context):
        self._query = f"https://launchlibrary.net/1.4/launch?startdate={ds}&enddate={tomorrow_ds}"
        self._destination = f"/tmp/rocket_launches/ds={ds}"
        pathlib.Path(self._destination).mkdir(parents=True, exist_ok=True)
        response = requests.get(self._query)
        print(f"response was {response}")

        with open(posixpath.join(self._destination, "launches.json"), "w") as f:
            print(f"Writing to file {f.name}")
            f.write(response.text)

