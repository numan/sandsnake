"""
Copyright 2012 Numan Sachwani <numan@7Geese.com>

This file is provided to you under the Apache License,
Version 2.0 (the "License"); you may not use this file
except in compliance with the License.  You may obtain
a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""
from sandsnake.backends.base import BaseAnalyticsBackend

from nydus.db import create_cluster

from dateutil.relativedelta import relativedelta

import datetime
import itertools
import calendar
import types


class Redis(BaseAnalyticsBackend):
    def __init__(self, settings, **kwargs):
        nydus_hosts = {}

        hosts = settings.get("hosts", [])
        if not hosts:
            raise Exception("No redis hosts specified")

        for i, host in enumerate(hosts):
            nydus_hosts[i] = host

        defaults = settings.get("defaults",
            {
                'host': 'localhost',
                'port': 6379,
            })

        self.__backend = create_cluster({
            'engine': 'nydus.db.backends.redis.Redis',
            'router': 'nydus.db.routers.keyvalue.ConsistentHashingRouter',
            'hosts': nydus_hosts,
            'defaults': defaults,
        })


