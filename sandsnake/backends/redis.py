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

from dateutil.parser import parse

import datetime
import calendar
import uuid


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

        self._backend = create_cluster({
            'engine': 'nydus.db.backends.redis.Redis',
            'router': 'nydus.db.routers.keyvalue.ConsistentHashingRouter',
            'hosts': nydus_hosts,
            'defaults': defaults,
        })

        self._prefix = kwargs.get('prefix', "ssnake:")

    def add_to_stream(self, obj, stream_name, activity, published=None):
        """
        Adds an activity to a stream(s) of an object.

        :type obj: string
        :param obj: string representation of the object for who the stream belongs to
        :type stream_name: string or list of strings
        :param stream_name: the name of the stream(s) you want to add the activity to
        :type activity: string
        :param activity: string representation of the activity you want to add to the stream(s)
        :type published: datetime
        :param published: the time this activity was published
        """
        if published is None:
            published = datetime.datetime.utcnow()
        published = self._parse_date(published)
        timestamp = self._get_timestamp(published)

        streams = self._listify(stream_name)

        with self._backend.map() as conn:
            for stream in streams:
                conn.zadd(self._get_stream_name(obj, stream), activity, timestamp)
                conn.sadd(self._get_stream_collection_name(obj), stream)

    def delete_from_stream(self, obj, stream_name, activity):
        """
        Deletes an activity from a stream or a list of streams that belongs to a object

        :type obj: string
        :param obj: string representation of the object for who the stream belongs to
        :type stream_name: string or list of strings
        :param stream_name: the name of the stream(s) you want to delete the activity from
        :type activity: string
        :param activity: string representation of the activity you want to add to the stream(s)
        """
        streams = self._listify(stream_name)

        with self._backend.map() as conn:
            for stream in streams:
                conn.zrem(self._get_stream_name(obj, stream), activity)

    def delete_stream(self, obj, stream_name):
        """
        Completely deletes the stream for an object

        :type obj: string
        :param obj: string representation of the object for who the stream belongs to
        :type stream_name: string or list of strings
        :param stream_name: the name of the stream(s) you want to delete
        """
        streams = self._listify(stream_name)

        with self._backend.map() as conn:
            for stream in streams:
                conn.delete(self._get_stream_name(obj, stream))
                conn.srem(self._get_stream_collection_name(obj), stream)
        #If the list is empty, there is no point in taking up more room.
        if self._backend.scard(self._get_stream_collection_name(obj)) == 0:
            self._backend.delete(self._get_stream_collection_name(obj))

    def get_stream_union(self, obj, stream_name):
        """
        Gets a union of all activities in ``stream_name``

        :type obj: string
        :param obj: string representation of the object for who the stream belongs to
        :type stream_name: string or list of strings
        :param stream_name: the name of the stream(s)
        """
        stream_names = map(lambda x: self._get_stream_name(obj, x), self._listify(stream_name))

        return self._get_set_union(stream_names)[0]

    def _get_set_union(self, *args):
        """
        Given a set of stream sames (or any sorted sets), it stors the union of them in a new sorted set
        and returns the key.
        """
        streams_list = list(args)
        unions = []
        for streams in streams_list:
            stream_set = set()
            for stream in streams:
                stream_set.update(self._backend.zrange(stream, 0, -1))
            unions.append(stream_set)
        return unions

    def _get_stream_name(self, obj, stream):
        """
        Gets the unique stream name for the obj stream pair

        :type obj: string
        :param obj: string representation of the object for who the stream belongs to
        :type stream_name: string
        :param stream_name: the name of the stream
        """
        return "%(prefix)sobj:%(obj)s:stream:%(stream)s" % {'prefix': self._prefix, 'obj': obj, 'stream': stream}

    def _get_stream_collection_name(self, obj):
        """
        Gets the unique name to the set that has all this objects streams:

        :type obj: string
        :param obj: string representation of the object
        """
        return "%(prefix)s%(obj)s:streams" % {'prefix': self._prefix, 'obj': obj}

    def _listify(self, list_or_string):
        """
        A simple helper that converts a single ``stream_name`` into a list of 1

        :type list_or_string: string or list
        :param list_or_string: the name of things as a string or a list of strings
        """
        if isinstance(list_or_string, basestring):
            list_or_string = [list_or_string]
        else:
            list_or_string = list_or_string

        return list_or_string

    def _parse_date(self, date=None):
        """
        Makes a best effort to convert ``date`` into a datetime object, resorting to ``datetime.datetime.utcnow()`` when everything fales

        :type date: anything
        :param date: something that represents a date and a time
        """
        dt = None
        if date is None or not isinstance(date, datetime.datetime):
            if isinstance(date, basestring):
                try:
                    dt = parse(date)
                except ValueError:
                    dt = datetime.datetime.utcnow()
            else:
                dt = datetime.datetime.utcnow()
        else:
            dt = date
        return dt

    def _get_timestamp(self, datetime):
        """
        returns a unix timestamp representing the datetime object
        """
        return long(str(calendar.timegm(datetime.timetuple())) + datetime.strftime("%f")[:4])

    def _get_new_uuid(self):
        return uuid.uuid1().hex
