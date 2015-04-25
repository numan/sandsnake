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
from sandsnake.backends.base import BaseSandsnakeBackend
from sandsnake.exceptions import SandsnakeValidationException

from nydus.db import create_cluster

from dateutil.parser import parse

import calendar
import datetime
import itertools


class Redis(BaseSandsnakeBackend):
    def __init__(self, settings, **kwargs):
        nydus_hosts = {}

        hosts = settings.get("hosts", [])
        if not hosts:
            raise Exception("No redis hosts specified")

        for i, host in enumerate(hosts):
            nydus_hosts[i] = host

        defaults = settings.get(
            "defaults",
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

    def get_backend(self):
        """
        returns the nydus backend
        """
        return self._backend

    def clear_all(self):
        """
        Deletes all ``sandsnake`` related data from redis.

        .. warning::

            Very expensive and destructive operation. Use with causion
        """
        keys = self._backend.keys()

        for key in itertools.chain(*keys):
            with self._backend.map() as conn:
                if key.startswith(self._prefix):
                    conn.delete(key)

    def get_count(self, obj, index, published, after=False):
        """
        Gets the number of items in the index. If ``after`` is ``False``,
        it gets the number of items in the index less than ``published``.

        If ``after`` is ``True``, it gets the number of items in the index
        greater than ``published``.

        :type obj: string
        :param obj: string representation of the object for who the index belongs to
        :type index: string
        :param index: the name of the index you want to add the activity to
        :type published: datetime
        :param published: the published within the index where we want to start the count
        :type after: boolean
        :param after: determins if we are dealing with after or before offset

        :return the number of index items before or after the offset
        """
        index_name = self._get_index_name(obj, index)
        published = self._parse_date(published)
        timestamp = self._get_timestamp(published)

        if after:
            start = timestamp
            end = "+inf"
        else:
            start = "-inf"
            end = timestamp

        return self._backend.zcount(index_name, start, end)

    def add(self, obj, index_name, activity, published=None):
        """
        Adds an activity to a index(s) of an object.

        :type obj: string
        :param obj: string representation of the object for who the index belongs to
        :type index_name: string or list of strings
        :param index_name: the name of the index(s) you want to add the activity to
        :type activity: string
        :param activity: string representation of the activity you want to add to the index(s)
        :type published: datetime
        :param published: the time this activity was published
        """
        if published is None:
            published = datetime.datetime.utcnow()
        published = self._parse_date(published)
        timestamp = self._get_timestamp(published)

        indexes = self._listify(index_name)
        indexes_added = []

        with self._backend.map() as conn:
            for index in indexes:
                index_name = self._get_index_name(obj, index)
                indexes_added.append(index_name)
                conn.zadd(index_name, timestamp, activity)
                conn.sadd(self._get_index_collection_name(obj), index)

        self._post_add(obj, indexes_added, activity, timestamp)

    def remove_values(self, obj, index_name, value):
        """
        Deletes activities from an index that belongs to a object

        :type obj: string
        :param obj: string representation of the object for who the index belongs to
        :type index_name: string
        :param index_name: the name of the index you want to delete the values from
        :type value: string or list
        :param value: string representation of the value you want to add to the index(s)
        """
        values = self._listify(value)
        index = self._get_index_name(obj, index_name)
        for value in values:
            self._backend.zrem(index, value)

        for value in values:
            self._post_remove(obj, [index], value)

    def remove(self, obj, index_name, activity):
        """
        Deletes an activity from a index or a list of indexes that belongs to a object

        :type obj: string
        :param obj: string representation of the object for who the index belongs to
        :type index_name: string or list of strings
        :param index_name: the name of the index(s) you want to delete the activity from
        :type activity: string
        :param activity: string representation of the activity you want to add to the index(s)
        """
        indexes = self._listify(index_name)
        indexes_removed = []

        with self._backend.map() as conn:
            for index in indexes:
                index_name = self._get_index_name(obj, index)
                indexes_removed.append(index_name)
                conn.zrem(index_name, activity)

        self._post_remove(obj, indexes_removed, activity)

    def delete_index(self, obj, index_name):
        """
        Completely deletes the index for an object

        :type obj: string
        :param obj: string representation of the object for who the index belongs to
        :type index_name: string or list of strings
        :param index_name: the name of the index(s) you want to delete
        """
        indexes = self._listify(index_name)
        indexes_removed = []

        with self._backend.map() as conn:
            for index in indexes:
                index_name = self._get_index_name(obj, index)
                indexes_removed.append(index_name)

                conn.delete(index_name)
                conn.srem(self._get_index_collection_name(obj), index)
        #If the list is empty, there is no point in taking up more room.
        if self._backend.scard(self._get_index_collection_name(obj)) == 0:
            self._backend.delete(self._get_index_collection_name(obj))

        self._post_delete_index(obj, indexes_removed)

    def get(self, obj, index_name, marker=None, limit=30, after=False, withscores=False, **kwargs):
        """
        Gets a list of values. Returns a maximum of ``limit`` index items. If ``after`` is ``True``
        returns a list of values after the marker.

        :type obj: string
        :param obj: string representation of the object for who the index belongs to
        :type index_name: string or list of strings
        :param index_name: the name of the index(s) you want to delete
        :type marker: string or datetime representing a date and a time
        :param marker: the starting point to retrieve values from
        :type limit: int
        :param limit: the maximum number of values to get
        :type after: boolean
        :param after: if ``True`` gets values after ``marker`` otherwise gets it before ``marker``
        :type withscores: boolean
        :param withscores: if ``True``, returns results as tuples where the second item is the score
        for that index item.
        """
        if marker is None:
            raise SandsnakeValidationException("You must provide a marker to get index items.")
        marker = self._parse_date(marker)
        timestamp = self._get_timestamp(marker)

        indexes = self._listify(index_name)

        results = []
        with self._backend.map() as conn:
            for index in indexes:
                if after:
                    results.append(conn.zrangebyscore(self._get_index_name(obj, index), timestamp, \
                        "+inf", start=0, num=limit, withscores=True, score_cast_func=long))
                else:
                    results.append(conn.zrevrangebyscore(self._get_index_name(obj, index), timestamp, \
                        "-inf", start=0, num=limit, withscores=True, score_cast_func=long))

        results = self._post_get(results, obj, index_name, marker, limit, \
            after, withscores, **kwargs)

        if len(results) == 1:
            return results[0]
        return results

    def _post_get(self, results, obj, index_name, marker, limit, after, withscores, **kwargs):
        """
        Returns a list of values after processing it.

        :type obj: string
        :param obj: string representation of the object for who the index belongs to
        :type index_name: string or list of strings
        :param index_name: the name of the index(s) you want to delete
        :type marker: string or datetime representing a date and a time
        :param marker: the starting point to retrieve values from
        :type limit: int
        :param limit: the maximum number of values to get
        :type after: boolean
        :param after: if ``True`` gets values after ``marker`` otherwise gets it before ``marker``
        :type withscores: boolean
        :param withscores: if ``True``, returns results as tuples where the second item is the score
        for that index item.
        """
        if withscores:
            processed_values = results
        else:
            processed_values = []
            for result in results:
                processed_values.append(map(lambda x: x[0], result))

        return processed_values

    def _post_add(self, obj, indexes, activity, timestamp):
        """
        Called after an activity has been added to indexes.

        :type obj: string
        :param obj: string representation of the object for who the index belongs to
        :type indexes: list
        :param indexes: a list of ``indexes`` to which the ``activity`` has been added
        :type activity: string
        :param activity: the name of the activity
        :type timestamp: the score of the activity
        :param timestamp: the score of the activity
        """
        pass

    def _post_remove(self, obj, indexes, activity):
        """
        Called after ``activity`` has been removed from ``indexes``

        :type obj: string
        :param obj: string representation of the object for who the index belongs to
        :type indexes: list
        :param indexes: a list of ``indexes`` to which the ``activity`` has been added
        :type activity: string
        :param activity: the name of the activity
        """
        pass

    def _post_delete_index(self, obj, indexes):
        """
        Called after ``indexes`` have been deleted.

        :type obj: string
        :param obj: string representation of the object for who the index belongs to
        :type indexes: list
        :param indexes: a list of ``indexes`` to which the ``activity`` has been added
        """
        pass

    def _get_index_name(self, obj, index):
        """
        Gets the unique index name for the obj index pair

        :type obj: string
        :param obj: string representation of the object for who the index belongs to
        :type index_name: string
        :param index_name: the name of the index
        """
        return "%(prefix)sobj:%(obj)s:index:%(index)s" % {'prefix': self._prefix, 'obj': obj, 'index': index}

    def _get_index_collection_name(self, obj):
        """
        Gets the unique name to the set that has all this objects indexes:

        :type obj: string
        :param obj: string representation of the object
        """
        return "%(prefix)s%(obj)s:indexes" % {'prefix': self._prefix, 'obj': obj}

    def _listify(self, list_or_string):
        """
        A simple helper that converts a single ``index_name`` into a list of 1

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

    def _get_timestamp(self, dt_obj):
        """
        returns a unix timestamp representing the ``datetime`` object
        """
        return long((calendar.timegm(dt_obj.utctimetuple()) * 1000)) + (dt_obj.microsecond / 1000)


class RedisWithMarker(Redis):
    def __init__(self, *args, **kwargs):
        super(RedisWithMarker, self).__init__(*args, **kwargs)
        self._default_marker_name = kwargs.get('default_marker_name', "_ssdefault")

    def set_markers(self, obj, index_name, markers_dict):
        """
        Allows you to set custom markers for a ``index`` belonging to an ``obj`

        :type obj: string
        :param obj: string representation of the object for who the index belongs to
        :type index_name: string
        :param index_name: the name of the index you want to update the markers for
        :type markers_dict: dict
        :param markers_dict: a dictionary when they keys are the marker names and values are the marker's new value. If the marker does not exist, it will be created
        """
        parsed_marker_dict = {}
        for key, value in markers_dict.items():
            parsed_marker_dict[self._get_index_marker_name(index_name, marker_name=key)] = value

        self._backend.hmset(self._get_obj_markers_name(obj), parsed_marker_dict)

    def get_markers(self, obj, index_name, marker, **kwargs):
        """
        Gets custom markers for a ``index`` belonging to an ``obj``

        :type obj: string
        :param obj: string representation of the object for who the index belongs to
        :type index_name: string
        :param index_name: the name of the index you want to update the markers for
        :type marker: string or list
        :param marker: a string or a list of strings of the name of the markers you want
        """
        markers = self._listify(marker)

        marker_names = map(lambda marker: self._get_index_marker_name(index_name, marker_name=marker), markers)
        results = self._backend.hmget(self._get_obj_markers_name(obj), marker_names)

        parsed_results = [(None if result is None else long(result)) for result in results]
        if len(parsed_results) == 1:
            return parsed_results[0]
        return parsed_results

    def get_default_marker(self, obj, index_name, **kwargs):
        """
        Gets the default marker for the ``index`` belonging to an ``obj``

        :type obj: string
        :param obj: string representation of the object for who the index belongs to
        :type index_name: string
        :param index_name: the name of the index you want to update the markers for
        """
        marker_name = self._get_index_marker_name(index_name)
        result = self._backend.hget(self._get_obj_markers_name(obj), marker_name)

        return None if result is None else long(result)

    def _post_delete_index(self, obj, indexes):
        """
        Called after ``indexes`` have been deleted.

        :type obj: string
        :param obj: string representation of the object for who the index belongs to
        :type indexes: list
        :param indexes: a list of ``indexes`` to which the ``activity`` has been added
        """
        super(RedisWithMarker, self)._post_delete_index(obj, indexes)
        with self._backend.map() as conn:
            for index in indexes:
                conn.hdel(self._get_obj_markers_name(obj), self._get_index_marker_name(index))

    def _get_obj_markers_name(self, obj):
        """
        Gets the unique name of the hash which stores the markers for this object

        :type obj: string
        :param obj: a unique string identifing the object
        """
        return "%(prefix)sobj:%(obj)s:markers" % {'prefix': self._prefix, 'obj': obj}

    def _get_index_marker_name(self, index, marker_name=None):
        """
        Gets the unique name of the marker for the index. The name of the default marker for the
        index is ``default``

        :type index: string
        :param index: a unique string identifing a index
        :type marker_name: string
        :param marker_name: the name of the marker for this index. The default name is ``default``
        """
        marker_name = marker_name if marker_name is not None else self._default_marker_name
        return "index:%(index)s:name:%(name)s" % {'index': index, 'name': marker_name}


class RedisWithBubbling(RedisWithMarker):

    def bubble_values(self, obj, index_name, values_dict):
        """
        Moves values up and down the sorted set based on score (in most cases, a timestamp)
        **NOTE:** If you decide to use timestamps, use UTC so you don't get screwed over by timezones.

        :type obj: string
        :param obj: a unique string identifing the object
        :type index_name: string
        :param index_name: a unique string identifing a index
        :type values_dict: dict
        :param values_dict: a dictionary where keys are the keys for the activity and values are the new
        score for the activity. You can pass ``None`` as the score for any activity and it will assign the score
        to the current utc timestamp.
        """
        for key, value in values_dict.items():
            #we we did not provide a custom score, then just set it the the current timestamp
            if value is None:
                score = self._get_timestamp(datetime.datetime.utcnow())
            else:
                #Try to parse the score as a long. If it doesn't work, try to parse a date.
                try:
                    score = long(value)
                except (ValueError, TypeError):
                    score = self._get_timestamp(self._parse_date(date=value))
            values_dict[key] = score

        self._backend.zadd(self._get_index_name(obj, index_name), **values_dict)
