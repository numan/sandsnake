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
from sandsnake.backends.base import BaseSunspearBackend
from sandsnake.exceptions import SandsnakeValidationException
from sandsnake.utils import OrderedSet

from nydus.db import create_cluster

from dateutil.parser import parse

import datetime
import calendar
import uuid


class Redis(BaseSunspearBackend):
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
        streams_added = []

        with self._backend.map() as conn:
            for stream in streams:
                stream_name = self._get_stream_name(obj, stream)
                streams_added.append(stream_name)
                conn.zadd(stream_name, activity, timestamp)
                conn.sadd(self._get_stream_collection_name(obj), stream)

        self._post_add_to_stream(obj, streams_added, activity, timestamp)

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
        streams_removed = []

        with self._backend.map() as conn:
            for stream in streams:
                stream_name = self._get_stream_name(obj, stream)
                streams_removed.append(stream_name)
                conn.zrem(stream_name, activity)

        self._post_delete_from_stream(obj, streams_removed, activity)

    def delete_stream(self, obj, stream_name):
        """
        Completely deletes the stream for an object

        :type obj: string
        :param obj: string representation of the object for who the stream belongs to
        :type stream_name: string or list of strings
        :param stream_name: the name of the stream(s) you want to delete
        """
        streams = self._listify(stream_name)
        streams_removed = []

        with self._backend.map() as conn:
            for stream in streams:
                stream_name = self._get_stream_name(obj, stream)
                streams_removed.append(stream_name)

                conn.delete(stream_name)
                conn.srem(self._get_stream_collection_name(obj), stream)
        #If the list is empty, there is no point in taking up more room.
        if self._backend.scard(self._get_stream_collection_name(obj)) == 0:
            self._backend.delete(self._get_stream_collection_name(obj))

        self._post_delete_stream(obj, streams_removed)

    def get_stream_items(self, obj, stream_name, marker=None, limit=30, after=False, **kwargs):
        """
        Gets a list of activities. Returns a maximum of ``limit`` stream items. If ``after`` is ``True``
        returns a list of activities after the marker.

        :type obj: string
        :param obj: string representation of the object for who the stream belongs to
        :type stream_name: string or list of strings
        :param stream_name: the name of the stream(s) you want to delete
        :type marker: string or datetime representing a date and a time
        :param marker: the starting point to retrieve activities from
        :type limit: int
        :param limit: the maximum number of activities to get
        :type after: boolean
        :param after: if ``True`` gets activities after ``marker`` otherwise gets it before ``marker``
        """
        if marker is None:
            raise SandsnakeValidationException("You must provide a marker to get stream items.")
        marker = self._parse_date(marker)
        timestamp = self._get_timestamp(marker)

        streams = self._listify(stream_name)

        results = []
        with self._backend.map() as conn:
            for stream in streams:
                if after:
                    results.append(conn.zrangebyscore(self._get_stream_name(obj, stream), timestamp, \
                        "+inf", start=0, num=limit, withscores=True, score_cast_func=long))
                else:
                    results.append(conn.zrevrangebyscore(self._get_stream_name(obj, stream), timestamp, \
                        "-inf", start=0, num=limit, withscores=True, score_cast_func=long))

        results = self._post_get_stream_items(results, obj, stream_name, marker, limit, after, **kwargs)

        if len(results) == 1:
            return results[0]
        return results

    def _post_get_stream_items(self, results, obj, stream_name, marker, limit, after, **kwargs):
        """
        Returns a list of activities after processing it.

        :type obj: string
        :param obj: string representation of the object for who the stream belongs to
        :type stream_name: string or list of strings
        :param stream_name: the name of the stream(s) you want to delete
        :type marker: string or datetime representing a date and a time
        :param marker: the starting point to retrieve activities from
        :type limit: int
        :param limit: the maximum number of activities to get
        :type after: boolean
        :param after: if ``True`` gets activities after ``marker`` otherwise gets it before ``marker``
        """
        processed_activities = []
        for result in results:
            processed_activities.append(map(lambda x: x[0], result))
        return processed_activities

    def _post_add_to_stream(self, obj, streams, activity, timestamp):
        """
        Called after an activity has been added to streams.

        :type obj: string
        :param obj: string representation of the object for who the stream belongs to
        :type streams: list
        :param streams: a list of ``streams`` to which the ``activity`` has been added
        :type activity: string
        :param activity: the name of the activity
        :type timestamp: the score of the activity
        :param timestamp: the score of the activity
        """
        pass

    def _post_delete_from_stream(self, obj, streams, activity):
        """
        Called after ``activity`` has been removed from ``streams``

        :type obj: string
        :param obj: string representation of the object for who the stream belongs to
        :type streams: list
        :param streams: a list of ``streams`` to which the ``activity`` has been added
        :type activity: string
        :param activity: the name of the activity
        """
        pass

    def _post_delete_stream(self, obj, streams):
        """
        Called after ``streams`` have been deleted.

        :type obj: string
        :param obj: string representation of the object for who the stream belongs to
        :type streams: list
        :param streams: a list of ``streams`` to which the ``activity`` has been added
        """
        pass

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


class RedisWithMarker(Redis):
    def __init__(self, *args, **kwargs):
        super(RedisWithMarker, self).__init__(*args, **kwargs)
        self._default_marker_name = kwargs.get('default_marker_name', "_ssdefault")

    def set_markers(self, obj, stream_name, markers_dict):
        """
        Allows you to set custom markers for a ``stream`` belonging to an ``obj`

        :type obj: string
        :param obj: string representation of the object for who the stream belongs to
        :type stream_name: string
        :param stream_name: the name of the stream you want to update the markers for
        :type markers_dict: dict
        :param markers_dict: a dictionary when they keys are the marker names and values are the marker's new value. If the marker does not exist, it will be created
        """
        parsed_marker_dict = {}
        for key, value in markers_dict.items():
            parsed_marker_dict[self._get_stream_marker_name(stream_name, marker_name=key)] = value

        self._backend.hmset(self._get_obj_markers_name(obj), parsed_marker_dict)

    def get_markers(self, obj, stream_name, marker, **kwargs):
        """
        Gets custom markers for a ``stream`` belonging to an ``obj``

        :type obj: string
        :param obj: string representation of the object for who the stream belongs to
        :type stream_name: string
        :param stream_name: the name of the stream you want to update the markers for
        :type marker: string or list
        :param marker: a string or a list of strings of the name of the markers you want
        """
        markers = self._listify(marker)

        marker_names = map(lambda marker: self._get_stream_marker_name(stream_name, marker_name=marker), markers)
        results = self._backend.hmget(self._get_obj_markers_name(obj), marker_names)

        parsed_results = [long(result) for result in results]
        if len(parsed_results) == 1:
            return parsed_results[0]
        return parsed_results

    def get_default_marker(self, obj, stream_name, **kwargs):
        """
        Gets the default marker for the ``stream`` belonging to an ``obj``

        :type obj: string
        :param obj: string representation of the object for who the stream belongs to
        :type stream_name: string
        :param stream_name: the name of the stream you want to update the markers for
        """
        marker_name = self._get_stream_marker_name(stream_name)
        result = self._backend.hget(self._get_obj_markers_name(obj), marker_name)

        return 0L if result is None else long(result)

    def _post_delete_stream(self, obj, streams):
        """
        Called after ``streams`` have been deleted.

        :type obj: string
        :param obj: string representation of the object for who the stream belongs to
        :type streams: list
        :param streams: a list of ``streams`` to which the ``activity`` has been added
        """
        super(RedisWithMarker, self)._post_delete_stream(obj, streams)
        with self._backend.map() as conn:
            for stream in streams:
                conn.hdel(self._get_obj_markers_name(obj), self._get_stream_marker_name(stream))

    def _post_get_stream_items(self, results, obj, stream_name, marker, limit, after, **kwargs):
        """
        Updates the default marker for streams.

        :type results: list
        :param results: list of activities for each of the stream
        :type obj: string
        :param obj: string representation of the object for who the stream belongs to
        :type stream_name: string or list of strings
        :param stream_name: the name of the stream(s) you want to delete
        :type marker: string or datetime representing a date and a time
        :param marker: the starting point to retrieve activities from
        :type limit: int
        :param limit: the maximum number of activities to get
        :type after: boolean
        :param after: if ``True`` gets activities after ``marker`` otherwise gets it before ``marker``
        """
        streams = self._listify(stream_name)

        if after:
            with self._backend.map() as conn:
                for index, stream in enumerate(streams):
                    if results[index]:
                        conn.hset(self._get_obj_markers_name(obj),\
                            self._get_stream_marker_name(stream), results[index][-1][1])

        return super(RedisWithMarker, self)._post_get_stream_items(results, obj, stream_name, marker, limit, after, **kwargs)

    def _get_obj_markers_name(self, obj):
        """
        Gets the unique name of the hash which stores the markers for this object

        :type obj: string
        :param obj: a unique string identifing the object
        """
        return "%(prefix)sobj:%(obj)s:markers" % {'prefix': self._prefix, 'obj': obj}

    def _get_stream_marker_name(self, stream, marker_name=None):
        """
        Gets the unique name of the marker for the stream. The name of the default marker for the
        stream is ``default``

        :type stream: string
        :param stream: a unique string identifing a stream
        :type marker_name: string
        :param marker_name: the name of the marker for this stream. The default name is ``default``
        """
        marker_name = marker_name if marker_name is not None else self._default_marker_name
        return "stream:%(stream)s:name:%(name)s" % {'stream': stream, 'name': marker_name}


class RedisWithBubbling(RedisWithMarker):

    def bubble_activities(self, obj, stream_name, activities_dict):
        """
        Moves activities up and down the sorted set based on score (in most cases, a timestamp)
        **NOTE:** If you decide to use timestamps, use UTC so you don't get screwed over by timezones.

        :type obj: string
        :param obj: a unique string identifing the object
        :type stream_name: string
        :param stream_name: a unique string identifing a stream
        :type activities_dict: dict
        :param activities_dict: a dictionary where keys are the keys for the activity and values are the new
        score for the activity. You can pass ``None`` as the score for any activity and it will assign the score
        to the current utc timestamp.
        """
        for key, value in activities_dict.items():
            #we we did not provide a custom score, then just set it the the current timestamp
            if value is None:
                score = self._get_timestamp(datetime.datetime.utcnow())
            else:
                #Try to parse the score as a long. If it doesn't work, try to parse a date.
                try:
                    score = long(value)
                except (ValueError, TypeError):
                    score = self._get_timestamp(self._parse_date(date=value))
            activities_dict[key] = score

        self._backend.zadd(self._get_stream_name(obj, stream_name), **activities_dict)
