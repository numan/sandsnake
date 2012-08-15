from __future__ import absolute_import

from nose.tools import ok_, eq_, raises, set_trace

from sandsnake import create_sandsnake_backend
from sandsnake.exceptions import SandsnakeValidationException

import datetime


class TestRedisBackend(object):
    def setUp(self):
        self._backend = create_sandsnake_backend({
            "backend": "sandsnake.backends.redis.Redis",
            "settings": {
                "hosts": [{"db": 3}, {"db": 4}, {"db": 5}]
            },
        })

        self._redis_backend = self._backend.get_backend()

        #clear the redis database so we are in a consistent state
        self._redis_backend.flushdb()

    def tearDown(self):
        self._redis_backend.flushdb()

    def test_get_timestamp(self):
        jan_first = datetime.datetime(2012, 01, 01, 12, 0, 0, 0)
        eq_(13254192000000L, self._backend._get_timestamp(jan_first))

    def _setup_basic_stream(self):
        self.obj = "streams"
        self.stream_name = "profile_stream"
        self.activity_name = "activity1234"
        published = datetime.datetime.utcnow()
        self.timestamp = self._backend._get_timestamp(published)

        self._backend.add_to_stream(self.obj, self.stream_name, self.activity_name, published=published)

    def test_parse_date(self):
        datetime_string = "2012-01-01T12:00:00"
        datetime_obj = datetime.datetime(2012, 01, 01, 12, 0, 0, 0)
        invalid_datetime = "qwerty"

        now = datetime.datetime.utcnow()

        eq_(datetime.datetime(2012, 1, 1, 12, 0, 0, 0), self._backend._parse_date(datetime_string))
        eq_(datetime.datetime(2012, 1, 1, 12, 0, 0, 0), self._backend._parse_date(datetime_obj))
        ok_(now < self._backend._parse_date(invalid_datetime))  # should return the current datetime
        ok_(now < self._backend._parse_date())  # Should return the current datetime

    def test_listify(self):
        single_stream = 'my_stream'
        many_streams = ['second_stream', 'third_stream']

        eq_([single_stream], self._backend._listify(single_stream))
        eq_(many_streams, self._backend._listify(many_streams))

    def test_get_stream_collection_name(self):
        obj = "user:1234"
        eq_("%(prefix)s%(obj)s:streams" % {'prefix': self._backend._prefix, 'obj': obj}, self._backend._get_stream_collection_name(obj))

    def test_get_stream_name(self):
        obj = "user:1234"
        stream_name = "profile_stream"
        eq_("%(prefix)sobj:%(obj)s:stream:%(stream)s" % {'prefix': self._backend._prefix, 'obj': obj, 'stream': stream_name}, self._backend._get_stream_name(obj, stream_name))

    def test_get_stream_name_collection_name_dont_clash(self):
        obj = "streams"
        stream_name = "profile_stream"
        ok_(self._backend._get_stream_collection_name(obj) != self._backend._get_stream_name(obj, stream_name))

    def test_add_to_stream(self):
        obj = "streams"
        stream_name = "profile_stream"
        activity_name = "activity1234"
        published = datetime.datetime.utcnow()
        timestamp = self._backend._get_timestamp(published)

        self._backend.add_to_stream(obj, stream_name, activity_name, published=published)

        eq_(self._redis_backend.zcard(self._backend._get_stream_name(obj, stream_name)), 1)
        eq_(self._redis_backend.scard(self._backend._get_stream_collection_name(obj)), 1)
        eq_(self._redis_backend.zrange(self._backend._get_stream_name(obj, stream_name), 0, -1, withscores=True), [(activity_name, timestamp)])

    def test_add_to_multiple_streams_at_the_same_time(self):
        obj = "streams"
        stream_names = ["profile_stream", "group_stream"]
        activity_name = "activity1234"
        published = datetime.datetime.utcnow()

        self._backend.add_to_stream(obj, stream_names, activity_name, published=published)

        eq_(self._redis_backend.zcard(self._backend._get_stream_name(obj, stream_names[0])), 1)
        eq_(self._redis_backend.zcard(self._backend._get_stream_name(obj, stream_names[1])), 1)
        eq_(self._redis_backend.scard(self._backend._get_stream_collection_name(obj)), 2)

    def test_delete_from_stream(self):
        self._setup_basic_stream()

        eq_(self._redis_backend.zcard(self._backend._get_stream_name(self.obj, self.stream_name)), 1)
        eq_(self._redis_backend.scard(self._backend._get_stream_collection_name(self.obj)), 1)
        eq_(self._redis_backend.zrange(self._backend._get_stream_name(self.obj, self.stream_name), 0, -1, withscores=True), [(self.activity_name, self.timestamp)])

        #now delete from the stream:
        self._backend.delete_from_stream(self.obj, self.stream_name, self.activity_name)

        eq_(self._redis_backend.zcard(self._backend._get_stream_name(self.obj, self.stream_name)), 0)
        eq_(self._redis_backend.scard(self._backend._get_stream_collection_name(self.obj)), 1, "we don't get rid of the stream unless the user explicitly asks to delete it.")

    def test_delete_stream(self):
        self._setup_basic_stream()

        eq_(self._redis_backend.zcard(self._backend._get_stream_name(self.obj, self.stream_name)), 1)
        eq_(self._redis_backend.scard(self._backend._get_stream_collection_name(self.obj)), 1)

        self._backend.delete_stream(self.obj, self.stream_name)

        ok_(not self._redis_backend.exists(self._backend._get_stream_name(self.obj, self.stream_name)))
        ok_(not self._redis_backend.exists(self._backend._get_stream_collection_name(self.obj)))

    def test_delete_stream_multiple_streams_at_the_same_time(self):
        obj = "streams"
        stream_names = ["profile_stream", "group_stream"]
        activity_name = "activity1234"
        published = datetime.datetime.utcnow()

        self._backend.add_to_stream(obj, stream_names, activity_name, published=published)

        eq_(self._redis_backend.zcard(self._backend._get_stream_name(obj, stream_names[0])), 1)
        eq_(self._redis_backend.zcard(self._backend._get_stream_name(obj, stream_names[1])), 1)
        eq_(self._redis_backend.scard(self._backend._get_stream_collection_name(obj)), 2)

        self._backend.delete_stream(obj, stream_names)

        ok_(not self._redis_backend.exists(self._backend._get_stream_name(obj, stream_names[0])))
        ok_(not self._redis_backend.exists(self._backend._get_stream_name(obj, stream_names[1])))
        ok_(not self._redis_backend.exists(self._backend._get_stream_collection_name(obj)))

    def test_delete_stream_multiple_streams_some_not_deleted(self):
        obj = "streams"
        stream_names = ["profile_stream", "group_stream"]
        activity_name = "activity1234"
        published = datetime.datetime.utcnow()

        self._backend.add_to_stream(obj, stream_names, activity_name, published=published)

        eq_(self._redis_backend.zcard(self._backend._get_stream_name(obj, stream_names[0])), 1)
        eq_(self._redis_backend.zcard(self._backend._get_stream_name(obj, stream_names[1])), 1)
        eq_(self._redis_backend.scard(self._backend._get_stream_collection_name(obj)), 2)

        self._backend.delete_stream(obj, stream_names[0])

        ok_(not self._redis_backend.exists(self._backend._get_stream_name(obj, stream_names[0])))
        ok_(self._redis_backend.exists(self._backend._get_stream_name(obj, stream_names[1])))
        ok_(self._redis_backend.exists(self._backend._get_stream_collection_name(obj)))
        eq_(self._redis_backend.scard(self._backend._get_stream_collection_name(obj)), 1)

    def test_delete_stream_stream_object_doesnt_exist(self):
        #fail silently if the streams/objects don't exist
        self._backend.delete_stream("non existing", "also does not exist")

    def test_delete_stream_stream_doesnt_exist(self):

        self._setup_basic_stream()

        eq_(self._redis_backend.zcard(self._backend._get_stream_name(self.obj, self.stream_name)), 1)
        eq_(self._redis_backend.scard(self._backend._get_stream_collection_name(self.obj)), 1)

        self._backend.delete_stream(self.obj, "this doesn't really exist")

        eq_(self._redis_backend.zcard(self._backend._get_stream_name(self.obj, self.stream_name)), 1)
        eq_(self._redis_backend.scard(self._backend._get_stream_collection_name(self.obj)), 1)

    def test_get_stream_difference(self):
        lhs_obj = "user:1"
        lhs_stream_names = ["profile_stream", "group_stream"]
        lhs_activities = [['a1', 'a2', 'a3'],  ['b1', 'b2', 'b3']]

        rhs_obj = "user:2"
        rhs_stream_names = ["profile_stream", "group_stream"]
        rhs_activities = [['a4', 'a2', 'a3'], ['b1', 'b4', 'b3']]

        #first stream
        self._backend.add_to_stream(lhs_obj, lhs_stream_names[0], lhs_activities[0][0])
        self._backend.add_to_stream(lhs_obj, lhs_stream_names[0], lhs_activities[0][1])
        self._backend.add_to_stream(lhs_obj, lhs_stream_names[0], lhs_activities[0][2])

        #second stream
        self._backend.add_to_stream(lhs_obj, lhs_stream_names[1], lhs_activities[1][0])
        self._backend.add_to_stream(lhs_obj, lhs_stream_names[1], lhs_activities[1][1])
        self._backend.add_to_stream(lhs_obj, lhs_stream_names[1], lhs_activities[1][2])

        #first stream
        self._backend.add_to_stream(rhs_obj, rhs_stream_names[0], rhs_activities[0][0])
        self._backend.add_to_stream(rhs_obj, rhs_stream_names[0], rhs_activities[0][1])
        self._backend.add_to_stream(rhs_obj, rhs_stream_names[0], rhs_activities[0][2])

        #second stream
        self._backend.add_to_stream(rhs_obj, rhs_stream_names[1], rhs_activities[1][0])
        self._backend.add_to_stream(rhs_obj, rhs_stream_names[1], rhs_activities[1][1])
        self._backend.add_to_stream(rhs_obj, rhs_stream_names[1], rhs_activities[1][2])

        eq_(self._backend.get_stream_union(lhs_obj, lhs_stream_names) - self._backend.get_stream_union(rhs_obj, rhs_stream_names), set(['a1', 'b2']))
        eq_(self._backend.get_stream_union(lhs_obj, lhs_stream_names[0]) - self._backend.get_stream_union(rhs_obj, rhs_stream_names[1]), set(['a1', 'a2', 'a3']))
        eq_(self._backend.get_stream_union(lhs_obj, lhs_stream_names[0]) - self._backend.get_stream_union(rhs_obj, rhs_stream_names[0]), set(['a1']))

    def test_get_stream_items(self):
        published = datetime.datetime.now()
        obj = "streams"
        stream_name = "profile_stream"

        for i in xrange(5):
            self._backend.add_to_stream(obj, stream_name, "activity_after_" + str(i), published=published + datetime.timedelta(seconds=i))

        for i in xrange(1, 3):
            self._backend.add_to_stream(obj, stream_name, "activity_before_" + str(i), published=published - datetime.timedelta(seconds=i))

        #get all activities after the marker
        result = self._backend.get_stream_items(obj, stream_name, marker=published)

        eq_(len(result), 3)
        eq_(['activity_after_0'] + ["activity_before_" + str(i) for i in xrange(1, 3)], result)

        #get all activities before the marker
        result = self._backend.get_stream_items(obj, stream_name, marker=published, after=True)

        eq_(len(result), 5)
        eq_(["activity_after_" + str(i) for i in xrange(5)], result)

    def test_get_stream_items_limit(self):
        published = datetime.datetime.now()
        obj = "streams"
        stream_name = "profile_stream"

        for i in xrange(5):
            self._backend.add_to_stream(obj, stream_name, "activity_after_" + str(i), published=published + datetime.timedelta(seconds=i))

        for i in xrange(1, 3):
            self._backend.add_to_stream(obj, stream_name, "activity_before_" + str(i), published=published - datetime.timedelta(seconds=i))

        #get all activities after the marker
        result = self._backend.get_stream_items(obj, stream_name, marker=published, limit=2)

        eq_(len(result), 2)
        eq_(['activity_after_0', 'activity_before_1'], result)

        #get all activities before the marker
        result = self._backend.get_stream_items(obj, stream_name, marker=published, after=True, limit=2)

        eq_(len(result), 2)
        eq_(["activity_after_" + str(i) for i in xrange(2)], result)

    @raises(SandsnakeValidationException)
    def test_get_stream_items_marker_required(self):
        obj = "streams"
        stream_name = "profile_stream"

        self._backend.get_stream_items(obj, stream_name)

    def test_post_get_stream_items(self):
        eq_(self._backend._post_get_stream_items([[('act:1', 1,), ('act:2', 2, ), ('act:3', 3)]],\
            "obj1", "stream1", 123, 20, False), [['act:1', 'act:2', 'act:3']])


class TestRedisWithMarkerBackend(object):
    def setUp(self):
        self._backend = create_sandsnake_backend({
            "backend": "sandsnake.backends.redis.RedisWithMarker",
            "settings": {
                "hosts": [{"db": 3}, {"db": 4}, {"db": 5}]
            },
        })

        self._redis_backend = self._backend.get_backend()

        #clear the redis database so we are in a consistent state
        self._redis_backend.flushdb()

    def tearDown(self):
        self._redis_backend.flushdb()

    def test_get_obj_markers_name(self):
        obj = "user123"

        eq_("%(prefix)sobj:%(obj)s:markers" % {'prefix': self._backend._prefix, 'obj': obj}, self._backend._get_obj_markers_name(obj))

    def test_get_stream_marker_name_default_name(self):
        stream = "userstream"

        eq_("stream:%(stream)s:name:%(name)s" % {'stream': stream, 'name': '_ssdefault'}, self._backend._get_stream_marker_name(stream))

    def test_get_stream_marker_name_custom_name(self):
        stream = "userstream"
        marker_name = "custom"

        eq_("stream:%(stream)s:name:%(name)s" % {'stream': stream, 'name': marker_name}, self._backend._get_stream_marker_name(stream, marker_name=marker_name))

    def test_delete_stream_remove_marker(self):
        obj = "streams"
        stream_name = "profile_stream"
        published = datetime.datetime.utcnow()
        timestamp = self._backend._get_timestamp(published)
        initial_marker_hash = {'stream:ssnake:obj:streams:stream:profile_stream:name:_ssdefault': str(timestamp)}

        self._redis_backend.hmset(self._backend._get_obj_markers_name(obj), initial_marker_hash)
        markers_hash = self._redis_backend.hgetall(self._backend._get_obj_markers_name(obj))
        eq_(initial_marker_hash, markers_hash)

        self._backend.delete_stream(obj, stream_name)
        markers_hash = self._redis_backend.hgetall(self._backend._get_obj_markers_name(obj))
        eq_({}, markers_hash)

    def test_delete_multiple_streams_at_the_same_time_markers_updated(self):
        obj = "streams"
        stream_names = ["profile_stream", "group_stream"]
        published = datetime.datetime.utcnow()
        timestamp = self._backend._get_timestamp(published)
        initial_marker_hash = {
            'stream:ssnake:obj:streams:stream:profile_stream:name:_ssdefault': str(timestamp), \
            'stream:ssnake:obj:streams:stream:group_stream:name:_ssdefault': str(timestamp)
        }

        self._redis_backend.hmset(self._backend._get_obj_markers_name(obj), initial_marker_hash)
        markers_hash = self._redis_backend.hgetall(self._backend._get_obj_markers_name(obj))
        eq_(initial_marker_hash, markers_hash)

        self._backend.delete_stream(obj, stream_names)
        markers_hash = self._redis_backend.hgetall(self._backend._get_obj_markers_name(obj))
        eq_({}, markers_hash)

    def test_get_stream_items_before_marker_doesnt_updates_marker(self):
        published = datetime.datetime.now()
        obj = "streams"
        stream_name = "profile_stream"

        for i in xrange(5):
            self._backend.add_to_stream(obj, stream_name, "activity_after_" + str(i), published=published + datetime.timedelta(seconds=i))

        for i in xrange(1, 3):
            self._backend.add_to_stream(obj, stream_name, "activity_before_" + str(i), published=published - datetime.timedelta(seconds=i))

        eq_(self._redis_backend.hget(self._backend._get_obj_markers_name(obj),\
            self._backend._get_stream_marker_name(stream_name)), None)

        #get all activities after the marker
        result = self._backend.get_stream_items(obj, stream_name, marker=published)

        eq_(len(result), 3)
        eq_(self._redis_backend.hget(self._backend._get_obj_markers_name(obj),\
            self._backend._get_stream_marker_name(stream_name)), None)

    def test_get_stream_items_after_marker_updates_marker(self):
        published = datetime.datetime.now()
        obj = "streams"
        stream_name = "profile_stream"

        for i in xrange(5):
            self._backend.add_to_stream(obj, stream_name, "activity_after_" + str(i), published=published + datetime.timedelta(seconds=i))

        for i in xrange(1, 5):
            self._backend.add_to_stream(obj, stream_name, "activity_before_" + str(i), published=published - datetime.timedelta(seconds=i))

        eq_(self._redis_backend.hget(self._backend._get_obj_markers_name(obj),\
            self._backend._get_stream_marker_name(stream_name)), None)

        #get all activities before the marker
        result = self._backend.get_stream_items(obj, stream_name, marker=published, after=True, limit=3)

        eq_(len(result), 3)
        eq_(long(self._redis_backend.hget(self._backend._get_obj_markers_name(obj),\
            self._backend._get_stream_marker_name(stream_name))), self._backend._get_timestamp(published + datetime.timedelta(seconds=2)))

    def get_default_marker(self):
        published = datetime.datetime.now()
        obj = "streams"
        stream_name = "profile_stream"

        for i in xrange(5):
            self._backend.add_to_stream(obj, stream_name, "activity_after_" + str(i), published=published + datetime.timedelta(seconds=i))

        for i in xrange(1, 5):
            self._backend.add_to_stream(obj, stream_name, "activity_before_" + str(i), published=published - datetime.timedelta(seconds=i))

        #adding stuff does not change the default marker
        eq_(self._redis_backend.get_default_marker(obj, stream_name), 0)

        self._backend.get_stream_items(obj, stream_name, marker=published, after=True, limit=3)

        #but retrieving stuff does
        eq_(self._redis_backend.get_default_marker(obj, stream_name)), self._backend._get_timestamp(published + datetime.timedelta(seconds=2))
