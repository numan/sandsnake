from __future__ import absolute_import

from nose.tools import ok_, eq_, raises, set_trace

from sandsnake import create_sandsnake_backend
from sandsnake.exceptions import SandsnakeValidationException

import datetime
import itertools


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
        eq_(1325419200000L, self._backend._get_timestamp(jan_first))

    def _setup_basic_index(self):
        self.obj = "indexes"
        self.index_name = "profile_index"
        self.activity_name = "activity1234"
        published = datetime.datetime.utcnow()
        self.timestamp = self._backend._get_timestamp(published)

        self._backend.add(self.obj, self.index_name, self.activity_name, published=published)

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
        single_index = 'my_index'
        many_indexes = ['second_index', 'third_index']

        eq_([single_index], self._backend._listify(single_index))
        eq_(many_indexes, self._backend._listify(many_indexes))

    def test_get_index_collection_name(self):
        obj = "user:1234"
        eq_("%(prefix)s%(obj)s:indexes" % {'prefix': self._backend._prefix, 'obj': obj}, self._backend._get_index_collection_name(obj))

    def test_get_index_name(self):
        obj = "user:1234"
        index_name = "profile_index"
        eq_("%(prefix)sobj:%(obj)s:index:%(index)s" % {'prefix': self._backend._prefix, 'obj': obj, 'index': index_name}, self._backend._get_index_name(obj, index_name))

    def test_get_index_name_collection_name_dont_clash(self):
        obj = "indexes"
        index_name = "profile_index"
        ok_(self._backend._get_index_collection_name(obj) != self._backend._get_index_name(obj, index_name))

    def test_add(self):
        obj = "indexes"
        index_name = "profile_index"
        activity_name = "activity1234"
        published = datetime.datetime.utcnow()
        timestamp = self._backend._get_timestamp(published)

        self._backend.add(obj, index_name, activity_name, published=published)

        eq_(self._redis_backend.zcard(self._backend._get_index_name(obj, index_name)), 1)
        eq_(self._redis_backend.scard(self._backend._get_index_collection_name(obj)), 1)
        eq_(self._redis_backend.zrange(self._backend._get_index_name(obj, index_name), 0, -1, withscores=True), [(activity_name, timestamp)])

    def test_add_to_multiple_indexes_at_the_same_time(self):
        obj = "indexes"
        index_names = ["profile_index", "group_index"]
        activity_name = "activity1234"
        published = datetime.datetime.utcnow()

        self._backend.add(obj, index_names, activity_name, published=published)

        eq_(self._redis_backend.zcard(self._backend._get_index_name(obj, index_names[0])), 1)
        eq_(self._redis_backend.zcard(self._backend._get_index_name(obj, index_names[1])), 1)
        eq_(self._redis_backend.scard(self._backend._get_index_collection_name(obj)), 2)

    def test_remove_values(self):
        published = datetime.datetime.now()
        obj = "indexes"
        index_name = "profile_index"

        values = []
        for i in xrange(5):
            value = ("activity_after_" + str(i), published + datetime.timedelta(seconds=i),)
            self._backend.add(obj, index_name, value[0], published=value[1])
            values.append(value)

        for i in xrange(1, 3):
            value = ("activity_before_" + str(i), published - datetime.timedelta(seconds=i),)
            self._backend.add(obj, index_name, value[0], published=value[1])
            values.insert(0, value)

        values = map(lambda val: (val[0], self._backend._get_timestamp(val[1])), values)

        eq_(self._redis_backend.zcard(self._backend._get_index_name(obj, index_name)), 7)
        eq_(self._redis_backend.scard(self._backend._get_index_collection_name(obj)), 1)
        eq_(self._redis_backend.zrange(self._backend._get_index_name(obj, index_name), 0, -1, withscores=True), values)

        #now delete from the index:
        self._backend.remove_values(obj, index_name, [values[0][0], values[1][0]])

        eq_(self._redis_backend.zcard(self._backend._get_index_name(obj, index_name)), 5)

    def test_remove_values_that_dont_exist(self):
        published = datetime.datetime.now()
        obj = "indexes"
        index_name = "profile_index"

        values = []
        for i in xrange(5):
            value = ("activity_after_" + str(i), published + datetime.timedelta(seconds=i),)
            self._backend.add(obj, index_name, value[0], published=value[1])
            values.append(value)

        for i in xrange(1, 3):
            value = ("activity_before_" + str(i), published - datetime.timedelta(seconds=i),)
            self._backend.add(obj, index_name, value[0], published=value[1])
            values.insert(0, value)

        values = map(lambda val: (val[0], self._backend._get_timestamp(val[1])), values)

        eq_(self._redis_backend.zcard(self._backend._get_index_name(obj, index_name)), 7)
        eq_(self._redis_backend.scard(self._backend._get_index_collection_name(obj)), 1)
        eq_(self._redis_backend.zrange(self._backend._get_index_name(obj, index_name), 0, -1, withscores=True), values)

        #now delete from the index:
        self._backend.remove_values(obj, index_name, ['foo', 'bar'])

        eq_(self._redis_backend.zcard(self._backend._get_index_name(obj, index_name)), 7)

        #now delete from the index:
        self._backend.remove_values(obj, index_name, ['foo', 'bar', values[0][0]])

        eq_(self._redis_backend.zcard(self._backend._get_index_name(obj, index_name)), 6)

    def test_remove(self):
        self._setup_basic_index()

        eq_(self._redis_backend.zcard(self._backend._get_index_name(self.obj, self.index_name)), 1)
        eq_(self._redis_backend.scard(self._backend._get_index_collection_name(self.obj)), 1)
        eq_(self._redis_backend.zrange(self._backend._get_index_name(self.obj, self.index_name), 0, -1, withscores=True), [(self.activity_name, self.timestamp)])

        #now delete from the index:
        self._backend.remove(self.obj, self.index_name, self.activity_name)

        eq_(self._redis_backend.zcard(self._backend._get_index_name(self.obj, self.index_name)), 0)
        eq_(self._redis_backend.scard(self._backend._get_index_collection_name(self.obj)), 1, "we don't get rid of the index unless the user explicitly asks to delete it.")

    def test_delete_index(self):
        self._setup_basic_index()

        eq_(self._redis_backend.zcard(self._backend._get_index_name(self.obj, self.index_name)), 1)
        eq_(self._redis_backend.scard(self._backend._get_index_collection_name(self.obj)), 1)

        self._backend.delete_index(self.obj, self.index_name)

        ok_(not self._redis_backend.exists(self._backend._get_index_name(self.obj, self.index_name)))
        ok_(not self._redis_backend.exists(self._backend._get_index_collection_name(self.obj)))

    def test_delete_index_multiple_indexes_at_the_same_time(self):
        obj = "indexes"
        index_names = ["profile_index", "group_index"]
        activity_name = "activity1234"
        published = datetime.datetime.utcnow()

        self._backend.add(obj, index_names, activity_name, published=published)

        eq_(self._redis_backend.zcard(self._backend._get_index_name(obj, index_names[0])), 1)
        eq_(self._redis_backend.zcard(self._backend._get_index_name(obj, index_names[1])), 1)
        eq_(self._redis_backend.scard(self._backend._get_index_collection_name(obj)), 2)

        self._backend.delete_index(obj, index_names)

        ok_(not self._redis_backend.exists(self._backend._get_index_name(obj, index_names[0])))
        ok_(not self._redis_backend.exists(self._backend._get_index_name(obj, index_names[1])))
        ok_(not self._redis_backend.exists(self._backend._get_index_collection_name(obj)))

    def test_delete_index_multiple_indexes_some_not_deleted(self):
        obj = "indexes"
        index_names = ["profile_index", "group_index"]
        activity_name = "activity1234"
        published = datetime.datetime.utcnow()

        self._backend.add(obj, index_names, activity_name, published=published)

        eq_(self._redis_backend.zcard(self._backend._get_index_name(obj, index_names[0])), 1)
        eq_(self._redis_backend.zcard(self._backend._get_index_name(obj, index_names[1])), 1)
        eq_(self._redis_backend.scard(self._backend._get_index_collection_name(obj)), 2)

        self._backend.delete_index(obj, index_names[0])

        ok_(not self._redis_backend.exists(self._backend._get_index_name(obj, index_names[0])))
        ok_(self._redis_backend.exists(self._backend._get_index_name(obj, index_names[1])))
        ok_(self._redis_backend.exists(self._backend._get_index_collection_name(obj)))
        eq_(self._redis_backend.scard(self._backend._get_index_collection_name(obj)), 1)

    def test_delete_index_index_object_doesnt_exist(self):
        #fail silently if the indexes/objects don't exist
        self._backend.delete_index("non existing", "also does not exist")

    def test_delete_index_index_doesnt_exist(self):

        self._setup_basic_index()

        eq_(self._redis_backend.zcard(self._backend._get_index_name(self.obj, self.index_name)), 1)
        eq_(self._redis_backend.scard(self._backend._get_index_collection_name(self.obj)), 1)

        self._backend.delete_index(self.obj, "this doesn't really exist")

        eq_(self._redis_backend.zcard(self._backend._get_index_name(self.obj, self.index_name)), 1)
        eq_(self._redis_backend.scard(self._backend._get_index_collection_name(self.obj)), 1)

    def test_get(self):
        published = datetime.datetime.now()
        obj = "indexes"
        index_name = "profile_index"

        for i in xrange(5):
            self._backend.add(obj, index_name, "activity_after_" + str(i), published=published + datetime.timedelta(seconds=i))

        for i in xrange(1, 3):
            self._backend.add(obj, index_name, "activity_before_" + str(i), published=published - datetime.timedelta(seconds=i))

        #get all values after the marker
        result = self._backend.get(obj, index_name, marker=published)

        eq_(len(result), 3)
        eq_(['activity_after_0'] + ["activity_before_" + str(i) for i in xrange(1, 3)], result)

        #get all values before the marker
        result = self._backend.get(obj, index_name, marker=published, after=True)

        eq_(len(result), 5)
        eq_(["activity_after_" + str(i) for i in xrange(5)], result)

    def test_get_limit(self):
        published = datetime.datetime.now()
        obj = "indexes"
        index_name = "profile_index"

        for i in xrange(5):
            self._backend.add(obj, index_name, "activity_after_" + str(i), published=published + datetime.timedelta(seconds=i))

        for i in xrange(1, 3):
            self._backend.add(obj, index_name, "activity_before_" + str(i), published=published - datetime.timedelta(seconds=i))

        #get all values after the marker
        result = self._backend.get(obj, index_name, marker=published, limit=2)

        eq_(len(result), 2)
        eq_(['activity_after_0', 'activity_before_1'], result)

        #get all values before the marker
        result = self._backend.get(obj, index_name, marker=published, after=True, limit=2)

        eq_(len(result), 2)
        eq_(["activity_after_" + str(i) for i in xrange(2)], result)

    @raises(SandsnakeValidationException)
    def test_get_marker_required(self):
        obj = "indexes"
        index_name = "profile_index"

        self._backend.get(obj, index_name)

    def test_post_get(self):
        eq_(self._backend._post_get([[('act:1', 1,), ('act:2', 2, ), ('act:3', 3)]],\
            "obj1", "index1", 123, 20, False, False), [['act:1', 'act:2', 'act:3']])

    def test_post_get_(self):
        eq_(self._backend._post_get([[('act:1', 1,), ('act:2', 2, ), ('act:3', 3)]],\
            "obj1", "index1", 123, 20, False, True), [[('act:1', 1,), ('act:2', 2, ), ('act:3', 3)]])


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

    def test_get_index_marker_name_default_name(self):
        index = "userindex"

        eq_("index:%(index)s:name:%(name)s" % {'index': index, 'name': '_ssdefault'}, self._backend._get_index_marker_name(index))

    def test_get_index_marker_name_custom_name(self):
        index = "userindex"
        marker_name = "custom"

        eq_("index:%(index)s:name:%(name)s" % {'index': index, 'name': marker_name}, self._backend._get_index_marker_name(index, marker_name=marker_name))

    def test_delete_index_remove_marker(self):
        obj = "indexes"
        index_name = "profile_index"
        published = datetime.datetime.utcnow()
        timestamp = self._backend._get_timestamp(published)
        initial_marker_hash = {'index:ssnake:obj:indexes:index:profile_index:name:_ssdefault': str(timestamp)}

        self._redis_backend.hmset(self._backend._get_obj_markers_name(obj), initial_marker_hash)
        markers_hash = self._redis_backend.hgetall(self._backend._get_obj_markers_name(obj))
        eq_(initial_marker_hash, markers_hash)

        self._backend.delete_index(obj, index_name)
        markers_hash = self._redis_backend.hgetall(self._backend._get_obj_markers_name(obj))
        eq_({}, markers_hash)

    def test_delete_multiple_indexes_at_the_same_time_markers_updated(self):
        obj = "indexes"
        index_names = ["profile_index", "group_index"]
        published = datetime.datetime.utcnow()
        timestamp = self._backend._get_timestamp(published)
        initial_marker_hash = {
            'index:ssnake:obj:indexes:index:profile_index:name:_ssdefault': str(timestamp), \
            'index:ssnake:obj:indexes:index:group_index:name:_ssdefault': str(timestamp)
        }

        self._redis_backend.hmset(self._backend._get_obj_markers_name(obj), initial_marker_hash)
        markers_hash = self._redis_backend.hgetall(self._backend._get_obj_markers_name(obj))
        eq_(initial_marker_hash, markers_hash)

        self._backend.delete_index(obj, index_names)
        markers_hash = self._redis_backend.hgetall(self._backend._get_obj_markers_name(obj))
        eq_({}, markers_hash)

    def test_get_count(self):
        published = datetime.datetime.now()
        obj = "indexes"
        index_name = "profile_index"

        for i in xrange(5):
            self._backend.add(obj, index_name, "activity_after_" + str(i), published=published + datetime.timedelta(seconds=i))

        for i in xrange(1, 3):
            self._backend.add(obj, index_name, "activity_before_" + str(i), published=published - datetime.timedelta(seconds=i))

        eq_(3, self._backend.get_count(obj, index_name, published))
        eq_(5, self._backend.get_count(obj, index_name, published, after=True))

    def test_get_before_marker_doesnt_updates_marker(self):
        published = datetime.datetime.now()
        obj = "indexes"
        index_name = "profile_index"

        for i in xrange(5):
            self._backend.add(obj, index_name, "activity_after_" + str(i), published=published + datetime.timedelta(seconds=i))

        for i in xrange(1, 3):
            self._backend.add(obj, index_name, "activity_before_" + str(i), published=published - datetime.timedelta(seconds=i))

        eq_(self._redis_backend.hget(self._backend._get_obj_markers_name(obj),\
            self._backend._get_index_marker_name(index_name)), None)

        #get all values after the marker
        result = self._backend.get(obj, index_name, marker=published)

        eq_(len(result), 3)
        eq_(self._redis_backend.hget(self._backend._get_obj_markers_name(obj),\
            self._backend._get_index_marker_name(index_name)), None)

    def test_get_default_marker(self):
        published = datetime.datetime.now()
        obj = "indexes"
        index_name = "profile_index"

        for i in xrange(5):
            self._backend.add(obj, index_name, "activity_after_" + str(i), published=published + datetime.timedelta(seconds=i))

        for i in xrange(1, 5):
            self._backend.add(obj, index_name, "activity_before_" + str(i), published=published - datetime.timedelta(seconds=i))

        #adding stuff does not change the default marker
        eq_(self._backend.get_default_marker(obj, index_name), None)

        self._backend.get(obj, index_name, marker=published, after=True, limit=3)

        #retrieving stuff doesn't either, buy default
        eq_(self._backend.get_default_marker(obj, index_name), None)

    def test_get_markers_single_marker(self):
        obj = "indexes"
        index_name = "profile_index"
        marker_name = "test marker"

        self._redis_backend.hmset(self._backend._get_obj_markers_name(obj), {self._backend._get_index_marker_name(index_name, marker_name=marker_name): 25L})

        marker_value = self._backend.get_markers(obj, index_name, marker_name)
        eq_(marker_value, 25L)

    def test_get_markers_multiple_marker(self):
        obj = "indexes"
        index_name = "profile_index"
        marker_names_dict = {"test marker": 25L, "test marker 2": 40L, "test marker 3": 50L}

        parsed_markers_dict = {}
        for key, value in marker_names_dict.items():
            parsed_markers_dict[self._backend._get_index_marker_name(index_name, marker_name=key)] = value
        self._redis_backend.hmset(self._backend._get_obj_markers_name(obj), parsed_markers_dict)

        markers = marker_names_dict.keys()
        marker_values = self._backend.get_markers(obj, index_name, markers)
        for marker, marker_value in zip(markers, marker_values):
            eq_(marker_value, marker_names_dict[marker])

    def test_get_markers_multiple_marker_non_existing(self):
        obj = "indexes"
        index_name = "profile_index"

        marker_values = self._backend.get_markers(obj, index_name, ["foo", "bar"])

        eq_(marker_values, [None, None])

    def test_set_markers_multiple_marker(self):
        obj = "indexes"
        index_name = "profile_index"
        marker_names_dict = {"test marker": 25L, "test marker 2": 40L, "test marker 3": 50L}

        self._backend.set_markers(obj, index_name, marker_names_dict)

        markers = marker_names_dict.keys()
        for marker in markers:
            eq_(long(self._redis_backend.hget(self._backend._get_obj_markers_name(obj),\
                            self._backend._get_index_marker_name(index_name, marker_name=marker))), marker_names_dict[marker])


class TestRedisWithBubblingBackend(object):
    def setUp(self):
        self._backend = create_sandsnake_backend({
            "backend": "sandsnake.backends.redis.RedisWithBubbling",
            "settings": {
                "hosts": [{"db": 3}, {"db": 4}, {"db": 5}]
            },
        })

        self._redis_backend = self._backend.get_backend()

        #clear the redis database so we are in a consistent state
        self._redis_backend.flushdb()

    def tearDown(self):
        self._redis_backend.flushdb()

    def test_clear_all(self):
        published = datetime.datetime.utcnow()
        obj = "indexes"
        index_name = "profile_index"

        for i in xrange(2):
            self._backend.add(obj, index_name, "activity_after_" + str(i), published=published + datetime.timedelta(seconds=i))

        for i in xrange(1, 2):
            self._backend.add(obj, index_name, "activity_before_" + str(i), published=published - datetime.timedelta(seconds=i))

        redis_client = self._backend.get_backend()

        ok_(not len(list(itertools.chain(*redis_client.keys()))) == 0)

        self._backend.clear_all()

        ok_(len(list(itertools.chain(*redis_client.keys()))) == 0)

    def test_bubble_values(self):
        published = datetime.datetime.utcnow()
        obj = "indexes"
        index_name = "profile_index"

        for i in xrange(5):
            self._backend.add(obj, index_name, "activity_after_" + str(i), published=published + datetime.timedelta(seconds=i))

        for i in xrange(1, 5):
            self._backend.add(obj, index_name, "activity_before_" + str(i), published=published - datetime.timedelta(seconds=i))

        eq_(self._redis_backend.hget(self._backend._get_obj_markers_name(obj),\
            self._backend._get_index_marker_name(index_name)), None)

        #get all values before the marker
        result = self._backend.get(obj, index_name, marker=published, after=True, limit=3)

        eq_(len(result), 3)
        eq_(['activity_after_0', 'activity_after_1', 'activity_after_2'], result)

        self._backend.bubble_values(obj, index_name, {'activity_after_2': published + datetime.timedelta(seconds=20)})

        #get all values before the marker
        result = self._backend.get(obj, index_name, marker=published, after=True, limit=3)

        eq_(len(result), 3)
        eq_(['activity_after_0', 'activity_after_1', 'activity_after_3'], result)

        self._backend.bubble_values(obj, index_name, {'activity_after_3': published + datetime.timedelta(seconds=21), 'activity_after_0': published + datetime.timedelta(seconds=2)})

        #get all values before the marker
        result = self._backend.get(obj, index_name, marker=published, after=True, limit=3)

        eq_(len(result), 3)
        eq_(['activity_after_1', 'activity_after_0', 'activity_after_4'], result)

