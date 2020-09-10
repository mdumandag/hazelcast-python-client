from hazelcast.config import _IndexConfig, _IndexUtil, IndexType, QueryConstants, \
    UniqueKeyTransformation
from hazelcast.util import TimeUnit, calculate_version
from unittest import TestCase


class TimeUnitTest(TestCase):
    def test_nano_to_second(self):
        self.assertEqual(0.1, TimeUnit.to_seconds(0.1e9, TimeUnit.NANOSECOND))

    def test_micro_to_second(self):
        self.assertEqual(2, TimeUnit.to_seconds(2e6, TimeUnit.MICROSECOND))

    def test_milli_to_second(self):
        self.assertEqual(3, TimeUnit.to_seconds(3e3, TimeUnit.MILLISECOND))

    def test_second_to_second(self):
        self.assertEqual(5.5, TimeUnit.to_seconds(5.5, TimeUnit.SECOND))

    def test_minute_to_second(self):
        self.assertEqual(60, TimeUnit.to_seconds(1, TimeUnit.MINUTE))

    def test_hour_to_second(self):
        self.assertEqual(1800, TimeUnit.to_seconds(0.5, TimeUnit.HOUR))

    def test_numeric_string_to_second(self):
        self.assertEqual(1, TimeUnit.to_seconds("1000", TimeUnit.MILLISECOND))

    def test_unsupported_types_to_second(self):
        types = ["str", True, None, list(), set(), dict()]
        for type in types:
            with self.assertRaises((TypeError, ValueError)):
                TimeUnit.to_seconds(type, TimeUnit.SECOND)


class VersionUtilTest(TestCase):
    def test_version_string(self):
        self.assertEqual(-1, calculate_version(""))
        self.assertEqual(-1, calculate_version("a.3.7.5"))
        self.assertEqual(-1, calculate_version("3.a.5"))
        self.assertEqual(-1, calculate_version("3,7.5"))
        self.assertEqual(-1, calculate_version("3.7,5"))
        self.assertEqual(-1, calculate_version("10.99.RC1"))
        self.assertEqual(30702, calculate_version("3.7.2"))
        self.assertEqual(19930, calculate_version("1.99.30"))
        self.assertEqual(30700, calculate_version("3.7-SNAPSHOT"))
        self.assertEqual(30702, calculate_version("3.7.2-SNAPSHOT"))
        self.assertEqual(109902, calculate_version("10.99.2-SNAPSHOT"))
        self.assertEqual(109930, calculate_version("10.99.30-SNAPSHOT"))
        self.assertEqual(109900, calculate_version("10.99-RC1"))


class IndexUtilTest(TestCase):
    def test_with_no_attributes(self):
        config = _IndexConfig()

        with self.assertRaises(ValueError):
            _IndexUtil.validate_and_normalize("", config)

    def test_with_too_many_attributes(self):
        attributes = ["attr_%s" % i for i in range(512)]
        config = _IndexConfig(attributes=attributes)

        with self.assertRaises(ValueError):
            _IndexUtil.validate_and_normalize("", config)

    def test_with_composite_bitmap_indexes(self):
        config = _IndexConfig(attributes=["attr1", "attr2"], type=IndexType.BITMAP)

        with self.assertRaises(ValueError):
            _IndexUtil.validate_and_normalize("", config)

    def test_canonicalize_attribute_name(self):
        config = _IndexConfig(attributes=["this.x.y.z", "a.b.c"])
        normalized = _IndexUtil.validate_and_normalize("", config)
        self.assertEqual("x.y.z", normalized.attributes[0])
        self.assertEqual("a.b.c", normalized.attributes[1])

    def test_duplicate_attributes(self):
        invalid_attributes = [
            ["a", "b", "a"],
            ["a", "b", " a"],
            [" a", "b", "a"],
            ["this.a", "b", "a"],
            ["this.a ", "b", " a"],
            ["this.a", "b", "this.a"],
            ["this.a ", "b", " this.a"],
            [" this.a", "b", "a"],
        ]

        for attributes in invalid_attributes:
            with self.assertRaises(ValueError):
                config = _IndexConfig(attributes=attributes)
                _IndexUtil.validate_and_normalize("", config)

    def test_normalized_name(self):
        config = _IndexConfig(None, IndexType.SORTED, ["attr"])
        normalized = _IndexUtil.validate_and_normalize("map", config)
        self.assertEqual("map_sorted_attr", normalized.name)

        config = _IndexConfig("test", IndexType.BITMAP, ["attr"])
        normalized = _IndexUtil.validate_and_normalize("map", config)
        self.assertEqual("test", normalized.name)

        config = _IndexConfig(None, IndexType.HASH, ["this.attr2.x"])
        normalized = _IndexUtil.validate_and_normalize("map2", config)
        self.assertEqual("map2_hash_attr2.x", normalized.name)

    def test_with_bitmap_indexes(self):
        bio = {
            "unique_key": QueryConstants.THIS_ATTRIBUTE_NAME,
            "unique_key_transformation": UniqueKeyTransformation.RAW
        }
        config = _IndexConfig(type=IndexType.BITMAP, attributes=["attr"], bitmap_index_options=bio)
        normalized = _IndexUtil.validate_and_normalize("map", config)
        self.assertEqual(bio["unique_key"], normalized.bitmap_index_options.unique_key)
        self.assertEqual(bio["unique_key_transformation"], normalized.bitmap_index_options.unique_key_transformation)
