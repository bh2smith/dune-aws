import unittest

from dune_aws.aws import BucketFileObject


class TestBucketFileObject(unittest.TestCase):
    def test_bucket_file_properties(self):
        bf = BucketFileObject(path="table", prefix="cow", index=2)

        self.assertEqual(bf.content_filename, "cow_2.json")

        self.assertEqual(bf.object_key, "table/cow_2.json")

    def test_bucket_file_constructor(self):
        indexed_key = "table/cow_2.json"

        self.assertEqual(BucketFileObject.from_key(indexed_key).object_key, indexed_key)

        other_key = "table/moo.json"
        self.assertEqual(BucketFileObject.from_key(other_key).object_key, other_key)

    def test_bucket_file_constructor_error(self):
        with self.assertRaises(ValueError):
            # No table
            # not enough values to unpack (expected 2, got 1)
            BucketFileObject.from_key("file.json")


if __name__ == "__main__":
    unittest.main()
