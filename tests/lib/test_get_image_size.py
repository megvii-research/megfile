import unittest

from megfile.lib.get_image_size import *


class Test_get_image_size(unittest.TestCase):
    data = [
        {
            'path': './tests/lib/lookmanodeps.png',
            'width': 251,
            'height': 208,
            'file_size': 22228,
            'type': 'PNG'
        }
    ]

    def setUp(self):
        pass

    def test_get_image_size_from_bytesio(self):
        img = self.data[0]
        p = img['path']
        with io.open(p, 'rb') as fp:
            b = fp.read()
        fp = io.BytesIO(b)
        sz = len(b)
        output = get_image_size_from_bytesio(fp, sz)
        self.assertTrue(output)
        self.assertEqual(output, (img['width'], img['height']))

    def test_get_image_metadata_from_bytesio(self):
        img = self.data[0]
        p = img['path']
        with io.open(p, 'rb') as fp:
            b = fp.read()
        fp = io.BytesIO(b)
        sz = len(b)
        output = get_image_metadata_from_bytesio(fp, sz)
        self.assertTrue(output)
        for field in image_fields:
            self.assertEqual(
                getattr(output, field), None if field == 'path' else img[field])

    def test_get_image_metadata(self):
        img = self.data[0]
        output = get_image_metadata(img['path'])
        self.assertTrue(output)
        for field in image_fields:
            self.assertEqual(getattr(output, field), img[field])

    def test_get_image_metadata__ENOENT_OSError(self):
        with self.assertRaises(OSError):
            get_image_metadata('THIS_DOES_NOT_EXIST')

    def test_get_image_metadata__not_an_image_UnknownImageFormat(self):
        with self.assertRaises(UnknownImageFormat):
            get_image_metadata('./tests/lib/test_get_image_size.py')

    def test_get_image_size(self):
        img = self.data[0]
        output = get_image_size(img['path'])
        self.assertTrue(output)
        self.assertEqual(output, (img['width'], img['height']))

    def tearDown(self):
        pass
