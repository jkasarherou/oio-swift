# Copyright (c) 2014,2018 OpenStack Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64
from hashlib import md5
from mock import patch
import os
import time
import unittest
import binascii
from urllib import quote

from swift.common import swob
from swift.common.swob import Request
from swift.common.utils import json

from tests.unit.common.middleware.s3api import S3TestCase
from tests.unit.common.middleware.s3api.helpers import UnreadableInput
from oioswift.common.middleware.s3api.etree import fromstring, tostring
from oioswift.common.middleware.s3api.subresource import Owner, Grant, User, \
    ACL, encode_acl, decode_acl, ACLPublicRead
from tests.unit.common.middleware.s3api.test_s3_acl import s3acl
from oioswift.common.middleware.s3api.cfg import CONF
from oioswift.common.middleware.s3api.utils import sysmeta_header, mktime, \
    S3Timestamp
from oioswift.common.middleware.s3api.request import MAX_32BIT_INT

XML = '<CompleteMultipartUpload>' \
      '<Part>' \
      '<PartNumber>1</PartNumber>' \
      '<ETag>0123456789abcdef0123456789abcdef</ETag>' \
      '</Part>' \
      '<Part>' \
      '<PartNumber>2</PartNumber>' \
      '<ETag>"fedcba9876543210fedcba9876543210"</ETag>' \
      '</Part>' \
      '</CompleteMultipartUpload>'

object_manifest = \
    [{'bytes': 11,
      'content_type': 'application/octet-stream',
      'etag': '0000',
      'last_modified': '2018-05-21T08:40:58.000000',
      'path': '/bucket+segments/object/X/1'},
     {'bytes': 21,
      'content_type': 'application/octet-stream',
      'etag': '0000',
      'last_modified': '2018-05-21T08:40:59.000000',
      'path': '/bucket+segments/object/X/2'}]

OBJECTS_TEMPLATE = \
    (('object/X/1', '2014-05-07T19:47:51.592270', '0123456789abcdef', 100),
     ('object/X/2', '2014-05-07T19:47:52.592270', 'fedcba9876543210', 200))

MULTIPARTS_TEMPLATE = \
    (('object/X', '2014-05-07T19:47:50.592270', 'HASH', 1),
     ('object/X/1', '2014-05-07T19:47:51.592270', '0123456789abcdef', 11),
     ('object/X/2', '2014-05-07T19:47:52.592270', 'fedcba9876543210', 21),
     ('object/Y', '2014-05-07T19:47:53.592270', 'HASH', 2),
     ('object/Y/1', '2014-05-07T19:47:54.592270', '0123456789abcdef', 12),
     ('object/Y/2', '2014-05-07T19:47:55.592270', 'fedcba9876543210', 22),
     ('object/Z', '2014-05-07T19:47:56.592270', 'HASH', 3),
     ('object/Z/1', '2014-05-07T19:47:57.592270', '0123456789abcdef', 13),
     ('object/Z/2', '2014-05-07T19:47:58.592270', 'fedcba9876543210', 23),
     ('subdir/object/Z', '2014-05-07T19:47:58.592270', 'HASH', 4),
     ('subdir/object/Z/1', '2014-05-07T19:47:58.592270',
      '0123456789abcdef', 41),
     ('subdir/object/Z/2', '2014-05-07T19:47:58.592270',
      'fedcba9876543210', 41))

S3_ETAG = '"%s-2' % md5(binascii.a2b_hex(
    '0123456789abcdef0123456789abcdef'
    'fedcba9876543210fedcba9876543210'
)).hexdigest()


class TestS3MultiUpload(S3TestCase):

    def setUp(self):
        super(TestS3MultiUpload, self).setUp()

        bucket = '/v1/AUTH_test/bucket'
        segment_bucket = bucket + '+segments'
        self.etag = '7dfa07a8e59ddbcd1dc84d4c4f82aea1'
        self.last_modified = 'Fri, 01 Apr 2014 12:00:00 GMT'
        put_headers = {'etag': self.etag, 'last-modified': self.last_modified}

        CONF.min_segment_size = 1

        objects = [{'name': item[0], 'last_modified': item[1],
                    'hash': item[2], 'bytes': item[3]}
                   for item in OBJECTS_TEMPLATE]
        object_list = json.dumps(objects)

        self.swift.register('PUT', segment_bucket,
                            swob.HTTPAccepted, {}, None)
        self.swift.register('GET', segment_bucket, swob.HTTPOk, {},
                            object_list)
        self.swift.register('HEAD', segment_bucket + '/object/X',
                            swob.HTTPOk, {'x-object-meta-foo': 'bar',
                                          'content-type': 'baz/quux'}, None)
        self.swift.register('PUT', segment_bucket + '/object/X',
                            swob.HTTPCreated, {}, None)
        self.swift.register('DELETE', segment_bucket + '/object/X',
                            swob.HTTPNoContent, {}, None)
        self.swift.register('GET', segment_bucket + '/object/invalid',
                            swob.HTTPNotFound, {}, None)
        self.swift.register('PUT', segment_bucket + '/object/X/1',
                            swob.HTTPCreated, put_headers, None)
        self.swift.register('DELETE', segment_bucket + '/object/X/1',
                            swob.HTTPNoContent, {}, None)
        self.swift.register('DELETE', segment_bucket + '/object/X/2',
                            swob.HTTPNoContent, {}, None)

        mp_manifest = bucket + '/object?format=raw&multipart-manifest=get'
        self.swift.register(
            'GET', mp_manifest, swob.HTTPOk,
            {'content-type': 'application/x-sharedlib;s3_etag=0002-2',
             'etag': '0001',
             'X-Static-Large-Object': 'True'},
            json.dumps(object_manifest))
        self.swift.register('HEAD', segment_bucket + '/object/X/1',
                            swob.HTTPOk,
                            {'etag': '0000',
                             'content-type': 'application/octet-stream',
                             'content-length': '11'},
                            None)

    def get_request(self, path, method, body=None):
        req = Request.blank(
            path,
            environ={'REQUEST_METHOD': method},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header()},
            body=body)
        return req

    @s3acl
    def test_bucket_upload_part(self):
        req = self.get_request('/bucket?partNumber=1&uploadId=x', 'PUT')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidRequest')

    @s3acl
    def test_object_multipart_uploads_list(self):
        req = self.get_request('/bucket/object?uploads', 'GET')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidRequest')

    @s3acl
    def test_bucket_multipart_uploads_initiate(self):
        req = self.get_request('/bucket?uploads', 'POST')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidRequest')

    @s3acl
    def test_bucket_list_parts(self):
        req = self.get_request('/bucket?uploadId=x', 'GET')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidRequest')

    @s3acl
    def test_bucket_multipart_uploads_abort(self):
        req = self.get_request('/bucket?uploadId=x', 'DELETE')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidRequest')
        self.assertEqual(self._get_error_message(body),
                         'A key must be specified')

    @s3acl
    def test_bucket_multipart_uploads_complete(self):
        req = self.get_request('/bucket?uploadId=x', 'POST')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidRequest')

    def _test_bucket_multipart_uploads_GET(self, query=None,
                                           multiparts=None):
        segment_bucket = '/v1/AUTH_test/bucket+segments'
        multiparts = multiparts or MULTIPARTS_TEMPLATE

        objects = [{'name': item[0], 'last_modified': item[1],
                    'hash': item[2], 'bytes': item[3]}
                   for item in multiparts]
        object_list = json.dumps(objects).encode('ascii')
        self.swift.register('GET', segment_bucket, swob.HTTPOk, {},
                            object_list)

        query = '?uploads&' + query if query else '?uploads'
        req = self.get_request('/bucket/' + query, 'GET')
        return self.call_s3api(req)

    @s3acl
    def test_bucket_multipart_uploads_GET(self):
        status, headers, body = self._test_bucket_multipart_uploads_GET()
        elem = fromstring(body, 'ListMultipartUploadsResult')
        self.assertEqual(elem.find('Bucket').text, 'bucket')
        self.assertIsNone(elem.find('KeyMarker').text)
        self.assertIsNone(elem.find('UploadIdMarker').text)
        self.assertEqual(elem.find('NextUploadIdMarker').text, 'Z')
        self.assertEqual(elem.find('MaxUploads').text, '1000')
        self.assertEqual(elem.find('IsTruncated').text, 'false')
        self.assertEqual(len(elem.findall('Upload')), 4)
        objects = [(o[0], o[1][:-3] + 'Z') for o in MULTIPARTS_TEMPLATE]
        for u in elem.findall('Upload'):
            name = u.find('Key').text + '/' + u.find('UploadId').text
            initiated = u.find('Initiated').text
            self.assertTrue((name, initiated) in objects)
            self.assertEqual(u.find('Initiator/ID').text, 'test:tester')
            self.assertEqual(u.find('Initiator/DisplayName').text,
                             'test:tester')
            self.assertEqual(u.find('Owner/ID').text, 'test:tester')
            self.assertEqual(u.find('Owner/DisplayName').text, 'test:tester')
            self.assertEqual(u.find('StorageClass').text, 'STANDARD')
        self.assertEqual(status.split()[0], '200')

    @s3acl
    def test_bucket_multipart_uploads_GET_without_segment_bucket(self):
        segment_bucket = '/v1/AUTH_test/bucket+segments'
        self.swift.register('GET', segment_bucket, swob.HTTPNotFound, {}, '')

        req = self.get_request('/bucket?uploads', 'GET')
        status, headers, body = self.call_s3api(req)

        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body, 'ListMultipartUploadsResult')
        self.assertEqual(elem.find('Bucket').text, 'bucket')
        self.assertIsNone(elem.find('KeyMarker').text)
        self.assertIsNone(elem.find('UploadIdMarker').text)
        self.assertIsNone(elem.find('NextUploadIdMarker').text)
        self.assertEqual(elem.find('MaxUploads').text, '1000')
        self.assertEqual(elem.find('IsTruncated').text, 'false')
        self.assertEqual(len(elem.findall('Upload')), 0)

    @s3acl
    @patch('oioswift.common.middleware.s3api.'
           'request.get_container_info', lambda x, y: {'status': 404})
    def test_bucket_multipart_uploads_GET_without_bucket(self):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket',
                            swob.HTTPNotFound, {}, '')
        req = self.get_request('/bucket?uploads', 'GET')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '404')
        self.assertEqual(self._get_error_code(body), 'NoSuchBucket')

    @s3acl
    def test_bucket_multipart_uploads_GET_encoding_type_error(self):
        query = 'encoding-type=xml'
        status, headers, body = \
            self._test_bucket_multipart_uploads_GET(query)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    @s3acl
    def test_bucket_multipart_uploads_GET_maxuploads(self):
        query = 'max-uploads=2'
        status, headers, body = \
            self._test_bucket_multipart_uploads_GET(query)
        elem = fromstring(body, 'ListMultipartUploadsResult')
        self.assertEqual(len(elem.findall('Upload/UploadId')), 2)
        self.assertEqual(elem.find('NextKeyMarker').text, 'object')
        self.assertEqual(elem.find('NextUploadIdMarker').text, 'Y')
        self.assertEqual(elem.find('MaxUploads').text, '2')
        self.assertEqual(elem.find('IsTruncated').text, 'true')
        self.assertEqual(status.split()[0], '200')

    @s3acl
    def test_bucket_multipart_uploads_GET_str_maxuploads(self):
        query = 'max-uploads=invalid'
        status, headers, body = \
            self._test_bucket_multipart_uploads_GET(query)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    @s3acl
    def test_bucket_multipart_uploads_GET_negative_maxuploads(self):
        query = 'max-uploads=-1'
        status, headers, body = \
            self._test_bucket_multipart_uploads_GET(query)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    @s3acl
    def test_bucket_multipart_uploads_GET_maxuploads_over_default(self):
        query = 'max-uploads=1001'
        status, headers, body = \
            self._test_bucket_multipart_uploads_GET(query)
        elem = fromstring(body, 'ListMultipartUploadsResult')
        self.assertEqual(len(elem.findall('Upload/UploadId')), 4)
        self.assertEqual(elem.find('NextKeyMarker').text, 'subdir/object')
        self.assertEqual(elem.find('NextUploadIdMarker').text, 'Z')
        self.assertEqual(elem.find('MaxUploads').text, '1000')
        self.assertEqual(elem.find('IsTruncated').text, 'false')
        self.assertEqual(status.split()[0], '200')

    @s3acl
    def test_bucket_multipart_uploads_GET_maxuploads_over_max_32bit_int(self):
        query = 'max-uploads=%s' % (MAX_32BIT_INT + 1)
        status, headers, body = \
            self._test_bucket_multipart_uploads_GET(query)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    @s3acl
    def test_bucket_multipart_uploads_GET_with_id_and_key_marker(self):
        query = 'upload-id-marker=Y&key-marker=object'
        multiparts = \
            (('object/Y', '2014-05-07T19:47:53.592270', 'HASH', 2),
             ('object/Y/1', '2014-05-07T19:47:53.592270', 'HASH', 12),
             ('object/Y/2', '2014-05-07T19:47:53.592270', 'HASH', 22))

        status, headers, body = \
            self._test_bucket_multipart_uploads_GET(query, multiparts)
        elem = fromstring(body, 'ListMultipartUploadsResult')
        self.assertEqual(elem.find('KeyMarker').text, 'object')
        self.assertEqual(elem.find('UploadIdMarker').text, 'Y')
        self.assertEqual(len(elem.findall('Upload')), 1)

        objects = [(obj[0], obj[1][:-3] + 'Z') for obj in multiparts]
        for u in elem.findall('Upload'):
            name = u.find('Key').text + '/' + u.find('UploadId').text
            initiated = u.find('Initiated').text
            self.assertTrue((name, initiated) in objects)
        self.assertEqual(status.split()[0], '200')

        _, path, _ = self.swift.calls_with_headers[-1]
        path, query_string = path.split('?', 1)
        query = {}
        for q in query_string.split('&'):
            key, arg = q.split('=')
            query[key] = arg
        self.assertEqual(query['format'], 'json')
        self.assertEqual(query['limit'], '1001')
        self.assertEqual(query['marker'], 'object/Y')

    @s3acl
    def test_bucket_multipart_uploads_GET_with_key_marker(self):
        query = 'key-marker=object'
        multiparts = \
            (('object/X', '2014-05-07T19:47:50.592270', 'HASH', 1),
             ('object/X/1', '2014-05-07T19:47:51.592270', 'HASH', 11),
             ('object/X/2', '2014-05-07T19:47:52.592270', 'HASH', 21),
             ('object/Y', '2014-05-07T19:47:53.592270', 'HASH', 2),
             ('object/Y/1', '2014-05-07T19:47:54.592270', 'HASH', 12),
             ('object/Y/2', '2014-05-07T19:47:55.592270', 'HASH', 22))
        status, headers, body = \
            self._test_bucket_multipart_uploads_GET(query, multiparts)
        elem = fromstring(body, 'ListMultipartUploadsResult')
        self.assertEqual(elem.find('KeyMarker').text, 'object')
        self.assertEqual(elem.find('NextKeyMarker').text, 'object')
        self.assertEqual(elem.find('NextUploadIdMarker').text, 'Y')
        self.assertEqual(len(elem.findall('Upload')), 2)
        objects = [(o[0], o[1][:-3] + 'Z') for o in multiparts]
        for u in elem.findall('Upload'):
            name = u.find('Key').text + '/' + u.find('UploadId').text
            initiated = u.find('Initiated').text
            self.assertTrue((name, initiated) in objects)
        self.assertEqual(status.split()[0], '200')

        _, path, _ = self.swift.calls_with_headers[-1]
        path, query_string = path.split('?', 1)
        query = {}
        for q in query_string.split('&'):
            key, arg = q.split('=')
            query[key] = arg
        self.assertEqual(query['format'], 'json')
        self.assertEqual(query['limit'], '1001')
        self.assertEqual(query['marker'], quote('object/~'))

    @s3acl
    def test_bucket_multipart_uploads_GET_with_prefix(self):
        query = 'prefix=X'
        multiparts = \
            (('object/X', '2014-05-07T19:47:50.592270', 'HASH', 1),
             ('object/X/1', '2014-05-07T19:47:51.592270', 'HASH', 11),
             ('object/X/2', '2014-05-07T19:47:52.592270', 'HASH', 21))
        status, headers, body = \
            self._test_bucket_multipart_uploads_GET(query, multiparts)
        elem = fromstring(body, 'ListMultipartUploadsResult')
        self.assertEqual(len(elem.findall('Upload')), 1)
        objects = [(o[0], o[1][:-3] + 'Z') for o in multiparts]
        for u in elem.findall('Upload'):
            name = u.find('Key').text + '/' + u.find('UploadId').text
            initiated = u.find('Initiated').text
            self.assertTrue((name, initiated) in objects)
        self.assertEqual(status.split()[0], '200')

        _, path, _ = self.swift.calls_with_headers[-1]
        path, query_string = path.split('?', 1)
        query = {}
        for q in query_string.split('&'):
            key, arg = q.split('=')
            query[key] = arg
        self.assertEqual(query['format'], 'json')
        self.assertEqual(query['limit'], '1001')
        self.assertEqual(query['prefix'], 'X')

    @s3acl
    def test_bucket_multipart_uploads_GET_with_delimiter(self):
        query = 'delimiter=/'
        multiparts = \
            (('object/X', '2014-05-07T19:47:50.592270', 'HASH', 1),
             ('object/X/1', '2014-05-07T19:47:51.592270', 'HASH', 11),
             ('object/X/2', '2014-05-07T19:47:52.592270', 'HASH', 21),
             ('object/Y', '2014-05-07T19:47:50.592270', 'HASH', 2),
             ('object/Y/1', '2014-05-07T19:47:51.592270', 'HASH', 21),
             ('object/Y/2', '2014-05-07T19:47:52.592270', 'HASH', 22),
             ('object/Z', '2014-05-07T19:47:50.592270', 'HASH', 3),
             ('object/Z/1', '2014-05-07T19:47:51.592270', 'HASH', 31),
             ('object/Z/2', '2014-05-07T19:47:52.592270', 'HASH', 32),
             ('subdir/object/X', '2014-05-07T19:47:50.592270', 'HASH', 4),
             ('subdir/object/X/1', '2014-05-07T19:47:51.592270', 'HASH', 41),
             ('subdir/object/X/2', '2014-05-07T19:47:52.592270', 'HASH', 42),
             ('subdir/object/Y', '2014-05-07T19:47:50.592270', 'HASH', 5),
             ('subdir/object/Y/1', '2014-05-07T19:47:51.592270', 'HASH', 51),
             ('subdir/object/Y/2', '2014-05-07T19:47:52.592270', 'HASH', 52),
             ('subdir2/object/Z', '2014-05-07T19:47:50.592270', 'HASH', 6),
             ('subdir2/object/Z/1', '2014-05-07T19:47:51.592270', 'HASH', 61),
             ('subdir2/object/Z/2', '2014-05-07T19:47:52.592270', 'HASH', 62))

        status, headers, body = \
            self._test_bucket_multipart_uploads_GET(query, multiparts)
        elem = fromstring(body, 'ListMultipartUploadsResult')
        self.assertEqual(len(elem.findall('Upload')), 3)
        self.assertEqual(len(elem.findall('CommonPrefixes')), 2)
        objects = [(o[0], o[1][:-3] + 'Z') for o in multiparts
                   if o[0].startswith('o')]
        prefixes = set([o[0].split('/')[0] + '/' for o in multiparts
                        if o[0].startswith('s')])
        for u in elem.findall('Upload'):
            name = u.find('Key').text + '/' + u.find('UploadId').text
            initiated = u.find('Initiated').text
            self.assertTrue((name, initiated) in objects)
        for p in elem.findall('CommonPrefixes'):
            prefix = p.find('Prefix').text
            self.assertTrue(prefix in prefixes)

        self.assertEqual(status.split()[0], '200')
        _, path, _ = self.swift.calls_with_headers[-1]
        path, query_string = path.split('?', 1)
        query = {}
        for q in query_string.split('&'):
            key, arg = q.split('=')
            query[key] = arg
        self.assertEqual(query['format'], 'json')
        self.assertEqual(query['limit'], '1001')
        self.assertTrue(query.get('delimiter') is None)

    @s3acl
    def test_bucket_multipart_uploads_GET_with_multi_chars_delimiter(self):
        query = 'delimiter=subdir'
        multiparts = \
            (('object/X', '2014-05-07T19:47:50.592270', 'HASH', 1),
             ('object/X/1', '2014-05-07T19:47:51.592270', 'HASH', 11),
             ('object/X/2', '2014-05-07T19:47:52.592270', 'HASH', 21),
             ('dir/subdir/object/X', '2014-05-07T19:47:50.592270',
              'HASH', 3),
             ('dir/subdir/object/X/1', '2014-05-07T19:47:51.592270',
              'HASH', 31),
             ('dir/subdir/object/X/2', '2014-05-07T19:47:52.592270',
              '0000', 32),
             ('subdir/object/X', '2014-05-07T19:47:50.592270', 'HASH', 4),
             ('subdir/object/X/1', '2014-05-07T19:47:51.592270', 'HASH', 41),
             ('subdir/object/X/2', '2014-05-07T19:47:52.592270', 'HASH', 42),
             ('subdir/object/Y', '2014-05-07T19:47:50.592270', 'HASH', 5),
             ('subdir/object/Y/1', '2014-05-07T19:47:51.592270', 'HASH', 51),
             ('subdir/object/Y/2', '2014-05-07T19:47:52.592270', 'HASH', 52),
             ('subdir2/object/Z', '2014-05-07T19:47:50.592270', 'HASH', 6),
             ('subdir2/object/Z/1', '2014-05-07T19:47:51.592270', 'HASH', 61),
             ('subdir2/object/Z/2', '2014-05-07T19:47:52.592270', 'HASH', 62))

        status, headers, body = \
            self._test_bucket_multipart_uploads_GET(query, multiparts)
        elem = fromstring(body, 'ListMultipartUploadsResult')
        self.assertEqual(len(elem.findall('Upload')), 1)
        self.assertEqual(len(elem.findall('CommonPrefixes')), 2)
        objects = [(o[0], o[1][:-3] + 'Z') for o in multiparts
                   if o[0].startswith('object')]
        prefixes = ('dir/subdir', 'subdir')
        for u in elem.findall('Upload'):
            name = u.find('Key').text + '/' + u.find('UploadId').text
            initiated = u.find('Initiated').text
            self.assertTrue((name, initiated) in objects)
        for p in elem.findall('CommonPrefixes'):
            prefix = p.find('Prefix').text
            self.assertTrue(prefix in prefixes)

        self.assertEqual(status.split()[0], '200')
        _, path, _ = self.swift.calls_with_headers[-1]
        path, query_string = path.split('?', 1)
        query = {}
        for q in query_string.split('&'):
            key, arg = q.split('=')
            query[key] = arg
        self.assertEqual(query['format'], 'json')
        self.assertEqual(query['limit'], '1001')
        self.assertTrue(query.get('delimiter') is None)

    @s3acl
    def test_bucket_multipart_uploads_GET_with_prefix_and_delimiter(self):
        query = 'prefix=dir/&delimiter=/'
        multiparts = \
            (('dir/subdir/object/X', '2014-05-07T19:47:50.592270',
              'HASH', 4),
             ('dir/subdir/object/X/1', '2014-05-07T19:47:51.592270',
              'HASH', 41),
             ('dir/subdir/object/X/2', '2014-05-07T19:47:52.592270',
              '0000', 42),
             ('dir/object/X', '2014-05-07T19:47:50.592270', 'HASH', 5),
             ('dir/object/X/1', '2014-05-07T19:47:51.592270', 'HASH', 51),
             ('dir/object/X/2', '2014-05-07T19:47:52.592270', 'HASH', 52))

        status, headers, body = \
            self._test_bucket_multipart_uploads_GET(query, multiparts)
        elem = fromstring(body, 'ListMultipartUploadsResult')
        self.assertEqual(len(elem.findall('Upload')), 1)
        self.assertEqual(len(elem.findall('CommonPrefixes')), 1)
        objects = [(o[0], o[1][:-3] + 'Z') for o in multiparts
                   if o[0].startswith('dir/o')]
        prefixes = ['dir/subdir/']
        for u in elem.findall('Upload'):
            name = u.find('Key').text + '/' + u.find('UploadId').text
            initiated = u.find('Initiated').text
            self.assertTrue((name, initiated) in objects)
        for p in elem.findall('CommonPrefixes'):
            prefix = p.find('Prefix').text
            self.assertTrue(prefix in prefixes)

        self.assertEqual(status.split()[0], '200')
        _, path, _ = self.swift.calls_with_headers[-1]
        path, query_string = path.split('?', 1)
        query = {}
        for q in query_string.split('&'):
            key, arg = q.split('=')
            query[key] = arg
        self.assertEqual(query['format'], 'json')
        self.assertEqual(query['limit'], '1001')
        self.assertEqual(query['prefix'], 'dir/')
        self.assertTrue(query.get('delimiter') is None)

    @patch('oioswift.common.middleware.s3api.'
           'controllers.multi_upload.unique_id', lambda: 'X')
    def _test_object_multipart_upload_initiate(self, headers):
        req = self.get_request('/bucket/object?uploads', 'POST')
        headers['x-amz-meta-foo'] = 'bar'
        req.headers.update(headers)
        status, headers, body = self.call_s3api(req)
        fromstring(body, 'InitiateMultipartUploadResult')
        self.assertEqual(status.split()[0], '200')

        _, _, req_headers = self.swift.calls_with_headers[-1]
        self.assertEqual(req_headers.get('X-Object-Meta-Foo'), 'bar')
        self.assertNotIn('Etag', req_headers)
        self.assertNotIn('Content-MD5', req_headers)
        self.assertEqual([
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', '/v1/AUTH_test'),
            ('HEAD', '/v1/AUTH_test/bucket+segments'),
            ('PUT', '/v1/AUTH_test/bucket+segments/object/X'),
        ], self.swift.calls)
        self.swift.clear_calls()

    def test_object_multipart_upload_initiate(self):
        self._test_object_multipart_upload_initiate({})
        self._test_object_multipart_upload_initiate({'Etag': 'blahblahblah'})
        self._test_object_multipart_upload_initiate({
            'Content-MD5': base64.b64encode('blahblahblahblah').strip()})

    @s3acl(s3acl_only=True)
    @patch('oioswift.common.middleware.s3api.'
           'controllers.multi_upload.unique_id', lambda: 'X')
    def test_object_multipart_upload_initiate_s3acl(self):
        req = self.get_request('/bucket/object?uploads', 'POST')
        req.headers['x-amz-acl'] = 'public-read'
        req.headers['x-amz-meta-foo'] = 'bar'
        status, headers, body = self.call_s3api(req)
        fromstring(body, 'InitiateMultipartUploadResult')
        self.assertEqual(status.split()[0], '200')

        _, _, req_headers = self.swift.calls_with_headers[-1]
        self.assertEqual(req_headers.get('X-Object-Meta-Foo'), 'bar')
        tmpacl_header = req_headers.get(sysmeta_header('object', 'tmpacl'))
        self.assertTrue(tmpacl_header)
        acl_header = encode_acl('object',
                                ACLPublicRead(Owner('test:tester',
                                                    'test:tester')))
        self.assertEqual(acl_header.get(sysmeta_header('object', 'acl')),
                         tmpacl_header)

    @patch('oioswift.common.middleware.s3api.'
           'controllers.multi_upload.unique_id', lambda: 'X')
    def test_object_multipart_upload_initiate_without_bucket(self):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket',
                            swob.HTTPNotFound, {}, None)
        req = self.get_request('/bucket/object?uploads', 'POST')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '404')
        self.assertEqual(self._get_error_code(body), 'NoSuchBucket')

    @s3acl
    def test_object_multipart_upload_complete_error(self):
        malformed_xml = 'malformed_XML'
        req = self.get_request('/bucket/object?uploadId=X', 'POST',
                               malformed_xml)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'MalformedXML')

        # without target bucket
        req = self.get_request('/nobucket/object?uploadId=X', 'POST', XML)
        with patch('oioswift.common.middleware.s3api.'
                   'request.get_container_info',
                   lambda x, y: {'status': 404}):
            self.swift.register('HEAD', '/v1/AUTH_test/nobucket',
                                swob.HTTPNotFound, {}, None)
            status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'NoSuchBucket')

    def test_object_multipart_upload_complete(self):
        content_md5 = base64.b64encode(md5(XML.encode('ascii')).digest())
        req = Request.blank('/bucket/object?uploadId=X',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'Content-MD5': content_md5, },
                            body=XML)
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'CompleteMultipartUploadResult')
        self.assertNotIn('Etag', headers)
        self.assertEqual(elem.find('ETag').text, S3_ETAG)
        self.assertEqual(status.split()[0], '200')

        self.assertEqual([
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', '/v1/AUTH_test/bucket+segments/object/X'),
            ('PUT', '/v1/AUTH_test/bucket/object?multipart-manifest=put'),
            ('DELETE', '/v1/AUTH_test/bucket+segments/object/X')
        ], self.swift.calls)

        _, _, headers = self.swift.calls_with_headers[-2]
        self.assertEqual(headers.get('X-Object-Meta-Foo'), 'bar')
        self.assertEqual(headers.get('Content-Type'), 'baz/quux')

        override_etag = '; s3_etag=' + S3_ETAG.strip('"')
        override_header = 'X-Object-Sysmeta-Container-Update-Override-Etag'
        self.assertEqual(headers.get(override_header), override_etag)

    def test_object_multipart_upload_invalid_md5(self):
        bad_md5 = base64.b64encode(md5(
            XML.encode('ascii') + b'junk').digest())
        req = Request.blank('/bucket/object?uploadId=X',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'Content-MD5': bad_md5, },
                            body=XML)
        status, headers, body = self.call_s3api(req)
        self.assertEqual('400 Bad Request', status)
        self.assertEqual(self._get_error_code(body), 'BadDigest')

    @patch('oioswift.common.middleware.s3api.'
           'controllers.multi_upload.time')
    def test_object_multipart_upload_complete_hearbeat(self, mock_time):
        self.swift.register(
            'HEAD', '/v1/AUTH_test/bucket+segments/heartbeat/X',
            swob.HTTPOk, {}, None)
        self.swift.register(
            'GET', '/v1/AUTH_test/bucket+segments', swob.HTTPOk, {},
            json.dumps([
                {'name': item[0].replace('object', 'heartbeat'),
                 'last-modified': item[1], 'hash': item[2], 'bytes': item[3]}
                for item in OBJECTS_TEMPLATE]))
        self.swift.register(
            'PUT', '/v1/AUTH_test/bucket/heartbeat',
            swob.HTTPAccepted, {}, [b' ', b' ', b' ', json.dumps({
                'Etag': '"slo-etag',
                'Response Status': '201 Created',
                'Errors': [],
            }).encode('ascii')])
        mock_time.side_effect = (1, 12, 13, 14, 15,)
        self.swift.register(
            'DELETE', '/v1/AUTH_test/bucket+segments/heartbeat/X',
            swob.HTTPNoContent, {}, None)

        req = self.get_request('/bucket/heartbeat?uploadId=X', 'POST', XML)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        lines = body.split(b'\n')
        self.assertTrue(lines[0].startswith(b'<?xml '))
        self.assertTrue(lines[1])
        self.assertFalse(lines[1].strip())
        elem = fromstring(body, 'CompleteMultipartUploadResult')
        self.assertEqual(elem.find('ETag').text, S3_ETAG)

    @patch('oioswift.common.middleware.s3api.'
           'controllers.multi_upload.time')
    def test_object_multipart_upload_complete_failure_hearbeat(
            self, mock_time):
        self.swift.register(
            'HEAD', '/v1/AUTH_test/bucket+segments/heartbeat/X',
            swob.HTTPOk, {}, None)
        self.swift.register(
            'GET', '/v1/AUTH_test/bucket+segments', swob.HTTPOk, {},
            json.dumps([
                {'name': item[0].replace('object', 'heartbeat'),
                 'last-modified': item[1], 'hash': item[2], 'bytes': item[3]}
                for item in OBJECTS_TEMPLATE]))
        self.swift.register(
            'PUT', '/v1/AUTH_test/bucket/heartbeat',
            swob.HTTPAccepted, {}, [b' ', b' ', b' ', json.dumps({
                'Response Status': '400 Bad Request',
                'Errors': [['foo/object', '403 Forbidden']],
            }).encode('ascii')])
        mock_time.side_effect = (1, 12, 13, 14, 15,)

        req = self.get_request('/bucket/heartbeat?uploadId=X', 'POST', XML)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        lines = body.split(b'\n')
        self.assertTrue(lines[0].startswith(b'<?xml '))
        self.assertTrue(lines[1])
        self.assertFalse(lines[1].strip())
        fromstring(body, 'Error')
        self.assertEqual(self._get_error_code(body), 'InvalidRequest')
        self.assertEqual(self._get_error_message(body),
                         'foo/object: 403 Forbidden')
        self.assertEqual(self.swift.calls, [
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', '/v1/AUTH_test/bucket+segments/heartbeat/X'),
            ('PUT', '/v1/AUTH_test/bucket/heartbeat?multipart-manifest=put'),
        ])

    @patch('oioswift.common.middleware.s3api.'
           'controllers.multi_upload.time')
    def test_object_multipart_upload_complete_missing_part_hearbeat(
            self, mock_time):
        self.swift.register(
            'HEAD', '/v1/AUTH_test/bucket+segments/heartbeat/X',
            swob.HTTPOk, {}, None)
        self.swift.register(
            'GET', '/v1/AUTH_test/bucket+segments', swob.HTTPOk, {},
            json.dumps([
                {'name': item[0].replace('object', 'heartbeat'),
                 'last-modified': item[1], 'hash': item[2], 'bytes': item[3]}
                for item in OBJECTS_TEMPLATE]))
        self.swift.register(
            'PUT', '/v1/AUTH_test/bucket/heartbeat',
            swob.HTTPAccepted, {}, [b' ', b' ', b' ', json.dumps({
                'Response Status': '400 Bad Request',
                'Errors': [['foo/object', '404 Not Found']],
            }).encode('ascii')])
        mock_time.side_effect = (1, 12, 13, 14, 15,)

        req = self.get_request('/bucket/heartbeat?uploadId=X', 'POST', XML)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        lines = body.split(b'\n')
        self.assertTrue(lines[0].startswith(b'<?xml '))
        self.assertTrue(lines[1])
        self.assertFalse(lines[1].strip())
        fromstring(body, 'Error')
        self.assertEqual(self._get_error_code(body), 'InvalidPart')
        self.assertIn('One or more of the specified parts could not be found',
                      self._get_error_message(body))
        self.assertEqual(self.swift.calls, [
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', '/v1/AUTH_test/bucket+segments/heartbeat/X'),
            ('PUT', '/v1/AUTH_test/bucket/heartbeat?multipart-manifest=put'),
        ])

    def test_object_multipart_upload_complete_404_on_marker_delete(self):
        self.swift.register(
            'DELETE', '/v1/AUTH_test/bucket+segments/object/X',
            swob.HTTPNotFound, {}, None)
        req = self.get_request('/bucket/object?uploadId=X', 'POST', XML)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        fromstring(body, 'CompleteMultipartUploadResult')

        _, _, headers = self.swift.calls_with_headers[-2]
        self.assertEqual(headers.get('X-Object-Meta-Foo'), 'bar')
        self.assertEqual(headers.get('Content-Type'), 'baz/quux')

    def test_object_multipart_upload_complete_weird_host_name(self):
        # This happens via boto signature v4
        req = self.get_request('/bucket/object?uploadId=X', 'POST', XML)
        req.environ['HTTP_HOST'] = 'localhost:8080:8080'
        status, headers, body = self.call_s3api(req)
        fromstring(body, 'CompleteMultipartUploadResult')
        self.assertEqual(status.split()[0], '200')

        _, _, headers = self.swift.calls_with_headers[-2]
        self.assertEqual(headers.get('X-Object-Meta-Foo'), 'bar')

    def _test_object_multipart_upload_complete_segment_too_small(
            self, min_segment_size):
        msg = ('some/foo: s3api requires that each segment be at least '
               '%d bytes') % min_segment_size
        self.swift.register('PUT', '/v1/AUTH_test/bucket/object',
                            swob.HTTPBadRequest, {}, msg)
        req = self.get_request('/bucket/object?uploadId=X', 'POST', XML)

        with patch('oioswift.common.middleware.s3api.'
                   'cfg.CONF.min_segment_size', min_segment_size):
            status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '400')
        self.assertEqual(self._get_error_code(body), 'EntityTooSmall')
        self.assertEqual(self._get_error_message(body), msg)
        self.assertEqual([
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', '/v1/AUTH_test/bucket+segments/object/X'),
            ('PUT', '/v1/AUTH_test/bucket/object?multipart-manifest=put'),
        ], self.swift.calls)
        self.swift.clear_calls()

    def test_object_multipart_upload_complete_segment_too_small(self):
        self._test_object_multipart_upload_complete_segment_too_small(1)
        self._test_object_multipart_upload_complete_segment_too_small(5242880)

    def test_object_multipart_upload_complete_single_zero_segments(self):
        segment_bucket = '/v1/AUTH_test/empty-bucket+segments'

        object_list = [{
            'name': 'object/X/1',
            'last_modified': self.last_modified,
            'hash': 'd41d8cd98f00b204e9800998ecf8427e',
            'bytes': '0',
        }]

        self.swift.register('GET', segment_bucket, swob.HTTPOk, {},
                            json.dumps(object_list))
        self.swift.register('HEAD', '/v1/AUTH_test/empty-bucket',
                            swob.HTTPNoContent, {}, None)
        self.swift.register('HEAD', segment_bucket + '/object/X',
                            swob.HTTPOk, {'x-object-meta-foo': 'bar',
                                          'content-type': 'baz/quux'}, None)
        self.swift.register('PUT', '/v1/AUTH_test/empty-bucket/object',
                            swob.HTTPCreated, {}, None)
        self.swift.register('DELETE', segment_bucket + '/object/X/1',
                            swob.HTTPOk, {}, None)
        self.swift.register('DELETE', segment_bucket + '/object/X',
                            swob.HTTPOk, {}, None)

        xml = '<CompleteMultipartUpload>' \
              '<Part>' \
              '<PartNumber>1</PartNumber>' \
              '<ETag>d41d8cd98f00b204e9800998ecf8427e</ETag>' \
              '</Part>' \
              '</CompleteMultipartUpload>'

        req = self.get_request('/empty-bucket/object?uploadId=X', 'POST', xml)
        status, headers, body = self.call_s3api(req)
        fromstring(body, 'CompleteMultipartUploadResult')
        self.assertEqual(status.split()[0], '200')

        self.assertEqual(self.swift.calls, [
            ('HEAD', '/v1/AUTH_test/empty-bucket'),
            ('HEAD', '/v1/AUTH_test/empty-bucket+segments/object/X'),
            ('PUT', '/v1/AUTH_test/empty-bucket/object?'
                    'multipart-manifest=put'),
            ('DELETE', '/v1/AUTH_test/empty-bucket+segments/object/X'),
        ])
        _, _, put_headers = self.swift.calls_with_headers[-2]
        self.assertEqual(put_headers.get('X-Object-Meta-Foo'), 'bar')
        self.assertEqual(put_headers.get('Content-Type'), 'baz/quux')

    def test_object_multipart_upload_complete_zero_length_final_segment(self):
        segment_bucket = '/v1/AUTH_test/bucket+segments'

        object_list = [{
            'name': 'object/X/1',
            'last_modified': self.last_modified,
            'hash': '0123456789abcdef0123456789abcdef',
            'bytes': '100',
        }, {
            'name': 'object/X/2',
            'last_modified': self.last_modified,
            'hash': 'fedcba9876543210fedcba9876543210',
            'bytes': '1',
        }, {
            'name': 'object/X/3',
            'last_modified': self.last_modified,
            'hash': 'd41d8cd98f00b204e9800998ecf8427e',
            'bytes': '0',
        }]

        self.swift.register('GET', segment_bucket, swob.HTTPOk, {},
                            json.dumps(object_list))
        self.swift.register('HEAD', '/v1/AUTH_test/bucket',
                            swob.HTTPNoContent, {}, None)
        self.swift.register('HEAD', segment_bucket + '/object/X',
                            swob.HTTPOk, {'x-object-meta-foo': 'bar',
                                          'content-type': 'baz/quux'}, None)
        self.swift.register('DELETE', segment_bucket + '/object/X/3',
                            swob.HTTPNoContent, {}, None)

        xml = '<CompleteMultipartUpload>' \
              '<Part>' \
              '<PartNumber>1</PartNumber>' \
              '<ETag>0123456789abcdef0123456789abcdef</ETag>' \
              '</Part>' \
              '<Part>' \
              '<PartNumber>2</PartNumber>' \
              '<ETag>fedcba9876543210fedcba9876543210</ETag>' \
              '</Part>' \
              '<Part>' \
              '<PartNumber>3</PartNumber>' \
              '<ETag>d41d8cd98f00b204e9800998ecf8427e</ETag>' \
              '</Part>' \
              '</CompleteMultipartUpload>'

        req = self.get_request('/bucket/object?uploadId=X', 'POST', xml)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        fromstring(body, 'CompleteMultipartUploadResult')
        self.assertNotIn('ETag', headers)

        self.assertEqual(self.swift.calls, [
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', '/v1/AUTH_test/bucket+segments/object/X'),
            ('PUT', '/v1/AUTH_test/bucket/object?multipart-manifest=put'),
            ('DELETE', '/v1/AUTH_test/bucket+segments/object/X'),
        ])

        _, _, headers = self.swift.calls_with_headers[-2]

    @s3acl(s3acl_only=True)
    def test_object_multipart_upload_complete_s3acl(self):
        acl_headers = encode_acl('object', ACLPublicRead(Owner('test:tester',
                                                               'test:tester')))
        headers = {}
        headers[sysmeta_header('object', 'tmpacl')] = \
            acl_headers.get(sysmeta_header('object', 'acl'))
        headers['X-Object-Meta-Foo'] = 'bar'
        headers['Content-Type'] = 'baz/quux'
        self.swift.register('HEAD', '/v1/AUTH_test/bucket+segments/object/X',
                            swob.HTTPOk, headers, None)
        req = self.get_request('/bucket/object?uploadId=X', 'POST', XML)
        status, headers, body = self.call_s3api(req)
        fromstring(body, 'CompleteMultipartUploadResult')
        self.assertEqual(status.split()[0], '200')

        _, _, headers = self.swift.calls_with_headers[-2]
        self.assertEqual(headers.get('X-Object-Meta-Foo'), 'bar')
        self.assertEqual(headers.get('Content-Type'), 'baz/quux')
        self.assertEqual(
            tostring(ACLPublicRead(Owner('test:tester',
                                         'test:tester')).elem()),
            tostring(decode_acl('object', headers).elem()))

    @s3acl
    def test_object_multipart_upload_abort_error(self):
        req = self.get_request('/bucket/object?uploadId=invalid', 'DELETE')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'NoSuchUpload')

        # without target bucket
        req = self.get_request('/nobucket/object?uploadId=invalid', 'DELETE')
        with patch('oioswift.common.middleware.s3api.'
                   'request.get_container_info',
                   lambda x, y: {'status': 404}):
            self.swift.register('HEAD', '/v1/AUTH_test/nobucket',
                                swob.HTTPNotFound, {}, None)
            status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'NoSuchBucket')

    @s3acl
    def test_object_multipart_upload_abort(self):
        req = self.get_request('/bucket/object?uploadId=X', 'DELETE')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '204')

    @s3acl
    @patch('oioswift.common.middleware.s3api.'
           'request.get_container_info', lambda x, y: {'status': 204})
    def test_object_upload_part_error(self):
        # without upload id
        req = self.get_request('/bucket/object?partNumber=1', 'PUT',
                               'part object')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

        # invalid part number
        req = self.get_request('/bucket/object?partNumber=invalid&uploadId=X',
                               'PUT', 'part object')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

        # part number must be > 0
        req = self.get_request('/bucket/object?partNumber=0&uploadId=X',
                               'PUT', 'part object')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

        # part number must be < 1001
        req = self.get_request('/bucket/object?partNumber=1001&uploadId=X',
                               'PUT', 'part object')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

        # without target bucket
        req = self.get_request('/nobucket/object?partNumber=1&uploadId=X',
                               'PUT', 'part object')
        with patch('oioswift.common.middleware.s3api.'
                   'request.get_container_info',
                   lambda x, y: {'status': 404}):
            self.swift.register('HEAD', '/v1/AUTH_test/nobucket',
                                swob.HTTPNotFound, {}, None)
            status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'NoSuchBucket')

    @s3acl
    def test_object_upload_part(self):
        req = self.get_request('/bucket/object?partNumber=1&uploadId=X',
                               'PUT', 'part object')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

    def _test_object_head_part(self, part_number=1):
        req = self.get_request('/bucket/object?partNumber=%d' % part_number,
                               'HEAD')
        return self.call_s3api(req)

    @s3acl
    def test_object_head_part(self):
        status, headers, body = self._test_object_head_part()
        self.assertEqual('200', status.split()[0])
        self.assertFalse(body)
        self.assertIn('ETag', headers)
        self.assertIn('X-Amz-Mp-Parts-Count', headers)
        self.assertEqual('"0002-2"', headers['ETag'])
        self.assertEqual('2', headers['X-Amz-Mp-Parts-Count'])

    @s3acl
    def test_object_head_part_error(self):
        status, headers, body = self._test_object_head_part(12)
        self.assertEqual('416', status.split()[0])

    @s3acl
    def test_object_list_parts_error(self):
        req = self.get_request('/bucket/object?uploadId=invalid', 'GET')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'NoSuchUpload')

        # without target bucket
        req = self.get_request('/nobucket/object?uploadId=X', 'GET')
        with patch('oioswift.common.middleware.s3api.'
                   'request.get_container_info',
                   lambda x, y: {'status': 404}):
            self.swift.register('HEAD', '/v1/AUTH_test/nobucket',
                                swob.HTTPNotFound, {}, None)
            status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'NoSuchBucket')

    @s3acl
    def test_object_list_parts(self):
        req = self.get_request('/bucket/object?uploadId=X', 'GET')
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListPartsResult')
        self.assertEqual(elem.find('Bucket').text, 'bucket')
        self.assertEqual(elem.find('Key').text, 'object')
        self.assertEqual(elem.find('UploadId').text, 'X')
        self.assertEqual(elem.find('Initiator/ID').text, 'test:tester')
        self.assertEqual(elem.find('Initiator/ID').text, 'test:tester')
        self.assertEqual(elem.find('Owner/ID').text, 'test:tester')
        self.assertEqual(elem.find('Owner/ID').text, 'test:tester')
        self.assertEqual(elem.find('StorageClass').text, 'STANDARD')
        self.assertEqual(elem.find('PartNumberMarker').text, '0')
        self.assertEqual(elem.find('NextPartNumberMarker').text, '2')
        self.assertEqual(elem.find('MaxParts').text, '1000')
        self.assertEqual(elem.find('IsTruncated').text, 'false')
        self.assertEqual(len(elem.findall('Part')), 2)
        for p in elem.findall('Part'):
            partnum = int(p.find('PartNumber').text)
            self.assertEqual(p.find('LastModified').text,
                             OBJECTS_TEMPLATE[partnum - 1][1][:-3] + 'Z')
            self.assertEqual(p.find('ETag').text.strip(),
                             '"%s"' % OBJECTS_TEMPLATE[partnum - 1][2])
            self.assertEqual(p.find('Size').text,
                             str(OBJECTS_TEMPLATE[partnum - 1][3]))
        self.assertEqual(status.split()[0], '200')

    def test_object_list_parts_encoding_type(self):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket+segments/object@@/X',
                            swob.HTTPOk, {}, None)
        req = self.get_request(
            '/bucket/object@@?uploadId=X&encoding-type=url', 'GET')
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListPartsResult')
        self.assertEqual(elem.find('Key').text, quote('object@@'))
        self.assertEqual(elem.find('EncodingType').text, 'url')
        self.assertEqual(status.split()[0], '200')

    def test_object_list_parts_without_encoding_type(self):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket+segments/object@@/X',
                            swob.HTTPOk, {}, None)
        req = self.get_request('/bucket/object@@?uploadId=X', 'GET')
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListPartsResult')
        self.assertEqual(elem.find('Key').text, 'object@@')
        self.assertEqual(status.split()[0], '200')

    def test_object_list_parts_encoding_type_error(self):
        req = self.get_request('/bucket/object?uploadId=X&encoding-type=xml',
                               'GET')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    def test_object_list_parts_max_parts(self):
        req = self.get_request('/bucket/object?uploadId=X&max-parts=1', 'GET')
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListPartsResult')
        self.assertEqual(elem.find('IsTruncated').text, 'true')
        self.assertEqual(len(elem.findall('Part')), 1)
        self.assertEqual(status.split()[0], '200')

    def test_object_list_parts_str_max_parts(self):
        req = self.get_request('/bucket/object?uploadId=X&max-parts=invalid',
                               'GET')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    def test_object_list_parts_negative_max_parts(self):
        req = self.get_request('/bucket/object?uploadId=X&max-parts=-1',
                               'GET')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    def test_object_list_parts_over_max_parts(self):
        req = self.get_request(
            '/bucket/object?uploadId=X&max-parts=%d' %
            (CONF.max_parts_listing + 1),
            'GET')
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListPartsResult')
        self.assertEqual(elem.find('Bucket').text, 'bucket')
        self.assertEqual(elem.find('Key').text, 'object')
        self.assertEqual(elem.find('UploadId').text, 'X')
        self.assertEqual(elem.find('Initiator/ID').text, 'test:tester')
        self.assertEqual(elem.find('Owner/ID').text, 'test:tester')
        self.assertEqual(elem.find('StorageClass').text, 'STANDARD')
        self.assertEqual(elem.find('PartNumberMarker').text, '0')
        self.assertEqual(elem.find('NextPartNumberMarker').text, '2')
        self.assertEqual(elem.find('MaxParts').text, '1000')
        self.assertEqual(elem.find('IsTruncated').text, 'false')
        self.assertEqual(len(elem.findall('Part')), 2)
        for p in elem.findall('Part'):
            partnum = int(p.find('PartNumber').text)
            self.assertEqual(p.find('LastModified').text,
                             OBJECTS_TEMPLATE[partnum - 1][1][:-3] + 'Z')
            self.assertEqual(p.find('ETag').text,
                             '"%s"' % OBJECTS_TEMPLATE[partnum - 1][2])
            self.assertEqual(p.find('Size').text,
                             str(OBJECTS_TEMPLATE[partnum - 1][3]))
        self.assertEqual(status.split()[0], '200')

    def test_object_list_parts_over_max_32bit_int(self):
        req = self.get_request(
            '/bucket/object?uploadId=X&max-parts=%d' % (MAX_32BIT_INT + 1),
            'GET')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    def test_object_list_parts_with_part_number_marker(self):
        req = self.get_request(
            '/bucket/object?uploadId=X&part-number-marker=1', 'GET')
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListPartsResult')
        self.assertEqual(len(elem.findall('Part')), 1)
        self.assertEqual(elem.find('Part/PartNumber').text, '2')
        self.assertEqual(elem.find('PartNumberMarker').text, '1')
        self.assertEqual(status.split()[0], '200')

    def test_object_list_parts_str_part_number_marker(self):
        req = self.get_request(
            '/bucket/object?uploadId=X&part-number-marker=invalid', 'GET')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    def test_object_list_parts_negative_part_number_marker(self):
        req = self.get_request(
            '/bucket/object?uploadId=X&part-number-marker=-1', 'GET')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    def test_object_list_parts_over_part_number_marker(self):
        part_number_marker = str(CONF.max_upload_part_num + 1)
        req = self.get_request(
            '/bucket/object?uploadId=X&part-number-marker=%s' %
            part_number_marker,
            'GET')
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListPartsResult')
        self.assertEqual(len(elem.findall('Part')), 0)
        self.assertEqual(elem.find('PartNumberMarker').text,
                         part_number_marker)
        self.assertEqual(status.split()[0], '200')

    def test_object_list_parts_over_max_32bit_int_part_number_marker(self):
        req = self.get_request(
            '/bucket/object?uploadId=X&part-number-marker=%s' %
            (MAX_32BIT_INT + 1),
            'GET')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    def test_object_list_parts_same_max_marts_as_objects_num(self):
        req = self.get_request('/bucket/object?uploadId=X&max-parts=2', 'GET')
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListPartsResult')
        self.assertEqual(len(elem.findall('Part')), 2)
        self.assertEqual(status.split()[0], '200')

    def _test_for_s3acl(self, method, query, account, hasObj=True, body=None):
        path = '/bucket%s' % ('/object' + query if hasObj else query)
        req = self.get_request(path, method, body)
        req.headers['Authorization'] = 'AWS %s:hmac' % account
        return self.call_s3api(req)

    @s3acl(s3acl_only=True)
    def test_upload_part_acl_without_permission(self):
        status, headers, body = \
            self._test_for_s3acl('PUT', '?partNumber=1&uploadId=X',
                                 'test:other')
        self.assertEqual(status.split()[0], '403')

    @s3acl(s3acl_only=True)
    def test_upload_part_acl_with_write_permission(self):
        status, headers, body = \
            self._test_for_s3acl('PUT', '?partNumber=1&uploadId=X',
                                 'test:write')
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_upload_part_acl_with_fullcontrol_permission(self):
        status, headers, body = \
            self._test_for_s3acl('PUT', '?partNumber=1&uploadId=X',
                                 'test:full_control')
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_list_multipart_uploads_acl_without_permission(self):
        status, headers, body = \
            self._test_for_s3acl('GET', '?uploads', 'test:other',
                                 hasObj=False)
        self.assertEqual(status.split()[0], '403')

    @s3acl(s3acl_only=True)
    def test_list_multipart_uploads_acl_with_read_permission(self):
        status, headers, body = \
            self._test_for_s3acl('GET', '?uploads', 'test:read',
                                 hasObj=False)
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_list_multipart_uploads_acl_with_fullcontrol_permission(self):
        status, headers, body = \
            self._test_for_s3acl('GET', '?uploads', 'test:full_control',
                                 hasObj=False)
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    @patch('oioswift.common.middleware.s3api.'
           'controllers.multi_upload.unique_id', lambda: 'X')
    def test_initiate_multipart_upload_acl_without_permission(self):
        status, headers, body = \
            self._test_for_s3acl('POST', '?uploads', 'test:other')
        self.assertEqual(status.split()[0], '403')

    @s3acl(s3acl_only=True)
    @patch('oioswift.common.middleware.s3api.'
           'controllers.multi_upload.unique_id', lambda: 'X')
    def test_initiate_multipart_upload_acl_with_write_permission(self):
        status, headers, body = \
            self._test_for_s3acl('POST', '?uploads', 'test:write')
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    @patch('oioswift.common.middleware.s3api.'
           'controllers.multi_upload.unique_id', lambda: 'X')
    def test_initiate_multipart_upload_acl_with_fullcontrol_permission(self):
        status, headers, body = \
            self._test_for_s3acl('POST', '?uploads', 'test:full_control')
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_list_parts_acl_without_permission(self):
        status, headers, body = \
            self._test_for_s3acl('GET', '?uploadId=X', 'test:other')
        self.assertEqual(status.split()[0], '403')

    @s3acl(s3acl_only=True)
    def test_list_parts_acl_with_read_permission(self):
        status, headers, body = \
            self._test_for_s3acl('GET', '?uploadId=X', 'test:read')
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_list_parts_acl_with_fullcontrol_permission(self):
        status, headers, body = \
            self._test_for_s3acl('GET', '?uploadId=X', 'test:full_control')
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_abort_multipart_upload_acl_without_permission(self):
        status, headers, body = \
            self._test_for_s3acl('DELETE', '?uploadId=X', 'test:other')
        self.assertEqual(status.split()[0], '403')

    @s3acl(s3acl_only=True)
    def test_abort_multipart_upload_acl_with_write_permission(self):
        status, headers, body = \
            self._test_for_s3acl('DELETE', '?uploadId=X', 'test:write')
        self.assertEqual(status.split()[0], '204')

    @s3acl(s3acl_only=True)
    def test_abort_multipart_upload_acl_with_fullcontrol_permission(self):
        status, headers, body = \
            self._test_for_s3acl('DELETE', '?uploadId=X', 'test:full_control')
        self.assertEqual(status.split()[0], '204')

    @s3acl(s3acl_only=True)
    def test_complete_multipart_upload_acl_without_permission(self):
        status, headers, body = \
            self._test_for_s3acl('POST', '?uploadId=X', 'test:other',
                                 body=XML)
        self.assertEqual(status.split()[0], '403')

    @s3acl(s3acl_only=True)
    def test_complete_multipart_upload_acl_with_write_permission(self):
        status, headers, body = \
            self._test_for_s3acl('POST', '?uploadId=X', 'test:write',
                                 body=XML)
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_complete_multipart_upload_acl_with_fullcontrol_permission(self):
        status, headers, body = \
            self._test_for_s3acl('POST', '?uploadId=X', 'test:full_control',
                                 body=XML)
        self.assertEqual(status.split()[0], '200')

    def _test_copy_for_s3acl(self, account, src_permission=None,
                             src_path='/src_bucket/src_obj', src_headers=None,
                             head_resp=swob.HTTPOk, put_header=None,
                             timestamp=None):
        owner = 'test:tester'
        grants = [Grant(User(account), src_permission)] \
            if src_permission else [Grant(User(owner), 'FULL_CONTROL')]
        src_o_headers = encode_acl('object', ACL(Owner(owner, owner), grants))
        src_o_headers.update({'last-modified': self.last_modified})
        src_o_headers.update(src_headers or {})
        self.swift.register('HEAD', '/v1/AUTH_test/%s' % src_path.lstrip('/'),
                            head_resp, src_o_headers, None)
        put_header = put_header or {}
        put_headers = {'Authorization': 'AWS %s:hmac' % account,
                       'Date': self.get_date_header(),
                       'X-Amz-Copy-Source': src_path}
        put_headers.update(put_header)
        req = Request.blank(
            '/bucket/object?partNumber=1&uploadId=X',
            environ={'REQUEST_METHOD': 'PUT'},
            headers=put_headers)
        timestamp = timestamp or time.time()
        with patch('oioswift.common.middleware.s3api.'
                   'utils.time.time', return_value=timestamp):
            return self.call_s3api(req)

    @s3acl
    def test_upload_part_copy(self):
        date_header = self.get_date_header()
        timestamp = mktime(date_header)
        last_modified = S3Timestamp(timestamp).s3xmlformat
        status, headers, body = self._test_copy_for_s3acl(
            'test:tester', put_header={'Date': date_header},
            timestamp=timestamp)
        self.assertEqual(status.split()[0], '200')
        self.assertEqual(headers['Content-Type'], 'application/xml')
        self.assertTrue(headers.get('etag') is None)
        elem = fromstring(body, 'CopyPartResult')
        self.assertEqual(elem.find('LastModified').text, last_modified)
        self.assertEqual(elem.find('ETag').text, '"%s"' % self.etag)

        _, _, headers = self.swift.calls_with_headers[-1]
        self.assertEqual(headers['X-Copy-From'], '/src_bucket/src_obj')
        self.assertEqual(headers['Content-Length'], '0')

    @s3acl(s3acl_only=True)
    def test_upload_part_copy_acl_with_owner_permission(self):
        status, headers, body = \
            self._test_copy_for_s3acl('test:tester')
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_upload_part_copy_acl_without_permission(self):
        status, headers, body = \
            self._test_copy_for_s3acl('test:other', 'READ')
        self.assertEqual(status.split()[0], '403')

    @s3acl(s3acl_only=True)
    def test_upload_part_copy_acl_with_write_permission(self):
        status, headers, body = \
            self._test_copy_for_s3acl('test:write', 'READ')
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_upload_part_copy_acl_with_fullcontrol_permission(self):
        status, headers, body = \
            self._test_copy_for_s3acl('test:full_control', 'READ')
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_upload_part_copy_acl_without_src_permission(self):
        status, headers, body = \
            self._test_copy_for_s3acl('test:write', 'WRITE')
        self.assertEqual(status.split()[0], '403')

    @s3acl(s3acl_only=True)
    def test_upload_part_copy_acl_invalid_source(self):
        status, headers, body = \
            self._test_copy_for_s3acl('test:write', 'WRITE', '')
        self.assertEqual(status.split()[0], '400')

        status, headers, body = \
            self._test_copy_for_s3acl('test:write', 'WRITE', '/')
        self.assertEqual(status.split()[0], '400')

        status, headers, body = \
            self._test_copy_for_s3acl('test:write', 'WRITE', '/bucket')
        self.assertEqual(status.split()[0], '400')

        status, headers, body = \
            self._test_copy_for_s3acl('test:write', 'WRITE', '/bucket/')
        self.assertEqual(status.split()[0], '400')

    @s3acl
    def test_upload_part_copy_headers_error(self):
        account = 'test:tester'
        etag = '7dfa07a8e59ddbcd1dc84d4c4f82aea1'
        last_modified_since = 'Fri, 01 Apr 2014 12:00:00 GMT'

        header = {'X-Amz-Copy-Source-If-Match': etag}
        status, header, body = \
            self._test_copy_for_s3acl(account,
                                      head_resp=swob.HTTPPreconditionFailed,
                                      put_header=header)
        self.assertEqual(self._get_error_code(body), 'PreconditionFailed')

        header = {'X-Amz-Copy-Source-If-None-Match': etag}
        status, header, body = \
            self._test_copy_for_s3acl(account,
                                      head_resp=swob.HTTPNotModified,
                                      put_header=header)
        self.assertEqual(self._get_error_code(body), 'PreconditionFailed')

        header = {'X-Amz-Copy-Source-If-Modified-Since': last_modified_since}
        status, header, body = \
            self._test_copy_for_s3acl(account,
                                      head_resp=swob.HTTPNotModified,
                                      put_header=header)
        self.assertEqual(self._get_error_code(body), 'PreconditionFailed')

        header = \
            {'X-Amz-Copy-Source-If-Unmodified-Since': last_modified_since}
        status, header, body = \
            self._test_copy_for_s3acl(account,
                                      head_resp=swob.HTTPPreconditionFailed,
                                      put_header=header)
        self.assertEqual(self._get_error_code(body), 'PreconditionFailed')

    def test_upload_part_copy_headers_with_match(self):
        account = 'test:tester'
        etag = '7dfa07a8e59ddbcd1dc84d4c4f82aea1'
        last_modified_since = 'Fri, 01 Apr 2014 11:00:00 GMT'

        header = {'X-Amz-Copy-Source-If-Match': etag,
                  'X-Amz-Copy-Source-If-Modified-Since': last_modified_since}
        status, header, body = \
            self._test_copy_for_s3acl(account, put_header=header)

        self.assertEqual(status.split()[0], '200')

        self.assertEqual(len(self.swift.calls_with_headers), 4)
        _, _, headers = self.swift.calls_with_headers[-2]
        self.assertEqual(headers['If-Match'], etag)
        self.assertEqual(headers['If-Modified-Since'], last_modified_since)
        _, _, headers = self.swift.calls_with_headers[-1]
        self.assertTrue(headers.get('If-Match') is None)
        self.assertTrue(headers.get('If-Modified-Since') is None)
        _, _, headers = self.swift.calls_with_headers[0]
        self.assertTrue(headers.get('If-Match') is None)
        self.assertTrue(headers.get('If-Modified-Since') is None)

    @s3acl(s3acl_only=True)
    def test_upload_part_copy_headers_with_match_and_s3acl(self):
        account = 'test:tester'
        etag = '7dfa07a8e59ddbcd1dc84d4c4f82aea1'
        last_modified_since = 'Fri, 01 Apr 2014 11:00:00 GMT'

        header = {'X-Amz-Copy-Source-If-Match': etag,
                  'X-Amz-Copy-Source-If-Modified-Since': last_modified_since}
        status, header, body = \
            self._test_copy_for_s3acl(account, put_header=header)

        self.assertEqual(status.split()[0], '200')
        self.assertEqual(len(self.swift.calls_with_headers), 4)
        # Before the check of the copy source in the case of s3acl is valid,
        # S3 check the bucket write permissions and the object existence
        # of the destination.
        _, _, headers = self.swift.calls_with_headers[-3]
        self.assertTrue(headers.get('If-Match') is None)
        self.assertTrue(headers.get('If-Modified-Since') is None)
        _, _, headers = self.swift.calls_with_headers[-2]
        self.assertEqual(headers['If-Match'], etag)
        self.assertEqual(headers['If-Modified-Since'], last_modified_since)
        _, _, headers = self.swift.calls_with_headers[-1]
        self.assertTrue(headers.get('If-Match') is None)
        self.assertTrue(headers.get('If-Modified-Since') is None)
        _, _, headers = self.swift.calls_with_headers[0]
        self.assertTrue(headers.get('If-Match') is None)
        self.assertTrue(headers.get('If-Modified-Since') is None)

    def test_upload_part_copy_headers_with_not_match(self):
        account = 'test:tester'
        etag = '7dfa07a8e59ddbcd1dc84d4c4f82aea1'
        last_modified_since = 'Fri, 01 Apr 2014 12:00:00 GMT'

        header = {'X-Amz-Copy-Source-If-None-Match': etag,
                  'X-Amz-Copy-Source-If-Unmodified-Since': last_modified_since}
        status, header, body = \
            self._test_copy_for_s3acl(account, put_header=header)

        self.assertEqual(status.split()[0], '200')
        self.assertEqual(len(self.swift.calls_with_headers), 4)
        _, _, headers = self.swift.calls_with_headers[-2]
        self.assertEqual(headers['If-None-Match'], etag)
        self.assertEqual(headers['If-Unmodified-Since'], last_modified_since)
        _, _, headers = self.swift.calls_with_headers[-1]
        self.assertTrue(headers.get('If-None-Match') is None)
        self.assertTrue(headers.get('If-Unmodified-Since') is None)
        _, _, headers = self.swift.calls_with_headers[0]
        self.assertTrue(headers.get('If-None-Match') is None)
        self.assertTrue(headers.get('If-Unmodified-Since') is None)

    @s3acl(s3acl_only=True)
    def test_upload_part_copy_headers_with_not_match_and_s3acl(self):
        account = 'test:tester'
        etag = '7dfa07a8e59ddbcd1dc84d4c4f82aea1'
        last_modified_since = 'Fri, 01 Apr 2014 12:00:00 GMT'

        header = {'X-Amz-Copy-Source-If-None-Match': etag,
                  'X-Amz-Copy-Source-If-Unmodified-Since': last_modified_since}
        status, header, body = \
            self._test_copy_for_s3acl(account, put_header=header)

        self.assertEqual(status.split()[0], '200')
        self.assertEqual(len(self.swift.calls_with_headers), 4)
        # Before the check of the copy source in the case of s3acl is valid,
        # S3 check the bucket write permissions and the object existence
        # of the destination.
        _, _, headers = self.swift.calls_with_headers[-3]
        self.assertTrue(headers.get('If-Match') is None)
        self.assertTrue(headers.get('If-Modified-Since') is None)
        _, _, headers = self.swift.calls_with_headers[-2]
        self.assertEqual(headers['If-None-Match'], etag)
        self.assertEqual(headers['If-Unmodified-Since'], last_modified_since)
        self.assertTrue(headers.get('If-Match') is None)
        self.assertTrue(headers.get('If-Modified-Since') is None)
        _, _, headers = self.swift.calls_with_headers[-1]
        self.assertTrue(headers.get('If-None-Match') is None)
        self.assertTrue(headers.get('If-Unmodified-Since') is None)
        _, _, headers = self.swift.calls_with_headers[0]

    def test_upload_part_copy_range_unsatisfiable(self):
        account = 'test:tester'

        header = {'X-Amz-Copy-Source-Range': 'bytes=1000-'}
        status, header, body = self._test_copy_for_s3acl(
            account, src_headers={'Content-Length': '10'}, put_header=header)

        self.assertEqual(status.split()[0], '400')
        self.assertIn('Range specified is not valid for '
                      'source object of size: 10', body)

        self.assertEqual([
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', '/v1/AUTH_test/bucket+segments/object/X'),
            ('HEAD', '/v1/AUTH_test/src_bucket/src_obj'),
        ], self.swift.calls)

    def test_upload_part_copy_range_invalid(self):
        account = 'test:tester'

        header = {'X-Amz-Copy-Source-Range': '0-9'}
        status, header, body = \
            self._test_copy_for_s3acl(account, put_header=header)

        self.assertEqual(status.split()[0], '400', body)

        header = {'X-Amz-Copy-Source-Range': 'asdf'}
        status, header, body = \
            self._test_copy_for_s3acl(account, put_header=header)

        self.assertEqual(status.split()[0], '400', body)

    def test_upload_part_copy_range(self):
        account = 'test:tester'

        header = {'X-Amz-Copy-Source-Range': 'bytes=0-9'}
        status, header, body = self._test_copy_for_s3acl(
            account, src_headers={'Content-Length': '20'}, put_header=header)

        self.assertEqual(status.split()[0], '200', body)

        self.assertEqual([
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', '/v1/AUTH_test/bucket+segments/object/X'),
            ('HEAD', '/v1/AUTH_test/src_bucket/src_obj'),
            ('PUT', '/v1/AUTH_test/bucket+segments/object/X/1'),
        ], self.swift.calls)
        put_headers = self.swift.calls_with_headers[-1][2]
        self.assertEqual('bytes=0-9', put_headers['Range'])
        self.assertEqual('/src_bucket/src_obj', put_headers['X-Copy-From'])

    def _test_no_body(self, use_content_length=False,
                      use_transfer_encoding=False, string_to_md5=''):
        content_md5 = md5(string_to_md5).digest().encode('base64').strip()
        with UnreadableInput(self) as fake_input:
            req = Request.blank(
                '/bucket/object?uploadId=X',
                environ={
                    'REQUEST_METHOD': 'POST',
                    'wsgi.input': fake_input},
                headers={
                    'Authorization': 'AWS test:tester:hmac',
                    'Date': self.get_date_header(),
                    'Content-MD5': content_md5},
                body='')
            if not use_content_length:
                req.environ.pop('CONTENT_LENGTH')
            if use_transfer_encoding:
                req.environ['HTTP_TRANSFER_ENCODING'] = 'chunked'
            status, headers, body = self.call_s3api(req)
        self.assertEqual(status, '400 Bad Request')
        self.assertEqual(self._get_error_code(body), 'InvalidRequest')
        self.assertEqual(self._get_error_message(body),
                         'You must specify at least one part')

    @s3acl
    def test_object_multi_upload_empty_body(self):
        self._test_no_body()
        self._test_no_body(string_to_md5='test')
        self._test_no_body(use_content_length=True)
        self._test_no_body(use_content_length=True, string_to_md5='test')
        self._test_no_body(use_transfer_encoding=True)
        self._test_no_body(use_transfer_encoding=True, string_to_md5='test')


class TestS3MultiUploadNonUTC(TestS3MultiUpload):
    def setUp(self):
        self.orig_tz = os.environ.get('TZ', '')
        os.environ['TZ'] = 'EST+05EDT,M4.1.0,M10.5.0'
        time.tzset()
        super(TestS3MultiUploadNonUTC, self).setUp()

    def tearDown(self):
        super(TestS3MultiUploadNonUTC, self).tearDown()
        os.environ['TZ'] = self.orig_tz
        time.tzset()


if __name__ == '__main__':
    unittest.main()
