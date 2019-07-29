# Copyright (c) 2014 OpenStack Foundation
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

import unittest

from swift.common.header_key_dict import HeaderKeyDict
from swift.common.swob import Response
from oioswift.common.middleware.s3api.response import Response as S3Response
from oioswift.common.middleware.s3api.utils import sysmeta_prefix


class TestRequest(unittest.TestCase):
    def test_from_swift_resp_slo(self):
        for expected, header_vals in \
                ((True, ('true', '1')), (False, ('false', 'ugahhh', None))):
            for val in header_vals:
                resp = Response(headers={'X-Static-Large-Object': val,
                                         'Etag': 'foo'})
                s3resp = S3Response.from_swift_resp(resp)
                self.assertEqual(expected, s3resp.is_slo)
                if s3resp.is_slo:
                    self.assertEqual('"foo-N"', s3resp.headers['ETag'])
                else:
                    self.assertEqual('"foo"', s3resp.headers['ETag'])

    def test_response_s3api_sysmeta_headers(self):
        for _type in ('object', 'container'):
            sw_headers = HeaderKeyDict(
                {sysmeta_prefix(_type) + 'foo': 'bar'})
            resp = Response(headers=sw_headers)
            s3resp = S3Response.from_swift_resp(resp)
            self.assertEqual(sw_headers, s3resp.sysmeta_headers)

    def test_response_s3api_sysmeta_headers_ignore_other(self):
        for _type in ('object', 'container'):
            sw_headers = HeaderKeyDict(
                {'x-{}-sysmeta-foo-s3api'.format(_type): 'bar',
                 sysmeta_prefix(_type) + 'foo': 'bar'})
            resp = Response(headers=sw_headers)
            s3resp = S3Response.from_swift_resp(resp)
            expected_headers = HeaderKeyDict(
                {sysmeta_prefix(_type) + 'foo': 'bar'})
            self.assertEqual(expected_headers, s3resp.sysmeta_headers)
            self.assertIn('x-{}-sysmeta-foo-s3api'.format(_type),
                          s3resp.sw_headers)


if __name__ == '__main__':
    unittest.main()
