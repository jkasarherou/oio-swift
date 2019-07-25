# Copyright (c) 2011-2014 OpenStack Foundation.
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
from datetime import datetime
import email
import time

from swift.common import swob

from oioswift.common.middleware.s3api.s3api import S3Middleware
from tests.unit.common.middleware.s3api.helpers import FakeSwift
from oioswift.common.middleware.s3api.etree import fromstring
from oioswift.common.middleware.s3api.cfg import CONF


class FakeApp(object):
    def __init__(self):
        self.swift = FakeSwift()

    def _update_s3_path_info(self, env):
        """
        For S3 requests, Swift auth middleware replaces a user name in
        env['PATH_INFO'] with a valid tenant id.
        E.g. '/v1/test:tester/bucket/object' will become
        '/v1/AUTH_test/bucket/object'. This method emulates the behavior.
        """
        _, authorization = env['HTTP_AUTHORIZATION'].split(' ')
        tenant_user, sign = authorization.rsplit(':', 1)
        tenant, user = tenant_user.rsplit(':', 1)

        path = env['PATH_INFO']
        env['PATH_INFO'] = path.replace(tenant_user, 'AUTH_' + tenant)

    def __call__(self, env, start_response):
        if 'HTTP_AUTHORIZATION' in env:
            self._update_s3_path_info(env)

        return self.swift(env, start_response)


class S3TestCase(unittest.TestCase):
    def __init__(self, name):
        unittest.TestCase.__init__(self, name)

        CONF.log_level = 'debug'
        CONF.storage_domain = 'localhost'

    def setUp(self):
        self.app = FakeApp()
        self.swift = self.app.swift
        self.s3api = S3Middleware(self.app, CONF)

        self.swift.register('HEAD', '/v1/AUTH_test',
                            swob.HTTPOk, {}, None)
        self.swift.register('HEAD', '/v1/AUTH_test/bucket',
                            swob.HTTPNoContent, {}, None)
        self.swift.register('PUT', '/v1/AUTH_test/bucket',
                            swob.HTTPCreated, {}, None)
        self.swift.register('POST', '/v1/AUTH_test/bucket',
                            swob.HTTPNoContent, {}, None)
        self.swift.register('DELETE', '/v1/AUTH_test/bucket',
                            swob.HTTPNoContent, {}, None)

        self.swift.register('GET', '/v1/AUTH_test/bucket/object',
                            swob.HTTPOk, {'etag': '0000'}, "")
        self.swift.register('PUT', '/v1/AUTH_test/bucket/object',
                            swob.HTTPCreated, {'etag': '0000'}, None)
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/object',
                            swob.HTTPNoContent, {}, None)

    def _get_error_code(self, body):
        elem = fromstring(body, 'Error')
        return elem.find('./Code').text

    def _get_error_message(self, body):
        elem = fromstring(body, 'Error')
        return elem.find('./Message').text

    def _test_method_error(self, method, path, response_class, headers={}):
        if not path.startswith('/'):
            path = '/' + path  # add a missing slash before the path

        uri = '/v1/AUTH_test'
        if path != '/':
            uri += path

        self.swift.register(method, uri, response_class, headers, None)
        headers.update({'Authorization': 'AWS test:tester:hmac',
                        'Date': self.get_date_header()})
        req = swob.Request.blank(path, environ={'REQUEST_METHOD': method},
                                 headers=headers)
        status, headers, body = self.call_s3api(req)
        return self._get_error_code(body)

    def get_date_header(self):
        # email.utils.formatdate returns utc timestamp in default
        return email.utils.formatdate(time.time())

    def get_v4_amz_date_header(self):
        return datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')

    def call_app(self, req, app=None, expect_exception=False):
        if app is None:
            app = self.app

        req.headers.setdefault("User-Agent", "Mozzarella Foxfire")

        class StartResponseContext(object):
            status = headers = None

            def __call__(self, s, h, ei=None):
                self.status = s
                self.headers = swob.HeaderKeyDict(h)

        sr = StartResponseContext()

        body_iter = app(req.environ, sr)
        body = ''
        caught_exc = None
        try:
            for chunk in body_iter:
                body += chunk
        except Exception as exc:
            if expect_exception:
                caught_exc = exc
            else:
                raise

        if expect_exception:
            return sr.status, sr.headers, body, caught_exc
        else:
            return sr.status, sr.headers, body

    def call_s3api(self, req, **kwargs):
        return self.call_app(req, app=self.s3api, **kwargs)
