# Copyright (C) 2017 OpenIO SAS
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from six.moves.urllib.parse import parse_qs, quote_plus
from swift.common.swob import HTTPBadRequest
from swift.common.utils import config_true_value, split_path
from oio.common.autocontainer import ContainerBuilder


class AutoContainerBase(object):

    BYPASS_QS = "bypass-autocontainer"
    BYPASS_HEADER = "X-bypass-autocontainer"

    def __init__(self, app, acct,
                 strip_v1=False, account_first=False, swift3_compat=False):
        self.app = app
        self.account = acct
        self.bypass_header_key = ("HTTP_" +
                                  self.BYPASS_HEADER.upper().replace('-', '_'))
        self.con_builder = ContainerBuilder()
        self.account_first = account_first
        self.swift3_compat = swift3_compat
        self.strip_v1 = strip_v1

    def should_bypass(self, env):
        """Should we bypass this filter?"""
        header = env.get(self.bypass_header_key, "").lower()
        query = parse_qs(env.get('QUERY_STRING', "")).get(self.BYPASS_QS, [""])
        return config_true_value(header) or config_true_value(query[0])

    def _convert_path(self, path):
        account = self.account
        # Remove leading '/' to be consistent with split_path()
        obj = path[1:]
        container = None

        if self.strip_v1:
            version, tail = split_path('/' + obj, 1, 2, True)
            if version in ('v1', 'v1.0'):
                obj = tail

        if self.account_first:
            account, tail = split_path('/' + obj, 1, 2, True)
            obj = tail

        if obj is not None and self.swift3_compat:
            container, tail = split_path('/' + obj, 1, 2, True)
            obj = tail

        if obj is None:
            return account, container, None

        container = quote_plus(self.con_builder(obj))
        return account, container, obj

    def _convert_special_headers(self, account, env):
        if 'HTTP_X_COPY_FROM' in env:
            # HTTP_X_COPY_FROM_ACCOUNT will just pass through
            if self.account_first:
                src_path = "/fake_account" + env['HTTP_X_COPY_FROM']
            else:
                src_path = env['HTTP_X_COPY_FROM']
            _, cont, obj = self._convert_path(src_path)
            if not cont or not obj:
                raise HTTPBadRequest(body="Malformed copy-source header")
            env['HTTP_X_COPY_FROM'] = "/%s/%s" % (cont, obj)

    def __call__(self, env, start_response):
        if self.should_bypass(env):
            return self.app(env, start_response)

        account, container, obj = self._convert_path(env.get('PATH_INFO'))

        if obj is None:
            # This is probably an account request
            return self.app(env, start_response)

        env['PATH_INFO'] = "/v1/%s/%s/%s" % (account, container, obj)

        self._convert_special_headers(account, env)
        return self.app(env, start_response)
