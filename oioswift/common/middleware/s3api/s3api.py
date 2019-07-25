# Copyright (c) 2010-2014 OpenStack Foundation.
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

"""
The s3api middleware will emulate the S3 REST api on top of swift.

The following operations are currently supported:

    * GET Service
    * DELETE Bucket
    * GET Bucket (List Objects)
    * PUT Bucket
    * DELETE Object
    * Delete Multiple Objects
    * GET Object
    * HEAD Object
    * PUT Object
    * PUT Object (Copy)

To add this middleware to your configuration, add the s3api middleware
in front of the auth middleware, and before any other middleware that
look at swift requests (like rate limiting).

To set up your client, the access key will be the concatenation of the
account and user strings that should look like test:tester, and the
secret access key is the account password.  The host should also point
to the swift storage hostname.  It also will have to use the old style
calling format, and not the hostname based container format.

An example client using the python boto library might look like the
following for an SAIO setup::

    from boto.s3.connection import S3Connection
    connection = S3Connection(
        aws_access_key_id='test:tester',
        aws_secret_access_key='testing',
        port=8080,
        host='127.0.0.1',
        is_secure=False,
        calling_format=boto.s3.connection.OrdinaryCallingFormat())
"""

from paste.deploy import loadwsgi

from swift.common.http import is_success
from swift.common.utils import closing_if_possible, get_logger, \
    register_swift_info, split_path
from swift.common.wsgi import PipelineWrapper, loadcontext, WSGIContext
from swift.proxy.controllers.base import get_container_info

from oioswift.common.middleware.s3api.exception import NotS3Request
from oioswift.common.middleware.s3api.request import get_request_class
from oioswift.common.middleware.s3api.response import ErrorResponse, \
    InternalError, MethodNotAllowed, ResponseBase
from oioswift.common.middleware.s3api.bucket_db import get_bucket_db
from oioswift.common.middleware.s3api.cfg import CONF
from oioswift.common.middleware.s3api.utils import LOGGER


class MissingDeleteOk(object):
    """Middlware to convert 404s on object deletions to 204s."""
    def __init__(self, app):
        self.app = app

    def __call__(self, env, start_response):
        ctx = WSGIContext(self.app)
        app_iter = ctx._app_call(env)
        try:
            split_path(env['PATH_INFO'], 4, 4, True)
        except ValueError:
            pass  # not an object request; don't care
        else:
            if env['REQUEST_METHOD'] == 'DELETE' and \
                    ctx._response_status[:3] == '404':
                # Should be a cache hit
                if is_success(get_container_info(
                        env, self.app, swift_source='S3').get('status')):
                    # Convert to a successful response
                    ctx._response_status = '204 No Content'
                    ctx._response_headers = [
                        (h, '0' if h.lower() == 'content-length' else v)
                        for h, v in ctx._response_headers]
                    with closing_if_possible(app_iter):
                        for chunk in app_iter:
                            pass  # should be short; just drop it on the floor
                    app_iter = ['']
        start_response(ctx._response_status, ctx._response_headers)
        return app_iter


class S3Middleware(object):
    """S3 compatibility middleware"""
    def __init__(self, app, conf, *args, **kwargs):
        self.app = app
        self.s3_app = MissingDeleteOk(app)
        self.slo_enabled = conf['allow_multipart_uploads']
        self.check_pipeline(conf)
        self.bucket_db = get_bucket_db(conf)

    def __call__(self, env, start_response):
        try:
            if self.bucket_db:
                env['swift3.bucket_db'] = self.bucket_db
            req_class = get_request_class(env)
            req = req_class(env, self.app, self.slo_enabled)
            resp = self.handle_request(req)
        except NotS3Request:
            resp = self.app
        except ErrorResponse as err_resp:
            if isinstance(err_resp, InternalError):
                LOGGER.exception(err_resp)
            resp = err_resp
        except Exception as e:
            LOGGER.exception(e)
            resp = InternalError(reason=e)

        if isinstance(resp, ResponseBase) and 'swift.trans_id' in env:
            resp.headers['x-amz-id-2'] = env['swift.trans_id']
            resp.headers['x-amz-request-id'] = env['swift.trans_id']

        return resp(env, start_response)

    def handle_request(self, req):
        LOGGER.debug('Calling S3 Middleware')
        LOGGER.debug(req.__dict__)

        controller = req.controller(self.s3_app)
        if hasattr(controller, req.method):
            handler = getattr(controller, req.method)
            if not getattr(handler, 'publicly_accessible', False):
                raise MethodNotAllowed(req.method,
                                       req.controller.resource_type())
            res = handler(req)
        else:
            raise MethodNotAllowed(req.method,
                                   req.controller.resource_type())

        return res

    def check_pipeline(self, conf):
        """
        Check that proxy-server.conf has an appropriate pipeline for s3api.
        """
        if conf.get('__file__', None) is None:
            return

        ctx = loadcontext(loadwsgi.APP, conf.__file__)
        pipeline = str(PipelineWrapper(ctx)).split(' ')

        # Add compatible with 3rd party middleware.
        check_filter_order(pipeline, ['s3api', 'proxy-server'])

        auth_pipeline = pipeline[pipeline.index('s3api') + 1:
                                 pipeline.index('proxy-server')]

        # Check SLO middleware
        if self.slo_enabled and 'slo' not in auth_pipeline:
            self.slo_enabled = False
            LOGGER.warning('s3api middleware requires SLO middleware '
                           'to support multi-part upload, please add it '
                           'in pipeline')

        if not conf.auth_pipeline_check:
            LOGGER.debug('Skip pipeline auth check.')
            return

        if 'tempauth' in auth_pipeline:
            LOGGER.debug('Use tempauth middleware.')
        elif 'keystoneauth' in auth_pipeline:
            check_filter_order(auth_pipeline,
                               ['s3token',
                                'keystoneauth'])
            LOGGER.debug('Use keystone middleware.')
        elif len(auth_pipeline):
            LOGGER.debug('Use third party(unknown) auth middleware.')
        else:
            raise ValueError('Invalid pipeline %r: expected auth between '
                             's3api and proxy-server ' % pipeline)


def check_filter_order(pipeline, required_filters):
    """
    Check that required filters are present in order in the pipeline.
    """
    indexes = []
    missing_filters = []
    for filter in required_filters:
        try:
            indexes.append(pipeline.index(filter))
        except ValueError as e:
            LOGGER.debug(e)
            missing_filters.append(filter)

    if missing_filters:
        raise ValueError('Invalid pipeline %r: missing filters %r' % (
            pipeline, missing_filters))

    if indexes != sorted(indexes):
        raise ValueError('Invalid pipeline %r: expected filter %s' % (
            pipeline, ' before '.join(required_filters)))


def filter_factory(global_conf, **local_conf):
    """Standard filter factory to use the middleware with paste.deploy"""
    CONF.update(global_conf)
    CONF.update(local_conf)

    # Reassign config to logger
    global LOGGER
    LOGGER = get_logger(CONF, log_route=CONF.get('log_name', 's3api'))

    register_swift_info(
        's3api',
        max_bucket_listing=CONF['max_bucket_listing'],
        max_parts_listing=CONF['max_parts_listing'],
        max_upload_part_num=CONF['max_upload_part_num'],
        max_multi_delete_objects=CONF['max_multi_delete_objects'],
        allow_multipart_uploads=CONF['allow_multipart_uploads'],
    )

    def s3api_filter(app):
        return S3Middleware(app, CONF)

    return s3api_filter
