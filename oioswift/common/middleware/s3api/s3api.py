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

from swift.common.utils import get_logger, \
    register_swift_info, config_true_value, config_positive_int_value
from swift.common.wsgi import PipelineWrapper, loadcontext

from oioswift.common.middleware.s3api.exception import NotS3Request
from oioswift.common.middleware.s3api.request import get_request_class
from oioswift.common.middleware.s3api.response import ErrorResponse, \
    InternalError, MethodNotAllowed, ResponseBase
from oioswift.common.middleware.s3api.bucket_db import get_bucket_db
from oioswift.common.middleware.s3api.cfg import Config
from oioswift.common.middleware.s3api.utils import LOGGER
from oioswift.common.middleware.s3api.acl_handlers import get_acl_handler


class S3Middleware(object):
    """S3 compatibility middleware"""
    def __init__(self, app, conf, *args, **kwargs):
        self.app = app
        self.conf = Config()
        self.conf.allow_no_owner = config_true_value(
            conf.get('allow_no_owner', False))
        self.conf.location = conf.get('location', 'US')
        self.conf.dns_compliant_bucket_names = config_true_value(
            conf.get('dns_compliant_bucket_names', True))
        self.conf.max_bucket_listing = config_positive_int_value(
            conf.get('max_bucket_listing', 1000))
        self.conf.max_parts_listing = config_positive_int_value(
            conf.get('max_parts_listing', 1000))
        self.conf.max_multi_delete_objects = config_positive_int_value(
            conf.get('max_multi_delete_objects', 1000))
        self.conf.multi_delete_concurrency = config_positive_int_value(
            conf.get('multi_delete_concurrency', 2))
        self.conf.s3_acl = config_true_value(
            conf.get('s3_acl', False))
        self.conf.s3_acl_inherit = config_true_value(
            conf.get('s3_acl_inherit', False))
        self.conf.s3_acl_openbar = config_true_value(
            conf.get('s3_acl_openbar', False))
        self.conf.storage_domain = conf.get('storage_domain', '')
        self.conf.auth_pipeline_check = config_true_value(
            conf.get('auth_pipeline_check', True))
        self.conf.max_upload_part_num = config_positive_int_value(
            conf.get('max_upload_part_num', 1000))
        self.conf.check_bucket_owner = config_true_value(
            conf.get('check_bucket_owner', False))
        self.conf.force_swift_request_proxy_log = config_true_value(
            conf.get('force_swift_request_proxy_log', False))
        self.conf.allow_multipart_uploads = config_true_value(
            conf.get('allow_multipart_uploads', True))
        self.conf.min_segment_size = config_positive_int_value(
            self.conf.get('min_segment_size', 5242880))
        self.conf.allow_anonymous_path_request = config_true_value(
            self.conf.get('allow_anonymous_path_request', True)
        )
        self.slo_enabled = self.conf.allow_multipart_uploads
        self.check_pipeline(self.conf)
        self.bucket_db = get_bucket_db(self.conf)

    def __call__(self, env, start_response):
        try:
            if self.bucket_db:
                env['swift3.bucket_db'] = self.bucket_db
            req_class = get_request_class(env, self.conf.s3_acl)
            req = req_class(
                env, self.app, self.slo_enabled, self.conf.storage_domain,
                self.conf.location, self.conf.force_swift_request_proxy_log,
                self.conf.dns_compliant_bucket_names,
                self.conf.allow_multipart_uploads,
                self.conf.allow_anonymous_path_request)
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

        controller = req.controller(self.app, self.conf)
        acl_handler = get_acl_handler(req.controller_name)(req, self.conf)
        req.set_acl_handler(acl_handler)

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
    conf = global_conf.copy()
    conf.update(local_conf)

    # Reassign config to logger
    global LOGGER
    LOGGER = get_logger(conf, log_route=conf.get('log_name', 's3api'))

    register_swift_info(
        's3api',
        max_bucket_listing=conf.get('max_bucket_listing', 1000),
        max_parts_listing=conf.get('max_parts_listing', 1000),
        max_upload_part_num=conf.get('max_upload_part_num', 1000),
        max_multi_delete_objects=conf.get('max_multi_delete_objects', 1000),
        allow_multipart_uploads=conf.get('allow_multipart_uploads', True),
        min_segment_size=conf.get('min_segment_size', 5242880),
        s3_acl=conf.get('s3_acl', False),
    )

    def s3api_filter(app):
        return S3Middleware(app, conf)

    return s3api_filter
