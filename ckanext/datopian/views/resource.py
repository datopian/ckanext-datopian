import os
import logging
import mimetypes

import flask
from ckantoolkit import config as ckan_config
from ckantoolkit import _, request, c, g
import ckantoolkit as toolkit
import ckan.logic as logic
import ckan.lib.base as base

import ckan.model as model
from botocore.exceptions import ClientError
from botocore.config import Config
import logging
import boto3

Blueprint = flask.Blueprint
NotFound = logic.NotFound
NotAuthorized = logic.NotAuthorized
get_action = logic.get_action
abort = base.abort
redirect = toolkit.redirect_to


s3_resource = Blueprint(
    u's3_resource',
    __name__,
    url_prefix=u'/dataset/<id>/resource',
    url_defaults={u'package_type': u'dataset'}
)



logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

S3_ACCESS_KEY_ID =  toolkit.config.get('ckanext.datopian.aws_access_key_id', '')
S3_SECRET_KEY_ID = toolkit.config.get('ckanext.datopian.aws_secret_key_id', '')
S3_BUCKET_NAME = toolkit.config.get('ckanext.datopian.aws_bucket_name', '') 
S3_REGION_NAME = toolkit.config.get('ckanext.datopian.aws_region_name', '') 




class S3Client:
    def __init__(self) -> None:
        self.s3_client = boto3.client(
            's3',
            region_name=S3_REGION_NAME,
            aws_access_key_id=S3_ACCESS_KEY_ID,
            aws_secret_access_key=S3_SECRET_KEY_ID,
            config = Config(signature_version='s3v4'),
            endpoint_url=f'https://s3.{S3_REGION_NAME}.amazonaws.com'
            
        )

    def get_presigned_url(self, key_path, method='get_object', expires_in=3600, extra_params=None):    
        try:
            params = {
                'Bucket': S3_BUCKET_NAME,
                'Key': key_path
            }
            
            # Merge extra parameters if provided
            if extra_params:
                params.update(extra_params)
            
            # Generate the pre-signed URL
            url = self.s3_client.generate_presigned_url(
                ClientMethod=method,
                Params=params,
                ExpiresIn=expires_in
            )
            return url
        except ClientError as e:
            raise e
        
    def delete_object(self, bucket_name, key_path):
        try:
            response = self.s3_client.delete_object(
                Bucket=bucket_name,
                Key=key_path
            )
            return response
        except ClientError as e:
            print(f"An error occurred: {e}")
            raise e
        



def resource_download(package_type, id, resource_id, filename=None):
    context = {'model': model, 'session': model.Session,
               'user': c.user or c.author, 'auth_user_obj': c.userobj}
    
    s3_client = S3Client()
    try:
        rsc = get_action('resource_show')(context, {'id': resource_id})
        get_action('package_show')(context, {'id': id})
    except NotFound:
        return abort(404, _('Resource not found'))
    except NotAuthorized:
        return abort(401, _('Unauthorized to read resource %s') % id)
    
    if rsc.get('url_type') == 'upload':
        preview = request.args.get('preview', False)
        key_path = f"resources/{rsc['id']}/{filename}"

        try:
            if preview:
                url = s3_client.get_presigned_url(key_path)
            else:
                params = {
                    'ResponseContentDisposition':
                        'attachment; filename=' + filename,
                }
                url = s3_client.get_presigned_url(key_path, extra_params=params)
            return redirect(url)
        except ClientError as e:
            return abort(404, str(e))
        
    else:
        return redirect(rsc['url'])




s3_resource.add_url_rule(u'/<resource_id>/download',
                         view_func=resource_download)
s3_resource.add_url_rule(u'/<resource_id>/download/<filename>',
                         view_func=resource_download)


def get_blueprints():
    return [s3_resource]