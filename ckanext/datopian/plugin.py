import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from flask import Blueprint, render_template
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
from ckanext.datopian.views.resource import get_blueprints
from ckanext.datopian.views.api import get_blueprintapi
import logging
import json
import ckan.logic as logic
import ckan

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

S3_ACCESS_KEY_ID =  toolkit.config.get('ckanext.datopian.aws_access_key_id', '')
S3_SECRET_KEY_ID = toolkit.config.get('ckanext.datopian.aws_secret_key_id', '')
S3_BUCKET_NAME = toolkit.config.get('ckanext.datopian.aws_bucket_name', '') 
S3_REGION_NAME = toolkit.config.get('ckanext.datopian.aws_region_name', '') 

s3_client = boto3.client(
    's3',
    region_name=S3_REGION_NAME,
    aws_access_key_id=S3_ACCESS_KEY_ID,
    aws_secret_access_key=S3_SECRET_KEY_ID,
    config = Config(signature_version='s3v4'),
    endpoint_url=f'https://s3.{S3_REGION_NAME}.amazonaws.com'
    
)

def hello_plugin():
    return u'Hello from the Datopian Theme extension'


class DatopianPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IBlueprint)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.IResourceController)


    def get_actions(self):
        return {
            'create-multipart-upload': create_multipart_upload,
            'prepare-upload-parts': prepare_upload_parts,
            'complete-multipart-upload': complete_multipart_upload,
            'list-parts': list_parts,
            'abort-multipart-upload': abort_multipart_upload,
            'sign-part': sign_part,
            'dataset_provider': dataset_provider
        }

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_template_directory(config_, 'theme/templates')
        toolkit.add_resource('assets',
                             'datopian')

    # IBlueprint

    def get_blueprint(self):
        u'''Return a Flask Blueprint object to be registered by the app.'''
        # Create Blueprint for plugin
        blueprint = Blueprint(self.name, self.__module__)
        blueprint.template_folder = u'templates'
        # Add plugin url rules to Blueprint object
        blueprint.add_url_rule('/hello_plugin', '/hello_plugin', hello_plugin)
        return get_blueprints() + get_blueprintapi()


    # IResourceController

    def before_create(self, context, resource_dict):
        '''Required by IResourceController'''
        pass

    def before_show(self, resource_dict):
        '''Required by IResourceController'''
        pass

    def after_create(self, context, resource_dict):
        '''Required by IResourceController'''
        pass

    def after_delete(self, context, resource_dict):
        '''Required by IResourceController'''
        pass

    def before_resource_update(self, context, current, resource_dict):
        '''Required by IResourceController'''
        pass

    def before_update(self, context, current, resource_dict):
        '''Required by IResourceController'''
        pass

    def after_resource_update(self, context, resource_dict):
        '''Required by IResourceController'''
        pass

    def after_update(self, context, resource_dict):
        '''Required by IResourceController'''
        pass

    def before_delete(self, context, resource, resources):
        # Delete the resource from the storage
        for rs in resources:
            if rs.get('id') == resource.get('id'):
                key_path = f"resources/{rs['id']}/{rs['name']}"
                try:
                    response = s3_client.delete_object(
                        Bucket=S3_BUCKET_NAME,
                        Key=key_path
                    )
                except ClientError as e:
                    raise e


def create_multipart_upload(context, data_dict):
    file = data_dict.get('file')
    # file_hash = data_dict.get('file_hash')
    content_type = data_dict.get('contentType')
    filename = file.get('name')
    resourceid = data_dict.get('resourceId')

    # return {'key': f'resources/{filename}'}

    try:
        response = s3_client.create_multipart_upload(
            Bucket=S3_BUCKET_NAME,
            Key=f'resources/{resourceid}/{filename}',
            ContentType=content_type,
            # Metadata={'x-amz-meta-file-hash': file_hash}
        )
        
        print(response)
        return {'uploadId': response['UploadId'], 'key': response['Key']}
    except ClientError as e:
        return {'error': str(e)}
    

def prepare_upload_parts(context, data_dict):
    part_data = data_dict.get('partData')
    parts = part_data.get('parts')

    presigned_urls = {}
    for part in parts:
        try:
            response = s3_client.generate_presigned_url(
                'upload_part',
                Params={
                    'Bucket': S3_BUCKET_NAME,
                    'Key': part_data['key'],
                    'PartNumber': part['number'],
                    'UploadId': part_data['uploadId']
                },
                ExpiresIn=3600
            )
            presigned_urls[part['number']] = response
        except ClientError as e:
            return {'error': str(e)}

    return {'presignedUrls': presigned_urls}


def list_parts(context, data_dict):
    key = data_dict.get('key')
    upload_id = data_dict.get('uploadId')

    try:
        response = s3_client.list_parts(
            Bucket=S3_BUCKET_NAME,
            Key=key,
            UploadId=upload_id
        )
        return response.get('Parts', [])
    except ClientError as e:
        return {'error': str(e)}
    

def complete_multipart_upload(context, data_dict):
    key = data_dict.get('key')
    upload_id = data_dict.get('uploadId')
    parts = data_dict.get('parts')

    # Ensure parts list contains only ETag and PartNumber
    formatted_parts = [{'ETag': part['ETag'], 'PartNumber': part['PartNumber']} for part in parts]
    try:
        response = s3_client.complete_multipart_upload(
            Bucket=S3_BUCKET_NAME,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={'Parts': formatted_parts}
        )
        return response
    except ClientError as e:
        return {'error': str(e)}


def abort_multipart_upload(context, data_dict):
    print(data_dict)
    key = data_dict.get('key')
    upload_id = data_dict.get('uploadId')

    try:
        response = s3_client.abort_multipart_upload(
            Bucket=S3_BUCKET_NAME,
            Key=key,
            UploadId=upload_id
        )
        return response
    except ClientError as e:
        return {'error': str(e)}


def sign_part(context, data_dict):
    key = data_dict.get('key')
    upload_id = data_dict.get('uploadId')
    part_number = int(data_dict.get('partNumber'))

    try:
        url = s3_client.generate_presigned_url(
            'upload_part',
            Params={
                'Bucket': S3_BUCKET_NAME,
                'Key': key,
                'PartNumber': part_number,
                'UploadId': upload_id
            },
            ExpiresIn=3600
        )
        
        return {'url': url}
    except ClientError as e:
        return {'error': str(e)}
    


@logic.validate(ckan.logic.schema.default_autocomplete_schema)
def dataset_provider(context, data_dict):
   provider = toolkit.config.get('ckan.access_provider', '').split(' ')
   q = data_dict['q']


   result = []
   if q:
        provider = [p for p in provider if q in p] 
        
        for p in provider:
            options = {}
            for k in ['id', 'name', 'title']:
                options[k] = p
            result.append(options)
   return result