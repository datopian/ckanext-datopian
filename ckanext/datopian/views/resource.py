import os
import logging
import cgi
import flask
from ckantoolkit import _, request, c, g
import ckantoolkit as toolkit
import ckan.logic as logic
import ckan.lib.base as base
from typing import Any, Optional, Union

import ckan.model as model
from botocore.exceptions import ClientError
from botocore.config import Config
import logging
import boto3
from flask.views import MethodView
import ckan.lib.base as base
from ckan.lib.helpers import helper_functions as h
import ckan.lib.navl.dictization_functions as dict_fns
import ckan.logic as logic
import ckan.model as model
from ckan.common import _, config, g, request, current_user
from ckan.views.dataset import (
    _get_pkg_template, _get_package_type, _setup_template_variables
)


from ckan.types import Context, Response

Blueprint = flask.Blueprint
NotFound = logic.NotFound
NotAuthorized = logic.NotAuthorized
get_action = logic.get_action
abort = base.abort
redirect = toolkit.redirect_to
check_access = logic.check_access
get_action = logic.get_action
tuplize_dict = logic.tuplize_dict
clean_dict = logic.clean_dict
parse_params = logic.parse_params
flatten_to_string_key = logic.flatten_to_string_key
ValidationError = logic.ValidationError


s3_resource = Blueprint(
    u's3_resource',
    __name__,
    url_prefix=u'/dataset/<id>/resource',
    url_defaults={u'package_type': u'dataset'}
)



# logging.basicConfig(level=logging.DEBUG)
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



class CreateView(MethodView):
    def post(self, package_type: str, id: str) -> Union[str, Response]:
        save_action = request.form.get(u'save')
        data = clean_dict(
            dict_fns.unflatten(tuplize_dict(parse_params(request.form)))
        )

        # we don't want to include save as it is part of the form
        del data[u'save']
        resource_id = data.pop(u'id')

        embed_url = data.pop(u'embedurl')
        data.pop(u'oldurl')
        if embed_url:
            data[u'url'] = embed_url

        context: Context = {
            u'user': current_user.name,
            u'auth_user_obj': current_user
        }

        # see if we have any data that we are trying to save
        data_provided = False
        for key, value in data.items():
            if (
                    (value or isinstance(value, cgi.FieldStorage))
                    and key != u'resource_type'):
                data_provided = True
                break

        if not data_provided and save_action != u"go-dataset-complete":
            if save_action == u'go-dataset':
                # go to final stage of adddataset
                return h.redirect_to(u'{}.edit'.format(package_type), id=id)
            # see if we have added any resources
            try:
                data_dict = get_action(u'package_show')(context, {u'id': id})
            except NotAuthorized:
                return base.abort(403, _(u'Unauthorized to update dataset'))
            except NotFound:
                return base.abort(
                    404,
                    _(u'The dataset {id} could not be found.').format(id=id)
                )
            if not len(data_dict[u'resources']):
                # no data so keep on page
                msg = _(u'You must add at least one data resource')
                # On new templates do not use flash message

                errors: dict[str, Any] = {}
                error_summary = {_(u'Error'): msg}
                return self.get(package_type, id, data, errors, error_summary)

            # XXX race condition if another user edits/deletes
            data_dict = get_action(u'package_show')(context, {u'id': id})
            get_action(u'package_update')(
                Context(context, allow_state_change=True),
                dict(data_dict, state=u'active')
            )
            return h.redirect_to(u'{}.read'.format(package_type), id=id)

        data[u'package_id'] = id
        try:
            if resource_id:
                data[u'id'] = resource_id
                get_action(u'resource_update')(context, data)
            else:
                get_action(u'resource_create')(context, data)
        except ValidationError as e:
            errors = e.error_dict
            error_summary = e.error_summary
            if data.get(u'url_type') == u'upload' and data.get(u'url'):
                data[u'url'] = u''
                data[u'url_type'] = u''
                data[u'previous_upload'] = True
            return self.get(package_type, id, data, errors, error_summary)
        except NotAuthorized:
            return base.abort(403, _(u'Unauthorized to create a resource'))
        except NotFound:
            return base.abort(
                404, _(u'The dataset {id} could not be found.').format(id=id)
            )
        if save_action == u'go-metadata':
            # XXX race condition if another user edits/deletes
            data_dict = get_action(u'package_show')(context, {u'id': id})
            get_action(u'package_update')(
                Context(context, allow_state_change=True),
                dict(data_dict, state=u'active')
            )
            return h.redirect_to(u'{}.read'.format(package_type), id=id)
        elif save_action == u'go-dataset':
            # go to first stage of add dataset
            return h.redirect_to(u'{}.edit'.format(package_type), id=id)
        elif save_action == u'go-dataset-complete':

            return h.redirect_to(u'{}.read'.format(package_type), id=id)
        else:
            # add more resources
            return h.redirect_to(
                u'{}_resource.new'.format(package_type),
                id=id
            )

    def get(self,
            package_type: str,
            id: str,
            data: Optional[dict[str, Any]] = None,
            errors: Optional[dict[str, Any]] = None,
            error_summary: Optional[dict[str, Any]] = None) -> str:
        # get resources for sidebar
        context: Context = {
            u'user': current_user.name,
            u'auth_user_obj': current_user
        }
        try:
            pkg_dict = get_action(u'package_show')(context, {u'id': id})
        except NotFound:
            return base.abort(
                404, _(u'The dataset {id} could not be found.').format(id=id)
            )
        try:
            check_access(
                u'resource_create', context, {u"package_id": pkg_dict["id"]}
            )
        except NotAuthorized:
            return base.abort(
                403, _(u'Unauthorized to create a resource for this package')
            )

        package_type = pkg_dict[u'type'] or package_type

        errors = errors or {}
        error_summary = error_summary or {}
        extra_vars: dict[str, Any] = {
            u'data': data,
            u'errors': errors,
            u'error_summary': error_summary,
            u'action': u'new',
            u'resource_form_snippet': _get_pkg_template(
                u'resource_form', package_type
            ),
            u'dataset_type': package_type,
            u'pkg_name': id,
            u'pkg_dict': pkg_dict
        }
        template = u'package/new_resource_not_draft.html'
        if pkg_dict[u'state'].startswith(u'draft'):
            extra_vars[u'stage'] = ['complete', u'active']
            template = u'package/new_resource.html'
        return base.render(template, extra_vars)
    

class EditView(MethodView):
    def _prepare(self, id: str):
        user = current_user.name
        context: Context = {
            u'api_version': 3,
            u'for_edit': True,
            u'user': user,
            u'auth_user_obj': current_user
        }
        try:
            check_access(u'package_update', context, {u'id': id})
        except NotAuthorized:
            return base.abort(
                403,
                _(u'User %r not authorized to edit %s') % (user, id)
            )
        return context

    def post(self, package_type: str, id: str,
             resource_id: str) -> Union[str, Response]:
        context = self._prepare(id)
        data = clean_dict(
            dict_fns.unflatten(tuplize_dict(parse_params(request.form)))
        )

        # we don't want to include save as it is part of the form
        del data[u'save']

        data[u'package_id'] = id
        embed_url = data.pop(u'embedurl')
        if embed_url:
            data[u'url'] = embed_url

            oldurl = data.pop(u'oldurl')
            url_type = data.get(u'url_type', '')
            if (oldurl != embed_url) and url_type == u'upload':
                s3_client = S3Client()
                key_path = f"resources/{resource_id}/{oldurl.split('/')[-1]}"
                try:
                    response = s3_client.delete_object(S3_BUCKET_NAME,key_path)
                except ClientError as e:
                    raise e
        try:
            if resource_id:
                data[u'id'] = resource_id
                get_action(u'resource_update')(context, data)
            else:
                get_action(u'resource_create')(context, data)
        except ValidationError as e:
            errors = e.error_dict
            error_summary = e.error_summary
            return self.get(
                package_type, id, resource_id, data, errors, error_summary
            )
        except NotAuthorized:
            return base.abort(403, _(u'Unauthorized to edit this resource'))
        return h.redirect_to(
            u'{}_resource.read'.format(package_type),
            id=id, resource_id=resource_id
        )

    def get(self,
            package_type: str,
            id: str,
            resource_id: str,
            data: Optional[dict[str, Any]] = None,
            errors: Optional[dict[str, Any]] = None,
            error_summary: Optional[dict[str, Any]] = None) -> str:
        context = self._prepare(id)
        pkg_dict = get_action(u'package_show')(context, {u'id': id})

        try:
            resource_dict = get_action(u'resource_show')(
                context, {
                    u'id': resource_id
                }
            )
        except NotFound:
            return base.abort(404, _(u'Resource not found'))

        if pkg_dict[u'state'].startswith(u'draft'):
            return CreateView().get(package_type, id, data=resource_dict)

        # resource is fully created
        resource = resource_dict
        # set the form action
        form_action = h.url_for(
            u'{}_resource.edit'.format(package_type),
            resource_id=resource_id, id=id
        )
        if not data:
            data = resource_dict

        package_type = pkg_dict[u'type'] or package_type

        errors = errors or {}
        error_summary = error_summary or {}
        extra_vars: dict[str, Any] = {
            u'data': data,
            u'errors': errors,
            u'error_summary': error_summary,
            u'action': u'edit',
            u'resource_form_snippet': _get_pkg_template(
                u'resource_form', package_type
            ),
            u'dataset_type': package_type,
            u'resource': resource,
            u'pkg_dict': pkg_dict,
            u'form_action': form_action
        }
        return base.render(u'package/resource_edit.html', extra_vars)




s3_resource.add_url_rule(u'/<resource_id>/download',
                         view_func=resource_download)
s3_resource.add_url_rule(u'/<resource_id>/download/<filename>',
                         view_func=resource_download)

s3_resource.add_url_rule(u'/new', view_func=CreateView.as_view(str(u'new')))

s3_resource.add_url_rule(
    u'/<resource_id>/edit', view_func=EditView.as_view(str(u'edit'))
)


def get_blueprints():
    return [s3_resource]