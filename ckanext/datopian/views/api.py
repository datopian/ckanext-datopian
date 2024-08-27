import flask
import logging
from ckan.views.api import _finish_ok
from ckan.types import Context, Response
import ckan.logic as logic
from ckan.common import json, _, g, request, current_user

log = logging.getLogger(__name__)

Blueprint = flask.Blueprint
check_access = logic.check_access
get_action = logic.get_action
API_REST_DEFAULT_VERSION = 1

api = Blueprint('agrfapi', __name__, url_prefix='/api/2/util/dataset_provider')\

def dataset_provider(ver: int = API_REST_DEFAULT_VERSION):
    q = request.args.get(u'q', u'')
    limit = request.args.get(u'limit', 10)
    providers = []
    if q:
        context: Context = {
            'user': current_user.name,
            'auth_user_obj': current_user,
        }
        data_dict = {u'q': q, u'limit': limit}
        providers = get_action(u'dataset_provider')(context, data_dict)

    
    return _finish_ok(providers)

api.add_url_rule(u'/autocomplete', view_func=dataset_provider, methods=[u'GET'])

def get_blueprintapi():
    return [api]


