import json
import logging
import ckan.plugins.toolkit as tk
import ckan.plugins as p
import ckan.lib.dictization.model_dictize as model_dictize
import ckan.model as model

ValidationError = tk.ValidationError
_get_or_bust = tk.get_or_bust

log = logging.getLogger(__name__)

@p.toolkit.chained_action
@tk.side_effect_free
def package_search(up_func, context, data_dict):
    result = up_func(context, data_dict)

    # Add bilingual groups 
    for pkg in result['results']:
        try:
            pkg['total_downloads'] = tk.get_action('package_stats')(context, {'package_id': pkg['id']})

        except:
            log.error('package {id} download stats not available'.format(id=pkg['id']))
            pkg['total_downloads'] = 0

    return result

@p.toolkit.chained_action   
@tk.side_effect_free
def package_show(up_func,context,data_dict): 
    result = up_func(context, data_dict)

    id = result.get('id')
    try:
        result['total_downloads'] = tk.get_action('package_stats')(context, {'package_id': id})
    except:
        log.error(f'package {id} download stats not available')
        result['total_downloads'] = 0

    return result
