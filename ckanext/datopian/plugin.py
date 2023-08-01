import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from flask import Blueprint, render_template
from ckanext.datopian.actions import package_search, package_show


def hello_plugin():
    return u'Hello from the Datopian Theme extension'


class DatopianPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IBlueprint)
    plugins.implements(plugins.IActions)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
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
        return blueprint


    # IActions
    def get_actions(self):
        if "googleanalytics" in toolkit.config.get("ckan.plugins", "").split():
            # if so, add the action to the list of actions
            return {
                'package_search': package_search,
                'package_show': package_show
            }

        return {}
