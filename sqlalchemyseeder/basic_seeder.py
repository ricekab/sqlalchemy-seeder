import json

import yaml


class BasicSeeder(object):
    """ Directly converts objects from dictionary without any further processing. """

    @staticmethod
    def entity_from_dict(entity_dict, entity_class):
        """ Created an entity using the dictionary as initializer arguments. """
        return entity_class(**entity_dict)

    @staticmethod
    def entity_from_json_string(json_string, entity_class):
        """ Extract entity from given json string. """
        return BasicSeeder.entity_from_dict(json.loads(json_string), entity_class)

    @staticmethod
    def entity_from_yaml_string(yaml_string, entity_class):
        """ Extract entity from given yaml string. """
        return BasicSeeder.entity_from_dict(yaml.load(yaml_string), entity_class)
