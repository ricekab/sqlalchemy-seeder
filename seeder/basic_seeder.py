import json


class BasicSeeder(object):
    """ Directly creates objects from dictionary without any further processing. """

    @staticmethod
    def entity_from_dict(entity_dict, entity_class):
        return entity_class(**entity_dict)

    @staticmethod
    def entity_from_json(entity_json, entity_class):
        return BasicSeeder.entity_from_dict(json.loads(entity_json), entity_class)