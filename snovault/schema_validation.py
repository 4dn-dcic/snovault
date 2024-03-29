from copy import deepcopy
from jsonschema import Draft202012Validator
from jsonschema import validators
from jsonschema.exceptions import ValidationError
from pyramid.settings import asbool
from pyramid.threadlocal import get_current_request
from pyramid.traversal import find_resource


NO_DEFAULT = object()


def get_resource_base(validator, linkTo):
    from snovault import COLLECTIONS
    request = get_current_request()
    collections = request.registry[COLLECTIONS]
    if validator.is_type(linkTo, 'string'):
        resource_base = collections.get(linkTo, request.root)
    else:
        resource_base = request.root
    return resource_base


def normalize_links(validator, links, linkTo):
    skip_links = (request := get_current_request()) and asbool(request.params.get('skip_links', False))
    resource_base = get_resource_base(validator, linkTo)
    normalized_links = []
    errors = []
    for link in links:
        try:
            # Difference between us and IGVF - it is possible in our system in a roundabout way to refer to a link
            # not by string but by dictionary, in most cases this shouldn't matter but does come up occasionally in
            # unit tests (looking for errors) - Will July 31 2023
            if isinstance(link, dict):
                for possible in ['uuid', '@id']:
                    if possible in link:
                        link = link[possible]
                        break
                else:
                    raise KeyError
            normalized_links.append(
                str(find_resource(resource_base, link.replace(':', '%3A')).uuid)
            )
        except KeyError:

            # 2024-02-13: To help out smaht-submitr refererential integrity checking,
            # include the schema type name (linkTo) as well as the idenitifying value (link).

            # 2024-02-21/dmichaels:
            # If skip_links then ignore reference/linkTo errors.
            # Currently ONLY used by smaht-submitr (via smaht-portal/loadxl_extensions.py).

            if not skip_links:
                errors.append(
                    ValidationError(f"Unable to resolve link: /{linkTo}/{link}")
                )
            normalized_links.append(
                link
            )
    return normalized_links, errors


def should_mutate_properties(validator, instance):
    if validator.is_type(instance, 'object'):
        return True
    return False


def get_items_or_empty_object(validator, subschema):
    items = subschema.get('items', {})
    if validator.is_type(items, 'object'):
        return items
    return {}


def maybe_normalize_links_to_uuids(validator, property, subschema, instance):
    errors = []
    if 'linkTo' in subschema:
        link = instance.get(property)
        if link:
            normalized_links, errors = normalize_links(
                validator,
                [link],
                subschema.get('linkTo'),
            )
            instance[property] = normalized_links[0]
    if 'linkTo' in get_items_or_empty_object(validator, subschema):
        links = instance.get(property, [])
        if links:
            normalized_links, errors = normalize_links(
                validator,
                links,
                subschema.get('items').get('linkTo'),
            )
            instance[property] = normalized_links
    for error in errors:
        yield error


def set_defaults(validator, property, subschema, instance):
    if 'default' in subschema:
        instance.setdefault(
            property,
            deepcopy(subschema['default'])
        )
    if 'serverDefault' in subschema:
        server_default = validator.server_default(
            instance,
            subschema
        )
        if server_default is not NO_DEFAULT:
            instance.setdefault(
                property,
                server_default
            )


def extend_with_default(validator_class):
    validate_properties = validator_class.VALIDATORS['properties']

    def mutate_properties(validator, properties, instance, schema):
        for property, subschema in properties.items():
            if not validator.is_type(subschema, 'object'):
                continue
            yield from maybe_normalize_links_to_uuids(validator, property, subschema, instance)
            set_defaults(validator, property, subschema, instance)

    def before_properties_validation_hook(validator, properties, instance, schema):
        if should_mutate_properties(validator, instance):
            yield from mutate_properties(validator, properties, instance, schema)
        yield from validate_properties(validator, properties, instance, schema)

    return validators.extend(
        validator_class, {'properties': before_properties_validation_hook},
    )


ExtendedValidator = extend_with_default(Draft202012Validator)


class SerializingSchemaValidator(ExtendedValidator):

    SERVER_DEFAULTS = {}

    def add_server_defaults(self, server_defaults):
        self.SERVER_DEFAULTS.update(server_defaults)
        return self

    def serialize(self, instance):
        self._original_instance = instance
        self._mutated_instance = deepcopy(
            self._original_instance
        )
        errors = list(
            self.iter_errors(
                self._mutated_instance
            )
        )
        return self._mutated_instance, errors

    def server_default(self, instance, subschema):
        factory_name = subschema['serverDefault']
        factory = self.SERVER_DEFAULTS[factory_name]
        return factory(instance, subschema)
