from collections import defaultdict
from functools import reduce
from pyramid.decorator import reify
from .interfaces import (
    CALCULATED_PROPERTIES,
    TYPES,
)
from .schema_utils import combine_schemas


def includeme(config):
    registry = config.registry
    registry[TYPES] = TypesTool(registry)


def extract_schema_links(schema):
    if not schema:
        return
    for key, prop in schema['properties'].items():
        if 'items' in prop:
            prop = prop['items']
        if 'properties' in prop:
            for path in extract_schema_links(prop):
                yield (key,) + path
        elif 'linkTo' in prop:
            yield (key,)


class AbstractTypeInfo(object):
    """
    Contains meta information per item type held in a AbstractCollection.
    Does not actually use the item resource itself. Has properties for finding
    subtypes and child types of the given item, as well as a schema that is
    formed by merging the subschemas.
    """
    factory = None

    def __init__(self, registry, name):
        """
        Args:
            registry (Registry): the Pyramid registry
            name (str): item name, e.g. "MyItem"
        """
        self.types = registry[TYPES]
        self.name = name

    @reify
    def subtypes(self):
        return [
            ti.name for ti in self.types.by_item_type.values()
            if self.name in ([ti.name] + ti.base_types)
        ]

    @reify
    def child_types(self):
        """
        Need to handle non-abstract item types and abstract types separately,
        due to the pre-existing setup of base_types
        """
        child_types = [
            ti.name for ti in self.types.by_item_type.values()
            if self.name == ti.base_types[0]
        ]
        # child abstract types include the parent name, but not the type itself
        child_types.extend([
            ti.name for ti in self.types.by_abstract_type.values()
            if (self.name != ti.name and self.name in ti.base_types)
        ])
        return child_types

    @reify
    def schema(self):
        """
        Schema resulting from merging the subschemas. NOT equivalent to the
        schema defined for the item type itself (use the TypeInfo contained in
        registry[TYPES].by_abstract_type[item_type] for that)
        """
        subschemas = (self.types[name].schema for name in self.subtypes)
        return reduce(combine_schemas, subschemas)


class TypeInfo(AbstractTypeInfo):
    """
    Extends AbstractTypeInfo for meta information per item type that is held
    in a Collection. Has properties to reference the schema and various
    related attributes, as well as connections to properties on the item
    itself (through `factory`)
    """
    def __init__(self, registry, item_type, factory, abstract=False):
        """
        Args:
            registry (Registry): the Pyramid registry
            item_type (str): item type for the item, e.g. "my_item"
            factory (Item): actual resource for the item, e.g. snovault.resources.Item
            abstract (bool): used to keep track if this is used for an abstract item
        """
        super(TypeInfo, self).__init__(registry, factory.__name__)
        self.registry = registry
        self.item_type = item_type
        self.factory = factory
        self.base_types = factory.base_types
        self.aggregated_items = factory.aggregated_items
        self.embedded_list = factory.embedded_list
        self.is_abstract = abstract

    @reify
    def calculated_properties(self):
        return self.registry[CALCULATED_PROPERTIES]

    @reify
    def schema_version(self):
        try:
            return self.factory.schema['properties']['schema_version']['default']
        except (KeyError, TypeError):
            return None

    @reify
    def schema_links(self):
        return sorted('.'.join(path) for path in extract_schema_links(self.factory.schema))

    @reify
    def schema_keys(self):
        if not self.factory.schema:
            return ()
        keys = defaultdict(list)
        for key, prop in self.factory.schema['properties'].items():
            uniqueKey = prop.get('items', prop).get('uniqueKey')
            if uniqueKey is True:
                uniqueKey = '%s:%s' % (self.factory.item_type, key)
            if uniqueKey is not None:
                keys[uniqueKey].append(key)
        return keys

    @reify
    def merged_back_rev(self):
        merged = {}
        types = [self.name] + self.base_types
        for name in reversed(types):
            back_rev = self.types.type_back_rev.get(name, ())
            merged.update(back_rev)
        return merged

    @reify
    def schema(self):
        props = self.calculated_properties.props_for(self.factory)
        schema = self.factory.schema or {'type': 'object', 'properties': {}}
        schema = schema.copy()
        schema['properties'] = schema['properties'].copy()
        for name, prop in props.items():
            if prop.schema is not None:
                schema['properties'][name] = prop.schema
        return schema


class TypesTool(object):
    """
    Helper class used to register and store TypeInfo/AbstractTypeInfo classes
    corresponding to different item types.
    Whether an item type is abstract or not depends on the collection decorator
    used for the function; see snovault.config.py for more information.
    Below will refer to registry[TYPES] as "types"...

    Includes:
    - TypeInfo objects (for Collections) that are registered in types.all by
      item type (e.g. my_item), item name (e.g. MyItem), and item class; also
      types.by_item_type using item_type. Uses `register` method.
    - AbstractTypeInfo objects (for AbstractCollections) that are registed in
      types.all by item name and item class. Additionally, register the
      TypeInfo for the item in types.by_abstract_type using item_type.
      Uses the `register_abstract` method.
    """
    def __init__(self, registry):
        self.registry = registry
        self.by_item_type = {}
        self.by_abstract_type = {}
        self.type_back_rev = {}
        self.all = {}

    def register(self, factory):
        name = factory.__name__
        item_type = factory.item_type or name
        ti = TypeInfo(self.registry, item_type, factory)
        self.all[ti.item_type] = self.by_item_type[ti.item_type] = ti
        self.all[ti.name] = ti
        self.all[ti.factory] = ti
        # for base in ti.base_types:
        #     self.register_abstract(base)

        # Calculate the reverse rev map
        for prop_name, spec in factory.rev.items():
            rev_type_name, rel = spec
            back = self.type_back_rev.setdefault(rev_type_name, {}).setdefault(rel, set())
            back.add((ti.name, prop_name))

        return ti

    def register_abstract(self, factory):
        # create the TypeInfo and register in self.by_abstract_type
        # `item_type` is likely not set on the Items of abstract collections
        name = factory.__name__
        item_type = factory.item_type or name
        ti = TypeInfo(self.registry, item_type, factory, abstract=True)
        self.by_abstract_type[ti.item_type] = ti

        # now create the AbstractTypeInfo, which is also registered
        abstract_ti = AbstractTypeInfo(self.registry, name)
        self.all[factory] = self.all[name] = abstract_ti
        return abstract_ti

    def __contains__(self, name):
        return name in self.all

    def __getitem__(self, name):
        return self.all[name]
