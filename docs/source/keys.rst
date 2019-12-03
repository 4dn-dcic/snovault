================
Keys in Snovault
================

Broadly speaking in the 4DN space there are four different types of 'keys'. 'Unique', 'name' and 'identifying' keys are used in both Fourfront/CGAP and Snovault while 'lookup' keys are used only in FF/CGAP. This document will only touch on the first three. Below is a small table illustrating where each type of key is defined.


.. csv-table:: 
   :header: "Schema", "Collection", "Type"
   :widths: 10, 20, 10

   uniqueKey, identification_key, name_key


Unique Key and Identification Key
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

These two keys are bundled together because they are closely related. A unique key is denoted in the schema of an item. This is a constraint on the items in the database that says that no two items can share the same value in this field. Note that if there are multiple fields that are marked as uniqueKey's then either one can be used to uniquely identify items. In either case a identification key must be specified in the collection decorator, which must be one of the unique keys. If a identification key is not specified you will not be able to lookup items via the resource path with either of their unique keys. See the below examples from the tests.

.. code-block:: python

  # All of the following collections are referencing a schema which denotes
  # fields 'obj_id' and 'name' as uniqueKey's. In the below case though if you
  # wanted to look up an item by this type from the point of view of snovault
  # you would only be able to do so via uuid
  @collection('testing-keys')
  class TestingKeys(Item):
      """ Intended to test the behavior of uniqueKey value in schema """
      item_type = 'testing_keys'
      schema = load_schema('snovault:test_schemas/TestingKeys.json')


  # In this case we specify the identification_key to be the obj_id. This allows us to
  # use the resource path to get the item ie: Get /testing-keys-def/<obj_id>
  # Note that the resource path is still the uuid
  @collection('testing-keys-def', identification_key='testing_keys_def:obj_id')
  class TestingKeysDef(Item):
      """
      Intended to test the behavior of setting a identification key equal to one of the
      uniqueKey's specified in the schema. This should allow us to get the object
      via obj_id whereas before we could not.
      """
      item_type = 'testing_keys_def'
      schema = load_schema('snovault:test_schemas/TestingKeys.json')

Name Key
^^^^^^^^

The name key is a special field specified on the item type definition. It augments the resource path so that the '@id' field of the item contains a path using the name_key instead of the uuid. To be explicit, the name key must match the identification key and is only really useful for changing the resource path. See final example below.

.. code-block:: python

  # In this case we specify matching identification_key and name_key. This means that
  # the resource path is augmented to show the name_key instead of the uuid AND
  # you can get the item via resource path ie: Get /testing-keys-name/<name>
  # and that is what always shows up when you access that item
  @collection('testing-keys-name', identification_key='testing_keys_name:name')
  class TestingKeysName(Item):
      """
      We set name as a identification key so that it can be used as a name_key in the
      resource path. We should now see the name key in the @id field instead of
      the uuid
      """
      item_type = 'testing_keys_name'
      schema = load_schema('snovault:test_schemas/TestingKeys.json')
      name_key = 'name'
