Testing
========================

Since there is no deployable application using solely Snovault, tests are extra important and can be used to learn about the features on Snovault. We use the PyTest framework. Below is a brief walkthrough on several important aspects of how Snovault is tested.

Running tests
=============

To run specific tests locally::

    $ bin/test -k test_name

To run with a debugger::

    $ bin/test --pdb

Specific tests to run locally for schema changes::

    $ bin/test -k test_load_workbook

Run the Pyramid tests with::

    $ bin/test

Testing Views
=============

We create a variety of data types designed to test the underlying functionality of Snovault. These tests are meant to complement those found in the Fourfront/CGAP in that they test similar things but are independent of functionality specific to any single web application. Thus many types and properties are defined in ``testing_views.py``. Many of these types are defined very similarly in Fourfront/CGAP. We go through the ``Collection`` type below.

.. code-block:: python

  # Below is all that is needed for the Collection object. It extends the Collection
  # object defined in Snovault, renamed BaseCollection and is meant to be a base
  # type for other testing objects
  class Collection(BaseCollection):
      def __init__(self, *args, **kw):
          super(BaseCollection, self).__init__(*args, **kw)
          if hasattr(self, '__acl__'):
              return
          self.__acl__ = (ALLOW_SUBMITTER_ADD + ALLOW_EVERYONE_VIEW)

In addition to collection, we also define ``Item``, which serves as a base for other testing views. We also define calculated properties on ``Item``, an example of which is below.

.. code-block:: python

  # We mark calculated properties as such and categorize them. 'Add' is an 'action'
  # one may want to do to a collection of items, so we implement it as a calculated
  # property here by checking that the user who sent this request has permission
  @calculated_property(context=Item.Collection, category='action')
  def add(context, request):
      if request.has_permission('add'):
          return {
              'name': 'add',
              'title': 'Add',
              'profile': '/profiles/{ti.name}.json'.format(ti=context.type_info),
              'href': '{item_uri}#!add'.format(item_uri=request.resource_path(context)),
              }

In Snovault, we often take advantage of the behavior of embedded objects and calculated properties. To best test this functionality we define new types that have a structure that we can take advantage of in our testing. One of those items is explained below.

.. code-block:: python

  # Here we define a new collection with our newly defined Item as it's base
  # class. We load it's schema directly from the specified file.
  # We automatically should resolve all properties of the base
  # item as well as new ones here. One thing we can test with an object like this
  # is if an embedded object is properly resolved, hence it's name.
  @collection(
      name='embedding-tests',
      unique_key='accession',
      properties={
          'title': 'EmbeddingTests',
          'description': 'Listing of EmbeddingTests'
      })
  class EmbeddingTest(Item):
      item_type = 'embedding_test'
      schema = load_schema('snovault:test_schemas/EmbeddingTest.json')
      name_key = 'accession'

      # use TestingDownload to test
      embedded_list = [
          'attachment.*'
      ]

It should be clear that adding new testing collections to ``testing_views.py`` may be necessary to test new functionality.

Fixtures
========

We define many PyTest fixtures that usually serve a multitude of purposes, given below. Examples follow.

* Back-end specific. Some fixtures are used to create database sessions. This is needed when in a test we are posting data that we'd like to rollback at a later date.
* Spin up a test application. We define several different fixtures that spin up test applications in different contexts. This can be useful when testing that permission structures are functioning correctly, as you could write fixtures that create test applications that run as if different types of users were interacting with Snovault.
* Loading test data. Some fixtures are configured to not just construct but load and post test data to a specific application.

TestApp Fixtures
================

First we describe the ``conn`` fixture, which initiates an ``sqlalchemy`` connection, initiates a transaction, executes it, then rolls it back once the test is done.

.. code-block:: python

  # This fixture serves to configure tests to utilize a DB connection that we can
  # rollback after the test is done. This is super convenient for testing purposes
  # since it allows us to isolate test behavior very easily.
  @pytest.yield_fixture(scope='session')
  def conn(engine_url):
    from snovault.app import configure_engine
    from snovault.storage import Base

    engine_settings = {
        'sqlalchemy.url': engine_url,
    }

    engine = configure_engine(engine_settings)
    conn = engine.connect()
    tx = conn.begin()
    try:
        Base.metadata.create_all(bind=conn)
        yield conn
    finally:
        tx.rollback()
        conn.close()
        engine.dispose()

Next we go through three different TestApp fixtures that start test applications in different contexts. You can use these to test behavior that should work under one use but not under another.

.. code-block:: python

  # The following three fixtures define TestApp's in different states, most useful
  # when testing user permissions. Depending on which one you use, the types of
  # actions you can perform should be different, and thus PyTest leverages these
  # fixtures to test that behavior
  @pytest.fixture
  def testapp(app):
      '''TestApp with JSON accept header.
      '''
      from webtest import TestApp
      environ = {
          'HTTP_ACCEPT': 'application/json',
          'REMOTE_USER': 'TEST',
      }
      return TestApp(app, environ)


  @pytest.fixture
  def anontestapp(app):
      '''TestApp with JSON accept header.
      '''
      from webtest import TestApp
      environ = {
          'HTTP_ACCEPT': 'application/json',
      }
      return TestApp(app, environ)


  @pytest.fixture
  def authenticated_testapp(app):
      '''TestApp with JSON accept header for non-admin user.
      '''
      from webtest import TestApp
      environ = {
          'HTTP_ACCEPT': 'application/json',
          'REMOTE_USER': 'TEST_AUTHENTICATED',
      }
      return TestApp(app, environ)

Next, we give an example of a fixture that creates and posts test data. These are particularly useful when you'd like to post some data that is required to post additional data that is part of a test. You can combine these with different TestApp fixtures to verify certain data actions work with some users and not with others.

.. code-block:: python

  targets = [
    {'name': 'one', 'uuid': '775795d3-4410-4114-836b-8eeecf1d0c2f'},
    {'name': 'two', 'uuid': 'd6784f5e-48a1-4b40-9b11-c8aefb6e1377'},
  ]

  @pytest.fixture
  def link_targets(testapp):
    url = '/testing-link-targets-sno/'
    for item in targets:
      testapp.post_json(url, item, status=201)


Overview of Tests
=================

What follows is a bulleted list of test files with a short description on what each test file is testing. Note that at this time testing for CGAP is largely incomplete and should be improved in addition to testing new features.

* ``test_attachment.py`` : tests posting and downloading attachments
* ``test_authentication.py`` : verifies Snovault ACL permissions function correctly
* ``test_create_mapping.py`` : tests creating ES mappings for test items
* ``test_embed_utils.py`` : tests various helper functions related to resolving embedded objects/fields
* ``test_embedding.py`` : tests that data store objects properly resolve their embedded fields
* ``test_es_permissions.py`` : tests behavior of calculated properties that resolve permissions for objects
* ``test_indexing.py`` : tests adding, interacting with and searching for data in elasticsearch with test data models
* ``test_key.py`` : tests that we can post and update keys
* ``test_link.py`` : tests that we are able to properly update links within items
* ``test_logging.py`` : tests that our log infrastructure functions
* ``test_post_put_patch.py`` : tests various behavior involving posting/patching test data
* ``test_schemas.py`` : tests some basic things about our test data
* ``test_snowflake_hash.py`` : verifies snowflake_hash is functioning
* ``test_storage.py`` : does sanity checks on Postgres
* ``test_upgrader.py`` : tests that we can create/add update steps so we can update object schemas
* ``test_views.py`` : tests various routes that are reachable on the backend and are associated with test objects

Now, we will go through non-test files giving a brief description of each.

* ``authentication.py`` : contains some sample authentication infrastructure code. This is meant to be specific to the application, so it is included as part of testing only since if it is needed it should be implemented in the web app.
* ``authorization.py`` : just contains a ``groupfinder`` helper method that is needed for our testing infrastructure.
* ``conftest.py`` : configuration file for PyTest
* ``elasticsearch_fixture.py`` : contains fixtures for using elasticsearch
* ``postgresql_fixture.py`` : contains fixtures for using postgresql
* ``pyramidfixtures.py`` : contains fixtures specific to pyramid that we need
* ``root.py`` : defines ``TestRoot`` which extends the ``Root`` object from Snovault
* ``search.py`` : contains old search code. Search should be specific to the application so it is included in tests as it should not be needed for Fourfront/CGAP. Its main use now is for testing object interactions that are visible through search.
* ``serverfixtures.py`` : contains fixtures for setting up DB connections
* ``snowflake_hash.py`` : contains snowflake_hash
* ``testappfixtures.py`` : contains fixtures to setup various TestApps
* ``testing_key.py`` : contains a data fixture for a key
* ``testing_upgrader.py`` : contains a data fixture for upgrader
* ``testing_views.py`` : contains test object definitions. The full schemas are loaded from snovault.test_schemas.
* ``toolfixtures.py`` : contains some fixtures for app configuration

These are the most important things to know about testing Snovault. New test files should be added as appropriate.
