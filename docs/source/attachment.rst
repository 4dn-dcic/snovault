=======================
Attachments in Snovault
=======================

Snovault allows you to create items with an attachment blob, typically an image. To support this we extend the base ``Item`` type with ``ItemWithAttachment`` and provide some additional methods for handling blob attachment storage. Where to store attachment blobs is configurable but in production we use ``S3BlobStorage`` as opposed to ``RDBBlobStorage`` (which is used in some tests). Creating an item with an attachment is as simple as extending the ``ItemWithAttachment`` class. See example below from ``testing_views.py``.

.. code-block:: python

  @collection(
      'testing-downloads',
      properties={
          'title': 'Test download collection',
          'description': 'Testing. Testing. 1, 2, 3.',
      },
  )
  class TestingDownload(ItemWithAttachment):
      item_type = 'testing_download'
      schema = load_schema('snovault:test_schemas/TestingDownload.json')


Given this definition, any property of this item with the ``href`` field will be treated as an attachment field and processed. For example, if we look at the following test fixture we can see the use of 2 attachments on the same item. Both would be processed as such and stored on the appropriate storage platform defined in the registry settings. Typically this setting is carried over from Fourfront/CGAP and is set by setting an S3Blob Bucket, thus indicating that we will use S3 for our attachment blob storage.

.. code-block:: python

  @pytest.fixture
  def testing_download(testapp):
      url = '/testing-downloads/'
      item = {
          'attachment': {
              'download': 'red-dot.png',
              'href': RED_DOT,
          },
          'attachment2': {
              'download': 'blue-dot.png',
              'href': BLUE_DOT,
          },
      }
      res = testapp.post_json(url, item, status=201)
      return res.location
