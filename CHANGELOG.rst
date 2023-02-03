========
snovault
========

----------
Change Log
----------

7.2.0
=====

* Fix C4-984:

  * Add ``pip install wheel`` in ``make configure``.

  * Remove dependency in ``pyproject.toml`` on ``futures`` library.

* Fix C4-985:

  * Make a wrapper for ``pkg_resources.parse_version`` in ``upgrader.py``
    that parses the empty string as if `'1'` had been supplied.

* In ``Makefile``, add ``make test-full``.


7.1.1
=====

* Small fix/adjustment to snapshot related error handling when re-mapping


7.1.0
=====

* Supress log errors from skip_indexing
* Suppress errors from SQLAlchemy relationship overlap
* Add reindex_by_type capabilities
* Small changes to indexing tests to speed them up


7.0.0
=====

* Upgrades ElasticSearch to version 7 (OpenSearch 1.3 in production)
* Upgrades SQLAlchemy to 1.4.41 (and other associated versions)
* Adds B-Tree index on max_sid to optimize retrieval of this value in indexing
* Drop support for Python 3.7


6.0.8
=====

* Environment variable NO_SERVER_FIXTURES suppresses creation of server
  fixtures during testing.


6.0.7
=====

* Miscellaneous PEP8.


6.0.6
=====

* Evaluate KMS args as truthy for blob storage to avoid errors for empty string KMS key


6.0.5
=====

* Add a CHANGELOG.rst file.
* Add tests for consistency of version and changelog.
* Make dev dependency on docutils explicit, adding a constraint that gets rid of a deprecation warning.


6.0.4
=====

6.0.3
=====

`PR 225 Genelist upload (C4-875) <https://github.com/4dn-dcic/snovault/pull/225>`_

Instrumentation added to help debug C4-875.

* Improved error messages for ``ValidationFailure`` in ``attachment.py``.

Actual proposed fix:

* In ``attachment.py``, replaced ``mimetypes.guess_type`` with new function ``guess_mime_type``
  (adjusting the receipt of return value, since I adjusted that slightly to return the mime type,
  not a tuple of mime type and encoding).
* Make sure that we have useful return values for common file extensions.

Opportunistic:

* Better ``.flake8`` file excluding a bunch of whitespace-related issues we don't need to care about yet.
* Add a lint target to the ``Makefile``.
* Suppress an annoying warning from the ``jose`` package (included by ``moto 1.3.7``)
  about how it's not going to work in Python 3.9.
* Do keyword-calling of ``ValidationFailure`` in ``attachment.py`` just to clarify what the weird args are.
* Add an extra warning message in ``create_mapping.py`` for certain unusual argument combinations.
  (This had come up elsewhere in a discussion I had with Will and was just waiting for a PR to ride in on.)


6.0.2
=====

`PR 223 Index Delete Retry <https://github.com/4dn-dcic/snovault/pull/223>`_

* Retry delete_index in case of an error,
  likely related to a snapshot occurring at the same time as the delete operation.
  Give it two minutes (12 tries) to succeed.


6.0.1
=====

6.0.0
=====

`PR 224 Use dcicutils 4.0 <https://github.com/4dn-dcic/snovault/pull/224>`_

**NOTE:** The breaking change here is the use of ``dcicutils 4.x``.

* This accepts ``dcicutils 4.0``.
* Minor change to ``.gitignore`` to add ``.python-cmd``.
* Constrains ``boto3``, ``botocore``, ``boto3-stubs``, and ``botocore-stubs``.


5.7.0
=====

`PR 222 Invalidation Scope Fix (C4-854) <https://github.com/4dn-dcic/snovault/pull/222>`_

* Repairs several important cases in invalidation scope by revising the core algorithm,
  which is now described in the ``filter_invalidation_scope`` docstring.
* Should work correctly for object fields, links beyond depth ``1`` and ``*``.
* Other small changes include repairing the test script
  and allowing indexer worker runs to re-use testapp for 100 iterations
  (thus preserving cache, probably speeding up indexing and reducing DB load)


5.6.2
=====

`PR 221 Remove embeds of unmappable properties <https://github.com/4dn-dcic/snovault/pull/221>`_

* Here, we remove embeds of properties that cannot be mapped within our system,
  namely those that fall under ``additionalProperties`` or ``patternProperties`` in our schema.

* As far as I understand things, since these fields cannot be mapped, adding them to an item's embedding list
  will not work regardless of the changes here, specifically the explicit removal of the properties
  from the default embeds in ``find_default_embeds_for_schema``.
  Thus, no properties in the schema defined under ``additionalProperties`` or ``patternProperties`` can be embedded
  or used for invalidation scope with our current set-up,
  and significant refactoring would be required to make these work.


5.6.1
=====

`PR 220 Further upgrader version fix <https://github.com/4dn-dcic/snovault/pull/220>`_

The recent upgrader fix (in v.5.6.0) added the default version of ``1`` for upgrader calls,
but not all calls to the upgrader were included in the fix.
Specifically, the upgrader call within ``resources.py`` is still resulting in errors.
We fix that here, as well as the call within the possibly defunct ``batchupgrade.py`` for good measure.
(Grepping ``snovault`` for ``upgrader.upgrade`` didn't reveal any other instances of calls to the upgrader to fix.)


5.6.0
=====

`PR 218 Lock 3.8, Repair Upgraders <https://github.com/4dn-dcic/snovault/pull/218>`_

* Locks Python 3.8, which appears stable with no changes
* Default ``current_version`` in upgraders to ``1`` instead of ``''``,
  so items that do not have a default ``schema_version``
  will default to a sane value that should hit an upgrade target.


5.5.1
=====

`PR 217 Repair mirror health resolution <https://github.com/4dn-dcic/snovault/pull/217>`_

* Resolve ``IDENTITY`` so authenticated requests can be made with credentials


5.5.0
=====

5.4.0
=====

`PR 215 Fix Serializer <https://github.com/4dn-dcic/snovault/pull/215>`_

* Undo JSON serializer override,
  falling back to the pyramid default which appears to be ~10x more performant with waitress


5.3.0
=====

`PR 214 Type Specific Index Setting <https://github.com/4dn-dcic/snovault/pull/214>`_

* Implements type specific index settings, documenting the important settings
* Configurable by overriding the ``Collection.index_settings`` method
  to return a custom ``snovault.util.IndexSettings`` object


5.2.0
=====

`PR 213 Make pillow, wheel, and pyyaml be dev dependencies. If the portals wa... <https://github.com/4dn-dcic/snovault/pull/213>`_

* Make ``pillow``, ``wheel``, and ``pyyaml`` be dev dependencies.
  If the portals want them, they can make them be regular dependencies.


5.1.1
=====

`PR 212 Fix some dependencies to be a bit more flexible <https://github.com/4dn-dcic/snovault/pull/222>`_

* Various adjustments in ``pyproject.toml``.


5.1.0
=====

`PR 211 Python 3.7 compatibility changes (C4-753) <https://github.com/4dn-dcic/snovault/pull/224>`_

This change intends to let Snovault work in Python 3.7.

* Update ``psycopg2`` to use ``psycopg2-binary``.
* Use matrix format testing and adjust the way indices are built in so they include Python version number.
  Needed to assure proper cleanup, but also to avoid these different processes colliding with one another.
* Adjusted GA testing to use ``250`` timeout instead of ``200``.

Opportunistic:

* Phase out use of ``TRAVIS_JOB_ID`` in favor of ``TEST_JOB_ID``.
  A tiny bit of additional code is retained in case ``cgap-portal`` or ``fourfront`` still use any of this,
  but none of the calls in ``snovault`` try to use ``TRAVIS_JOB_ID`` any more.
* Rename the ``travis-test`` recipe to ``remote-test`` in ``Makefile``.


5.0.0
=====

`PR 210 Encryption Support <https://github.com/4dn-dcic/snovault/pull/210>`_

* Implements encryption support for S3BlobStorage
* Adds tests for (encrypted) S3BlobStorage (previously untested)
  by repurposing and slightly modifying the existing tests for the RDB blob storage


4.9.2
=====

`PR 209 Changes to remove variable imports from env_utils (C4-700) <https://github.com/4dn-dcic/snovault/pull/209>`_


Older Versions
==============

A record of older changes can be found
`in GitHub <https://github.com/4dn-dcic/utils/pulls?q=is%3Apr+is%3Aclosed>`_.
To find the specific version numbers, see the ``version`` value in
the ``poetry.app`` section of ``pyproject.toml`` for the corresponding change, as in::

   [poetry.app]
   name = "dcicutils"
   version = "100.200.300"
   ...etc.

This would correspond with ``dcicutils 100.200.300``.