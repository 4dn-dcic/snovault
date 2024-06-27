========
snovault
========

----------
Change Log
----------

11.17.0
=======

* Updates related to Python 3.12.
  - Had to update venusian (from 1.2.0) to 3.1.0.
  - Had to update pyramid (from 1.10.4) to 2.0.2 (for imp import not found).
    - Had to add pmdarima (no module pyramid.compat).
    - Had to define/update numpy (to 1.26.4) for this.
      - And had to update lower bound of Python (from 3.8.1) to 3.9 for this.
  - Had to dcicutils (from 8.11.0) to 8.13.0  (for pyramid update for imp import not found).


11.16.0
=======

* Update `/submission-schemas/` to capture required prop via new key in property schema


11.15.1
=======

* Update ``drs_download`` to not guard on Authentication, as this check is superfluous since @@drs as_user is evaluated


11.15.0
=======

* Update ``drs`` primitive to only return JSON


11.14.3
=======

* Fix `update-inserts-from-server` command to display `--help` option


11.14.2
=======

* Fix `update-inserts-from-server` command to move away from direct ES interaction
  * Rewrite signicantly
  * Add new options to allow for more flexible use


11.14.1
=======

* Create constants for submission-schemas endpoint to share with downstream portals


11.14.0
=======
* 2024-03-25
* Changes to loadxl to support tracking ingestion progess for smaht-submitr (via Redis).
* Changed dev_servers.py


11.13.0
=======

* Fix in loadxl to PATCH on validate_only for items which already exist;
  discovered during smaht-submitr testing.
* Fix in loadxl.normalize_deleted_properties which was creating/returning
  a new (an_item) item, which was messing up determination of identifying
  path for patch (as second_round_items comes from store but we had set uuid
  in an_item which, without this fix, became a different object).
* Added skip_links feature to loadxl which will cause reference/link integrity
  checking to be skipped altogether; this is (currently) only set by smaht-portal/
  ingestion/loadxl_extensions.py for smaht-submitr, since that process already
  does thorough reference integrity checking anyways (via structured_data).


11.12.4
=======

* Remove restricted permissions for AccessKey status to enable non-admins to delete access keys


11.12.3
=======

* Changed ACCESSION_PREFIX in server_defaults.py to GET_ACCESSION_PREFIX() function;
  called only within snovault (and only from schema_formats.py); to get around
  app_project call at file scope (came up as circular import in smaht ingester).


11.12.2
=======

* Gets total results from ES, then try to get exact count if total hits ES_MAX_HIT_TOTAL limitation


11.12.1
=======

* Repairs schema format validation


11.12.0
=======

* Change the exception message for a unresolved object reference (linkTo) in schema_validation.normalize_links.
* Added instance info to ERROR in loadxl.load_all_gen.
* Both of above in support of reference integrity validation code within smaht-submitr.


11.11.0
=======

* Removes strip of ``role.`` permissions so smaht-portal roles work


11.10.0
=======

* Version updates to dcicutils.
  Changes to itemize SMaHT submission ingestion create/update/diff situation.


11.9.0
======

* Added support for an optional gitinfo.json file (deployed via portal buildspec.yml).


11.8.0
======

* Add submission-schemas api


11.7.0
======
* Updated dcicutils to 8.6.0 (with minor fixes related to structured_data and SMaHT ingestion).


11.6.0
======
* Updated dcicutils to 8.4.1 (with structured_data).
* Updated loadxl to pass "filename" in yields (for smaht-portal/ingester).


11.5.0
======
* More work related to SMaHT ingestion.


11.4.0
======

* RAS updates


11.3.1
======

* Broaden schema ``$merge`` regex to allow mixin and other references


11.3.0
======

* Another thug commit to add CHANGELOG for below.


11.2.0
======

* Thug commit to change dcictuils from 8.2.0 to ^8.2.0.


11.1.0
======
* Merging in Doug's drr_schema_updates branch with new types.
* Added limited support to loadxl for required properties within anyOf of data type schemas.
* Merged in load_data_fix branch.
* Update dcicutils to 8.2.0
* 2023-11-02


11.0.1
======

* Repair reference to ``load_data_by_type`` to resolve correctly when loadxl
  is absent entirely from the application repo


11.0.0
======

* Upgrade to Python 3.11.
* Fixed access of user in types/access_key.py in access_key_add WRT request.validated['user'].
* Added identifyingProperties with just uuid in schemas/access_key.json.
* Fix in setup_eb.py to handle jsonschema in pyproject.toml like {extras = ..., version = ...}.
* Added snovault/commands/generate_local_access_key.py script; originally just for
  smaht-portal to create access-key for local dev/testing because doing it via UI
  not yet fully supported; but generally convenient for cgap-portal and fourfront as well.
  * Minor changes (e.g. create_testapp) to loadxl.py to help load data from a specified directory;
    called from dev_server.py; for creating access-keys on the fly after startup for local dev/testing.
    * Enhancement in load_data in loadxl.py to respect a fully qualified data directory path name,
      i.e. do not make it relative to the current working directory if it is fully qualified.
    * Updates to load_all_gen to allow object create/update with no uuid.
* Added snovault/commands/view_local_object.py script for dev/testing to
  retrieve and output a given object (uuid) from a locally running portal.
* Added support for consortia and submission_centers in ingestion_listener.py.
* Added unique_key to types/access_key.py (helps get rid of this in cgap-portal/fourfront).


10.0.5
======

* Bug fix in schema reference resolution when the schema is loaded from a file


10.0.4
======

* Bug fix in access key refresh to predicate on whether
expiration is enabled


10.0.3
======

* Update ``drs`` primitive to resolve specific access types with preferential defaulting to https, http


10.0.2
======

* Repair bug in ``permission`` implementation involving restricted fields
* Repair bug in user registration, allowing customization through ``app_project`` definition


10.0.1
======

* Extend ``FormatChecker`` to ensure date and date-time validation


10.0.0
======

* Updates ``jsonschema`` version, removing dependency on ``jsonschema-serialize-fork`` and allowing
  us to use ``$merge`` refs.
  * Breaking Change: dependencies --> dependentRequired in schema
  * Breaking Change: object serialization in schema no longer valid


9.1.1
=====

* Small fix for JWT Decode incompatible change

9.1.0
=====

* Fix for MIME type ordering in renderers.py (differs between cgap and fourfront).


9.0.0
=====

* Merge/unify ingestion and other code from cgap-portal and fourfront.


8.1.0
=====

* Add several modules/commands from upstream portals that are generic enough to live in
  this repository (to reduce code/library maintenace overhead)

* Port support for ``make deploy1`` from the portals:

  * In ``Makefile``:

    * Support for ``make deploy1``

    * Support for ``make psql-dev``

    * Support for ``make psql-test``

    * Support for ``make kibana-start`` (commented out for now, pending testing)

    * Support for ``make kibana-start-test`` (commented out)

    * Support for ``make kibana-stop`` (commented out)

  * In ``pyproject.toml``:

    * Template file ``development.ini.template``

    * Template file ``test.ini.template``

    * Support for ``prepare-local-dev`` script,
      which creates ``development.ini`` from ``development.ini.template``
      and ``test.ini`` from ``test.ini.template``.

 * Port the ``dev_servers.py`` support from CGAP.

 * In the ``scripts/`` dir:

   * Add ``scripts/psql-start``
     in support of ``make psql-dev`` and ``make psql-test``.


8.0.1
=====

* Fix some warnings from ``pytest``

  * If a method has "test" in its name but isn't a test, it needs a prefix "_"

* Fix some warnings from ``sqlalchemy``

  * ``session.connection()`` doesn't need to ``.connect()``
  * ``.join(x, y, ...)`` should be ``.join(x).join(y)...``
  * ``session.query(Foo).get(bar)`` should be ``session.get(Foo, bar)``


8.0.0
=====

* Redis support, adding /callback info to /auth0_config if a Redis server is configured


7.3.1
=====

* Change ``pytest.yield_fixture`` to ``pytest.yield``. This is techinically incompatible since it would break downstream portals if they were below ``pytest`` 6, but they are both at ``pytest 7`` now, so they should be unaffected.
* Address some places involving ``.execute(raw_string)`` that should be ``.execute(text(raw_string))``.


7.3.0
=====

* In ``Makefile``:

  * Make sure ``make test`` and ``make test-full`` also run ``make test-static``.

* In ``snovault/storage.py``:

  * Add ``POSTGRES_COMPATIBLE_MAJOR_VERSIONS`` (moved from ``snovault/tests/test_storage.py``)

* In ``snovault/elasticsearch/create_mapping.py``:

  * Per Will's direction, replace a call to ``run_index_data`` with a ``vapp`` creation and
    a call to an index post with given uuids.

* In ``snovault/elasticsearch/mpindexer.py``:

  * Very minor syntactic refactor to make a use of ``global`` more clear.

* In ``snovault/tools.py``:

  * Reimplement ``index_n_items_for_testing`` for better clarity and to fix a potential bug.

* In ``snovault/tests/test_indexing.py``

  * Various test optimizations using better synchronization for robustness.


7.2.1
=====

* In ``Makefile``:

  * New ``make`` target ``test-one``.


  * Separate testing of indexing tests from other unit tests,
    renaming the "npm" tests to "indexing" tests.

* Make github workflow ``main.yml`` consistent with ``Makefile`` changes.

* In ``pyproject.toml``:

  * Use ``pytest 7.2.2``.


7.2.0
=====

* In ``Makefile``:

  * Add ``make test-full`` to test like ``make test`` but without the ``instafail`` option.

  * Add ``make test-static`` to run static checks.

  * Add ``make test-one TEST_NAME=<test_name_or_filename_base>`` so you can test a single file or test from ``make``.
    This is not so important in ``snovault`` as in ``cgap-portal`` but I want the interface to be uniform.

  * In all testing, added ``SQLALCHEMY_WARN_20=1`` at start of command line to enable SQLAlchemy 2.0
    compatibility warnings, since we're using ``SQLAlchemy 1.4``, which has those warnings.

* In ``pyproject.toml``:
  * Require ``dcicutils 6,7`` for fixes to ``Eventually``.

  * Include ``pipdeptree`` as a dev dependency for debugging.

  * Remove "backports.statistics", needed for Python 3.3 support and earlier.

  * Bump python_magic foothold (no effective change, just faster locking)

  * Update some comments.

* In ``snovault/updater.py``:

  * Better error message for UUID integrity errors, noting they might not be conflits but just maybe also UUID missing.

  * Rearrange imports for clarity.

* In new file ``snovault/tools.py``:

  * New functions ``make_testapp``, ``make_htmltestapp``, ``make_authenticated_testapp``,
    ``make_submitter_testapp``, ``make_indexer_testapp``, and ``make_embed_testapp``.

  * New context managers ``being_nested`` and ``local_collections``.

  * New function ``index_n_items_for_testing``.

  These functions are potentially useful in the portal repos, so are not part of the test files.

* In file ``snovault/tests/serverfixtures.py``:

  * New fixture ``engine``

* In file ``snovault/tests/test_indexing.py``:

  * Material changes to testing to use better storage synchronization (semaphor-style rather than sleep-style),
    hopefully achieving fewer intermittent errors in testing both locally and in GA.

  * Bug fixes in a few tests that were assigning settings or other dictionary structures but not assuring an
    undo was done if the test failed.

* In files ``snovault/util.py``, ``snovault/tests/test_embedding.py``, ``snovault/tests/test_storage.py``:

  * Various changes for PEP8 or other readability reasons, including to satisfy ``PyCharm`` linters.

  * Allow Postgres 14 to be used.


7.1.3
=====

* In ``upgrader.py``, default ``parse_version`` argument to ``'0'``, rather than ``'1'``
  when ``None`` or the empty string is given.

* Remove the Python 3.7 classifier in ``pyproject.toml``.

* Add ``make clear-poetry-cache`` in ``Makefile``.

* Misc PEP8.


7.1.2
=====

* Fix C4-984:

  * Add ``pip install wheel`` in ``make configure``.

  * Remove dependency in ``pyproject.toml`` on ``futures`` library.

* Fix C4-985:

  * Make a wrapper for ``pkg_resources.parse_version`` in ``upgrader.py``
    that parses the empty string as if ``'1'`` had been supplied.

* Fix C4-987:

  * Use ``in str(exc.value)`` rather than ``in str(exc)`` after ``with pytest.raises(....) as exc:``


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
