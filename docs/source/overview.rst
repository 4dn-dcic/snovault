===============================
Overview
===============================

This document does not contain installation or operating instructures.  See index.rst for that.

Snovault as it exists now only serves as a backend for the Fourfront and CGAP Portals. It's documentation is in serious need of updates.

SOURCE CODE ORGANIZATION
------------------------

	* the top level contains configuration files and install scripts along with other accessory directories
		- *bin* - command line excutables (see src/commmands) from buildout (see PyramidDocs_)
		- *develop* & *develop-eggs* - source and python eggs (created by buildout)
		- *docs* - documentation (including this file)
		- *eggs* - Python dependencies from PyPi (created by buildout)
		- *parts* - wsgi interfaces and ruby dependencies (gems) (created by buildout)
		- *scripts* - cron jobs

	* src directory - contains all the python and javascript code for front and backends
		- *commands* - the python source for command line scripts used for synching, indexing and other utilities independent of the main Pyramid application
		- *elasticsearch* - contains code relevant for interacting with elasticsearch
		- *test_schemas* - JSON schemas (JSONSchema_, JSON-LD_) for tests
		- *tests* - Unit and integration tests
		- *snovault* - contains base code

Backend/Frontend
-----------

XXX: All out of date. Lots of work necessary in this document.

**API**

Parameters (to be supplied in POST object or via GET url parameters):
---------------------------------------------------------------------
	* datastore=(database|elasticsearch) default: elasticsearch
	* format=json  Return JSON objects instead of XHTML from browser.
	* limit=((int)|all) return only some or all objects in a collection
	* Searching
		*


.. _Pyramid: http://www.pylonsproject.org/
.. _JSONSchema: http://json-schema.org/
.. _JSON-LD:  http://json-ld.org/
.. _Elasticsearch: http://www.elasticsearch.org/
.. _PyramidDocs: http://docs.pylonsproject.org/en/latest/
