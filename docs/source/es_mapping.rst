Elasticsearch Mapping
=====================

Although Elasticsearch (ES) can dynamically create mappings for ingested documents, we specify specific mappings to optimize the storage and searching. This is done through `create_mapping.py <https://github.com/4dn-dcic/snovault/blob/master/src/snovault/elasticsearch/create_mapping.py>`_, along with a number of other ES-related operations. After running buildout, you can call the ``main`` function of create_mapping.py with ``bin/create-mapping``.

This document provides an overview for the Snovault ES mapping and outlines the uses of ``create_mapping``.

Overview
-----------------
Each item type defined in the application has its own indexed, which is named after the ``item_type`` attribute on the resource. Each index is initialized with a mapping and settings through ``create_mapping``, allowing us to customize how documents are stored and search with ES. A large part of this converting the item schemas in the application to ES mappings; this is covered in "The Mapping" section below. The index settings are equally as important and also defined in create_mapping.py. For example, they hold the ``analyzers`` which define how terms for each document are created in the underlying Lucene inverted index.

In addition to defining the mappings and settings used when an ES index is created, ``create_mapping`` has a main ``run`` function that is used to coordinate ES index operations. It takes care of things like queueing items for indexing when a new index is created, or determining when an index is already up-to-date and can be skipped. See the "Usage" section for morning

The Mapping
-----------------
The ``create_mapping_by_type`` function is used to create the full, unique mapping for any given item type. It does three things for each item type:

1. Creates the mapping for the ``@@embedded`` view of the item using ``type_mapping``.
2. Creates the mapping for the ``@@aggregated-items`` view of the item using  ``aggregated_items_mapping``.
3. Combines both 1. and 2. with ``es_mapping`` to return a full mapping with additional generic fields.

Embedded mapping
^^^^^^^^^^^^^^^^^
The embedded mapping corresponds to the fully expanded ``@@embedded`` view of an item, which contains the base item fields and all expanded fields dictated by the ``embedded_list`` of the item. This mapping is made precisely according to the item schemas, which is critical because the embedded view is used for all filtering and aggregating done when searching. Additionally, it is used for free text searching of the ``_all`` field by setting ``'include_in_all': True``.

NOTE: `_all field <https://www.elastic.co/guide/en/elasticsearch/reference/5.3/mapping-all-field.html>`_ is deprecated in Elasticsearch 6.

The embedded mapping is made in the ``type_mapping`` function, which recursively crawls through the schemas and embedded list of a given item type. A key function in the process is ``schema_mapping``, which is used to build the mapping for any field, whether it is an object or a terminal field within an object (e.g. text or date field). The resulting per-field mapping includes the ``raw`` and ``lower_case_sort`` keyword subfields within the mapping, which are used for filtering and sorting ES documents.

Aggregated Items Mappings
^^^^^^^^^^^^^^^^^
Item types may defined an ``aggregated_items`` attribute, which is a dictionary that is used to find certain fields within the embedded view of an item and pull them to the top level. Since these fields need be filtered just like fields in the embedded mapping, we define a ``aggregated_items_mapping`` function to build such a mapping given an item type. This function may look daunting, but it is straightforward in concept. First it builds a dictionary with mappings for the top level fields ``parent``, ``embedded_path``, and ``item``. Then it iterates through fields within the ``aggregated_items`` attribute of the resource and adds them to the ``item`` sub-mapping.

Combining the Mappings
^^^^^^^^^^^^^^^^^
After creating the embedded and aggregated items mappings, both of which are unique to a given item type, we finish the mapping using the ``es_mapping`` function. It serves as a generic template which the two other mappings are injected into. Below is a brief overview of its contents:

* **_all** configuration for free-text search. Sets the ``analyzer`` used for free-text search at indexing-time and the ``search_analyzer`` used at search-time.
* **dynamic_templates** create templates used to dynamically map some fields of sub-mappings created by this function, including ``unique_keys`` and ``links``. `Read more here <https://www.elastic.co/guide/en/elasticsearch/reference/5.3/dynamic-templates.html>`_.
* **properties** the actual properties of the mapping. ``embedded`` and ``aggregated_items`` are customized by item type, as described above, but there are many other fields as well.

The ``properties`` of the mapping include a number of important fields that are set in the ES documents. For more information, look at the ``@@index-data`` view, which is used to generate the document for individual items. See `indexing_views.py <https://github.com/4dn-dcic/snovault/blob/master/src/snovault/indexing_views.py>`_ for more info.

The Settings
-----------------
In addition to a mapping, each Elasticsearch index must be created with a settings configuration. We create these using the ``index_setting`` function, which works the same for each index. To reiterate: every index has the same settings. The settings can be categorized into two groups, explained below.

Index Configuration
^^^^^^^^^^^^^^^^^
There are a few top level index settings that we define, some of which use global variables for easier programmatic access. Here they are:

* **number_of_shards** set to ``NUM_SHARDS`` global variable. Shards are segments of the entire ES data. Maximum size of each shard should be ~30 GB, so this setting only needs to be increased for very large indices. Keep in mind that each shard has an overhead cost.
* **number_of_replicas** set to ``NUM_REPLICAS`` global variable. Replicas are copies of shards and used for redundancy and search performance.
* **max_result_window** set to ``SEARCH_MAX`` global variable. Controls the maximum depth of searches using ``from`` and ``size`` parameters. Used as a safeguard against searches taking too long or using too much heap memory.
* **mapping.total_fields.limit** total number of fields allowed for the mapping of an index. Used to prevent mapping explosions.
* **mapping.depth.limit** total number of levels deep a mapping can be for a given index. Used to prevent recursive mappings.

Analysis Configuration
^^^^^^^^^^^^^^^^^
We set a couple of custom analyzers and filters that are used for free-text indexing and searching the ``_all`` field. Here's an `overview <https://www.elastic.co/blog/found-text-analysis-part-1>`_ of analyzers in ES. Additionally, we set a normalizer to process some keyword fields. The configuration details are broken down below:

* **ngram_filter** `edgeNGram <https://www.elastic.co/guide/en/elasticsearch/reference/5.3/analysis-edgengram-tokenfilter.html>`_  filter used to break down tokens down into nGrams starting from the left side. ``MIN_NGRAM`` and ``MAX_NGRAM`` are used to control the size of the tokens created. This filter is used in the ``snovault_index_analyzer``.
* **truncate_to_ngram** `truncate <https://www.elastic.co/guide/en/elasticsearch/reference/5.3/analysis-truncate-tokenfilter.html>`_ filter used to truncate tokens to ``MAX_NGRAM`` size so that they will match tokens created by the ``ngram_filter``. This filter is used in ``snovault_search_analyzer``.
* **snovault_index_analyzer** analyzer used on indexing time for ``_all`` field, which means it creates keys in the Lucene inverted index used to find documents when using free-text search. It tokenizes on whitespace, strips HTML characters from tokens, and then applies the following filters: ``lowercase`` (`info <https://www.elastic.co/guide/en/elasticsearch/reference/5.3/analysis-lowercase-tokenfilter.html>`_), ``asciifolding`` (`info <https://www.elastic.co/guide/en/elasticsearch/reference/5.3/analysis-asciifolding-tokenfilter.html>`_), and ``ngram_filter``.
* **snovault_search_analyzer** analyzer used on searching time for ``_all`` field. Used to create tokens from the free-text query value, which are then searched for in the Lucene inverted index. Tokenizes on whitespace and then applies the ``lowercase``, ``asciifolding``, and ``truncate_to_ngram`` filters.
* **case_insensitive** `normalizer <https://www.elastic.co/guide/en/elasticsearch/reference/5.3/analysis-normalizers.html>`_ used to lowercase the ``lower_case_sort`` keyword mappings.

Usage
-----------------
``create_mapping.run`` (and by extension, ``create_mapping.main``) can be used to manage a number of things about the ES configuration. In general, the ``run`` function is responsible for creating ES indices for each item type and then queueing up the associated items for indexing. The options available for ``create_mapping.main`` and some examples are provided below.

NOTE: running buildout on Fourfront or CGAP creates a console script named ``bin/create-mapping`` that uses ``create_mapping.main``.

Command Line Options
^^^^^^^^^^^^^^^^^
As provided to the argument parser uses in ``create_mapping.main``.

* **--app-name** Pyramid application name, should usually be "app".
* **--item-type <value>** Item type of the index to run on. Item type should correspond to ``Resource.item_type`` and can be provided any number of times to specify multiple indices to run over. If not provided, run over all item types.
* **--dry-run** If set, bail before making any actual changes to the ES indices.
* **--check-first** If set, check existing indices and attempt to reuse them. If the settings and mapping for each existing index has not changed and all items are present, then skip re-creating that index. Used to save time by not deleting indices that are already properly configured.
* **--skip-indexing** If set, do not queue up any items for indexing when creating new indices. This can sometimes be useful when you want to change mappings without triggering indexing, but is mostly used in tests.
* **--index-diff** If set, *skip the index creation step* and attempt to queue any items for reindexing that are found in the DB but not ES for the given indices. This is a bit strange because it skips the mapping steps altogether, but leverages the second half of the typical process. Useful when something went wrong with indexing and you need to identify and fix items that did not get indexed.
* **--strict** If set, all indexing queued will be in strict mode. This means that indexed items will not cause validation. Useful when doing a total remapping. If ``create_mapping.run`` detects that all items are getting reindexed, then they will automatically be queued with ``strict: True``.
* **--sync-index** If set, indexing will occur synchronously within the same process and bypass the SQS queue. Does this by calling `es_index_data.py <https://github.com/4dn-dcic/snovault/blob/master/src/snovault/commands/es_index_data.py>`_. Use with care, since indexing can take a long time.
* **--print-count-only** If set, will print the ES counts for each index and exit before changing indices or queueing anything. If you also provide **--index-diff**, this argument will display the uuids that are missing from ES, which can be quite useful.
* **--purge-queue** Purge the contents of all SQS queues before changing the indices. This is useful when duplicate messages may get introduced to the queue. The queues are automatically purged if ``create_mapping.run`` detects that all items are getting reindexed.

In addition to the command line arguments, a list of item types/names called ``item_order`` can be manually passed to ``create_mapping.run``. This allows sorting of the indices when running through this function. Item types will be sorted by index within the ``item_order`` list, meaning the first item type in the list will run through the mapping progress first. All items not found in `` item_order`` are run alphabetically at the end.

Example Usage
^^^^^^^^^^^^^^^^^
Here are some useful ways to leverage ``create_mapping``. The code below assumes that you've run buildout on the corresponding portal and created a console script named ``create-mapping`` that points to ``create_mapping.main``.

Run on all item types without checking current indices::

    bin/create-mapping production.ini --app-name app

Run on two specific item types, first checking the indices to verify that they need be recreated. Skip indices that have up-to-date mapping, settings, and counts::

    bin/create-mapping production.ini --app-name app --item-type <type1> --item-type <type2> --check-first

Skip the index creation step. Identify items of given item type that are unindexed and index those in strict mode. Purge the indexing queues first::

    bin/create-mapping production.ini --app-name app --item-type <type1> --index-diff --purge-queue --strict
