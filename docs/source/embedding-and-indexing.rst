Embedding and Indexing
=================================

EMBEDDING
^^^^^^^^^

The 4DN DCIC team has moved away from the "embed everything" mentality that was previously in snovault. In it's place, we've allowed a precise control of what gets embedded for each object type. These are the 'embedded lists' (embedded_list property) defined for each object in the corresponding /types/ file. To provide minimal embeds without forcing us to have to manually embed common fields (read: add to individual types files), default embedding was added. This will automatically provide @id, link_id, display_title, principals_allowed, and uuid fields for any linked object at the top level. Additionally, all subobjects defined in the schema are automatically fully embedded. Embedding a linkTo with a '*' at the end of the embed path will emulate this functionality for any object in the embedded list (this means that all linkTos within that object will also get the three fields mentioned above, as well as all top level fields). Otherwise, valid embeds are single fields or just the object (in which case link_id, display_title, and uuid) are added. Below are some examples to illustrate:

In the types file
-----------------
embedded_list = [
    lab.*,
    award.title,
    submitted_by
]

Since lab, award, and submitted by are all linkTo's, default embeds will be added for each of these (this code is housed in src/util.py).

Starting with 'lab.*', the '*' means we add all fields for the lab as well as link_id, display_title, and uuid to any linkTo's within the lab. Let's say that lab has the following fields: title, @id, principals_allowed, link_id, display_title, and uuid. It also has the following linkTo: pi. Effectively, 'lab.*' would expand to the following given the default embedding:

[
    lab.display_title,
    lab.link_id,
    lab.uuid,
    lab.@id,
    lab.principals_allowed.*,
    lab.title,
    lab.pi.display_title,
    lab.pi.link_id,
    lab.pi.uuid,
    lab.pi.@id,
    lab.pi.principals_allowed.*
]

As a word of caution, be careful of using `.*`, because it can drastically increase the scope of invalidation needed for a given item by adding many more items to the `linked_uuids` list. This will be covered in-depth later in this document and in the invalidation document (./invalidation.rst).

The next embed, 'award.title', has a terminal field. This means we are only interested in embedding the 'title' field for award, but we will automatically add @id, principals_allowed, link_id, display_title, and uuid to get some baseline information for the object. Thus, 'award.title' expands to:

[
    award.title,
    award.display_title,
    award.link_id,
    award.uuid,
    award.@id,
    award.principals_allowed.*
]

The last embed is 'submitted_by'. This will actually throw a warning from the tests written around embedded_list, since this embed is completely unnecessary. Since submitted_by is a top-level linkTo in our object, there is no reason to add it to the embedded_list; it will already get link_id, display_title, and uuid added. So let's change this embed to submitted_by.some_object (where some_object is a fictional linkTo). This would automatically add @id, principals_allowed, link_id, display_title, and uuid for some_object.

Since embedding directly affects the elasticsearch (ES) mapping and indexing, it is important to discuss the role of embedding in the mapping process. In create_mapping.py, the embedded_list for the object to be mapped is obtained and run through the embedding process. These embeds are then used to create a mapping that is fitted exactly to the data that we want to embed in our object.

Similarly, the embedded fields are passed into the embed.py code to trim the result to the specific fields we desire. A cache is used during embedding to speed up the embedding process (see embed_cache.py). It caches the calculated results for view of a given item, keyed by path. For example, <my-item>/@@embedded and <my-item>/@@object would be separate entries in the cache.

INDEXING
^^^^^^^^

As of spring 2018, the 4DN DCIC team diverged significantly from the indexing strategy previously used by ENCODE. Whereas the old system was based on creating database snapshots identified by the most recent transaction (xmin), the new system queues individual items for indexing and the /index endpoint triggers a process to pull them off and index them into ES. The queues currently used are from simple queue service (SQS), an AWS product. This provided us a performance increase as well as increased visibility into what was happening in the processes (which was previously a black box). The system is built of the following components:

- Hooks to add items to the queue after a POST or PATCH (crud_views.py and invalidation.py)
- Hooks to queue items after initial mapping/subsequent re-mappings (create_mapping.py)
- The manager class of the queues (indexer_queue.py)
- Indexer (used for non-parallel indexing; parent of MPIndexer; indexer.py)
- MPIndexer (mpindexer.py)
- Item view to build the content to be indexed for any item (indexing_views.py)
- Index listener which drives ongoing indexing and consumption of the queue (es_index_listener.py)

Currently, the anatomy of a item to be put on the queue is:
```
{
    'uuid': <str>,
    'sid': <int or None>,
    'strict': <boolean>,
    'timestamp': <str>,
    'method': 'POST' or 'PATCH' (not present on secondary items)
}
```

uuid and timestamp are pretty self-explanatory. The sid is the DB transaction count, which is used as the version number in ES. Because sid is incremented for each transaction in the DB, this allows us a convenient method for ES versioning. The strict parameter is a boolean that controls whether associated uuids are also reindexed for the given uuid. A false value causes the consumer to find and queue associated targets after the item is rendered; a true value is a full-render terminal job and does not fan out again. The method property shows whether an item was added to the queue through a POST or PATCH; if the item is on the secondary queue, this field will not be present, since the item was queued as a result of invalidation caused by a primary item.

There are currently three queues for each environment. The ``primary`` queue contains items posted or patched. The ``secondary`` queue contains terminal invalidation targets and is also used by create-mapping. A message whose sid is newer than the indexer's repeatable-read snapshot is resent to its current queue and the worker restarts with a fresh snapshot; there is no separate deferred queue. The dead letter queue, or ``dlq``, receives messages that exhaust the SQS redrive policy and holds them for operator replay.

The process of finding the secondary uuids that need to be indexed when a primary item is created or edited is called invalidation and has it's own document (./invalidation.rst). This process is complex and has been one of the largest pain points in creating and optimizing our indexing system. Please read that document for an in-depth overview of invalidation. From the indexing standpoint, all secondary items that are invalidated are indexed on the secondary queue so that they themselves do not cause a further cascading of more items on the secondary queue. Reverse links (rev_links) are taken into account during the invalidation process.

Secondary fan-out coalescing
----------------------------

``indexer.coalesce_secondary`` optionally coalesces duplicate invalidation fan-out
for the same target while retaining SQS as the transport.  The allowed rollout
values are ``off`` (the default), ``shadow`` (run the state machine and measure
suppression, but send every message), and ``on`` (suppress work already covered by
an outstanding message).  Primary POST/PATCH messages, bulk reindex messages, and
manual ``/queue_indexing`` requests do not use this state.

Coalescing happens in ``Indexer.find_and_queue_secondary_items`` only after the ES
linker lookup, invalidation-scope filtering, and new reverse-link augmentation have
selected the final target UUIDs.  A secondary fan-out message contains no field
diff: the diff has already served its purpose by deciding target membership, and
the consumer renders the selected target's complete index document.  The table
``secondary_indexing_pending`` therefore needs only ``(rid, namespace)``,
``pending``, ``queued_sid``, and ``queued_at``.  Namespace is the sanitized queue
environment name and is part of the primary key because blue and green indexers fan
out independently to their own secondary queues.

The producer arms sorted UUID batches in a short READ COMMITTED write transaction,
commits, and only then sends marker messages to SQS.  Inserts plus unconditional
``FOR UPDATE`` locking serialize concurrent producers and consumer claims.  A
consumer claims a marker before rendering.  If the indexer's repeatable-read
snapshot has a maximum sid below the row's latest ``queued_sid``, the claim rolls
back and the existing SQS defer/resend path obtains a fresh snapshot.  Otherwise
the claim releases ``pending`` before the full render.  SQS deletion remains after
a successful ES write; a crash or redelivery after release causes a harmless full
render with a no-op claim.

A failed send after the state commit leaves a recoverable pending row.  The index
listener periodically re-arms old rows and commits before resending marker messages.
If the state table itself is unavailable, the producer fails open by sending the
legacy unmarked secondary payload; if both PostgreSQL state and that SQS send fail,
the error propagates so the causing primary message is not deleted and can retry.
Defaults are 1,800 seconds before repair, a 300 second sweep interval, and 500 rows
per sweep; configure these with
``indexer.coalesce_secondary.stale_seconds``,
``indexer.coalesce_secondary.sweep_interval``, and
``indexer.coalesce_secondary.sweep_limit``.  The sweeper uses the partial
``(namespace, queued_at) WHERE pending`` index and ``FOR UPDATE SKIP LOCKED`` so
multiple listeners or operator actions do not wait on hot rows.

Operations and rollout
~~~~~~~~~~~~~~~~~~~~~~

* ``GET /secondary_coalescing_status`` returns aggregate pressure for the current
  namespace, or per-target state for every namespace when passed ``?uuid=...``.
  During ``shadow`` and ``on``, the same information is also included in the
  existing ``/indexing_status`` and ``/indexing-info`` responses. ``off`` leaves
  those existing response contracts unchanged.
* ``POST /queue_indexing`` remains a force-queue/bypass interface and never changes
  pending state.
* ``POST /reset_secondary_coalescing`` requires ``index`` permission and accepts
  exactly one of ``{"uuids": [...]}`` or ``{"all": true}``.  It is dry-run by
  default, is capped at 1,000 rows, and is audit-logged with the authenticated
  principal.  ``requeue=true`` keeps rows pending, commits a fresh ``queued_at``,
  and then sends; a failed administrative send is therefore still sweepable.
* Treat rollout as one-way after the state table is deployed: use a dry run
  followed by a namespace reset before moving from ``off`` or ``shadow`` to
  ``on``, then keep coalescing enabled.  If rollback is required, roll back the
  database and the associated application version together rather than changing
  the runtime mode independently.

Structured logs use ``coalescing_event`` to expose fan-out targets/suppression,
database and SQS enqueue failures, claims and stale deferrals, sweeper repairs,
administrative actions, and operation latency.  SQS receive logs include
``ApproximateReceiveCount``, ``SentTimestamp``-derived queue latency, message
origin, and claim outcome, distinguishing producer duplication from redelivery.
Queue deletion failures are logged and remain safe because the message is deleted
only after successful indexing.

PostgreSQL deployment and capacity
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The table is registered in ``snovault.storage.Base`` and is created by the normal
``Base.metadata.create_all`` path when ``create_tables=true``.  Deployments that
disable automatic table creation must apply the SQLAlchemy-generated table, index,
and storage-parameter DDL before enabling ``shadow``; no backfill is required.
The table uses fillfactor 70 and aggressive per-table autovacuum scale factors
(0.02 vacuum, 0.05 analyze).  ``queued_sid`` is deliberately kept out of every
index (keys and INCLUDE lists alike; INCLUDE columns also block HOT), so
suppression updates that only merge a newer sid into an already-pending row are
HOT-eligible; pending transitions and sweeper re-arms necessarily touch the
partial index.  Resource deletion cascades to state rows, and inactive rows are
bounded by resources multiplied by active environment namespaces.

Each producer batch or consumer claim checks out one additional connection only
for its short write transaction; it does not hold a connection across an SQS API
call or an ES render.  Before enabling ``shadow``, operators must verify database
pool and PostgreSQL ``max_connections`` headroom for the maximum concurrent indexer
workers, then measure actual table/index growth, WAL, dead tuples/autovacuum cadence,
batch transaction latency, sweep lag, and suppression ratio.  The implementation
batches at 500 targets, uses primary-key point locks, and uses a partial index
for sweeps; these measurements, rather than an assumed production item or
edit count, determine capacity.

A couple endpoints were added to make the queue more useful. First, /indexing_status takes a GET request and returns waiting and in-flight counts for the three queues. The /queue_indexing endpoint is a POST endpoint used to manually queue items. It requires administrator privileges and takes a JSON body where you can either specify a list of `collections` (e.g. file_fastq or biosample) or a list of `uuids` for indexing. You can also specify whether the items should be indexed in strict mode using the `strict` keyword and a boolean value. Lastly, you can specify which queue you want to send your items to using the `target_queue` keyword and a value of `primary`, `secondary`, or `dlq`. The default strict value is False and the default target is primary.

The queue is only actively cleared when create-mapping is run for a total reindex. This is because past records should not be lost for the alternate create-mapping functions, such as --check-first or --index-diff. The queue can be managed directly from the AWS console.

Please note that you must have the correct AWS credentials configured for your project to use it.

Further possible improvements to the queue system include:
- Evolving the single-slot PostgreSQL state into a full durable work/outbox table while retaining SQS as a doorbell during migration.
- Change the /index endpoint to only pull and index one item per transaction, which would eliminate need for the deferred queue.
- Ordering of items on the queue, either at time of create_mapping (initial indexing) or for ongoing indexing.
- Speed up indexing by refining what items are considered invalid when an item is indexed (in most cases, secondary items do not need to be reindexed).

Other related improvements:
- Use closure tables in the embedding process to make the /index-data view much faster.
