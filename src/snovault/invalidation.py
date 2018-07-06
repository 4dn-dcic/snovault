from .elasticsearch.interfaces import INDEXER_QUEUE, INDEXER_QUEUE_MIRROR
import structlog
import datetime

log = structlog.getLogger(__name__)

def includeme(config):
    config.scan(__name__)


def add_to_indexing_queue(success, request, item, edit_or_add):
    """
    Add item to queue for indexing. This function should be called from
    addAfterCommitHook.
    item arg is a dict: {'uuid': <item uuid>, 'sid': <item sid>}
    See item_edit and collection_add in .crud_view.py.
    edit_or_add is a string with value 'edit' or 'add'. If 'add', the item
    will be queued with strict indexing (no secondary items indexed).
    Otherwise, secondary items will also be queued.
    """
    error_msg = None
    if success:  # only queue if the transaction is successful
        try:
            # use strict mode if the item was added
            item['strict'] = edit_or_add == 'add'
            # add the type of request used to make the uuid
            item['method'] = 'POST' if item['strict'] else 'PATCH'
            item['timestamp'] = datetime.datetime.utcnow().isoformat()
            indexer_queue = request.registry.get(INDEXER_QUEUE)
            indexer_queue_mirror = request.registry.get(INDEXER_QUEUE_MIRROR)
            if indexer_queue:
                # send to primary queue
                indexer_queue.send_messages([item], target_queue='primary')
                if indexer_queue_mirror:
                    indexer_queue_mirror.send_messages([item], target_queue='primary')
            else:
                # if the indexer queue is not configured but ES is, log an error
                from .elasticsearch.interfaces import ELASTIC_SEARCH
                es = request.registry.get(ELASTIC_SEARCH)
                if es:
                    raise Exception("Indexer queue not configured! Attempted to queue %s for method %s." % (str(item), edit_or_add))
        except Exception as e:
            error_msg = repr(e)
    else:
        error_msg = 'Transaction not successful! %s not queued for method %s.' % (str(item), edit_or_add)
    if error_msg:
        log.error('___Error queueing %s for indexing. Error: %s' % (str(item), error_msg))
