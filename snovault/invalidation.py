from .elasticsearch.interfaces import INDEXER_QUEUE, INDEXER_QUEUE_MIRROR, ELASTIC_SEARCH
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
    edit_or_add is a string with value 'edit' or 'add'.
    Queue item with strict=False so that secondary items and new rev links
    are also indexed
    """
    error_msg = None
    # only queue if the transaction is successful and we do no explicitly skip indexing (loadxl phase 1)
    if success and not request.params.get('skip_indexing'):
        try:
            item['strict'] = False
            item['method'] = 'POST' if edit_or_add == 'add' else 'PATCH'
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
                es = request.registry.get(ELASTIC_SEARCH)
                if es:
                    raise Exception(f'Indexer queue not configured!'
                                    f' Attempted to queue {item} for method {edit_or_add}.')
        except Exception as e:
            error_msg = repr(e)
    else:
        if request.params.get('skip_indexing'):
            log.info(f'skip_indexing param passed - {item} not queued for method {edit_or_add}')
        else:
            error_msg = f'DB transaction not successful! {item} not queued for method {edit_or_add}.'
    if error_msg:
        log.error(f'___Error queueing {item} for indexing. Error: {error_msg}')
