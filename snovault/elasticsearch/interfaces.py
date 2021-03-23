from zope.interface import Interface

# Registry tool id
APP_FACTORY = 'app_factory'
ELASTIC_SEARCH = 'elasticsearch'
INDEXER = 'indexer'
INDEXER_QUEUE = 'indexer_queue'
INDEXER_QUEUE_MIRROR = 'indexer_queue_mirror'
INVALIDATION_SCOPE_ENABLED = 'invalidation_scope.enabled'


class ICachedItem(Interface):
    """
    Marker for cached Item
    """
