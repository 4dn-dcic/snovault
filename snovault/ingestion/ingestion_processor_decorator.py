from ..types.ingestion import IngestionSubmission
from .exceptions import UndefinedIngestionProcessorType


_INGESTION_UPLOADERS = {}


def ingestion_processor(processor_type):
    """
    @ingestion_uploader(<ingestion-type-name>) is a decorator that declares the upload handler for an ingestion type.
    """

    # Make sure the ingestion type specified for the decorated function is supported by
    # our IngestionSubmission type; this info comes from schemas/ingestion_submission.json.
    if not IngestionSubmission.supports_type(processor_type):
        raise UndefinedIngestionProcessorType(processor_type)

    def ingestion_type_decorator(fn):
        if processor_type in _INGESTION_UPLOADERS:
            raise RuntimeError(f"Ingestion type {processor_type} is already defined.")
        _INGESTION_UPLOADERS[processor_type] = fn
        return fn

    return ingestion_type_decorator


def get_ingestion_processor(processor_type):
    handler = _INGESTION_UPLOADERS.get(processor_type, None)
    if not handler:
        raise UndefinedIngestionProcessorType(processor_type)
    return handler
