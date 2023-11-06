from ..types.ingestion import SubmissionFolio
from .ingestion_processor_decorator import ingestion_processor


@ingestion_processor('data_bundle')
def handle_data_bundle(submission: SubmissionFolio):

    # We originally called it 'data_bundle' and we retained that as OK in the schema
    # to not upset anyone testing with the old name, but this is not the name to use
    # any more, so reject new submissions of this kind. -kmp 27-Aug-2020

    with submission.processing_context():

        raise RuntimeError("handle_data_bundle was called (for ingestion_type=%s). This is always an error."
                           " The ingestion_type 'data_bundle' was renamed to 'metadata_bundle'"
                           " prior to the initial release. Your submission program probably needs to be updated."
                           % submission.ingestion_type)
