# Ingestion related functions which may be overriden by an implementing app,
# e.g. Foursight or CGAP portal, using the dcicutils project_utils mechanism.

class SnovaultProjectIngestion:
    def note_ingestion_enqueue_uuids_for_request(self, ingestion_type, request, uuids):
        pass
