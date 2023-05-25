# Ingestion related functions which may be overriden by an implementing app,
# e.g. Foursight or CGAP portal, using the dcicutils project_utils mechanism.

class SnovaultProjectIngestion:
    def ingestion_submission_schema_file(self):
        return "snovault:schemas/ingestion_submission.json"
