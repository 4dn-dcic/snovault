# Renderers related functions which may be overriden by an implementing app,
# e.g. Foursight or CGAP portal, using the dcicutils project_utils mechanism.

from ..mime_types import MIME_TYPE_HTML, MIME_TYPE_JSON, MIME_TYPE_LD_JSON


class SnovaultProjectRenderers:
    def renderers_mime_types_supported(self):
        return [MIME_TYPE_JSON, MIME_TYPE_HTML, MIME_TYPE_LD_JSON]
