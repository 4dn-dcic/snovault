from dcicutils.project_utils import C4ProjectRegistry, C4Project
from .project.access_key import SnovaultProjectAccessKey
from .project.authentication import SnovaultProjectAuthentication
from .project.authorization import SnovaultProjectAuthorization
from .project.ingestion import SnovaultProjectIngestion
from .project.loadxl import SnovaultProjectLoadxl
from .project.renderers import SnovaultProjectRenderers
from .project.schema_views import SnovaultProjectSchemaViews


@C4ProjectRegistry.register("dcicsnovault")
class SnovaultProject(SnovaultProjectAccessKey,
                      SnovaultProjectAuthentication,
                      SnovaultProjectAuthorization,
                      SnovaultProjectIngestion,
                      SnovaultProjectLoadxl,
                      SnovaultProjectRenderers,
                      SnovaultProjectSchemaViews,
                      C4Project):
    NAMES = {"NAME": "snovault", "PYPI_NAME": "dcicsnovault"}
    ACCESSION_PREFIX = "SNO"
