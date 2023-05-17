from .project import ProjectRegistry, Project
from .authentication import SnovaultProjectAuthentication

@ProjectRegistry.register('dcicsnovault')
class SnovaultProject(Project, SnovaultProjectAuthentication):
    NAME = 'dcicsnovault'
    ACCESSION_PREFIX = 'SNO'
