from dcicutils.project_utils import ProjectRegistry, C4Project

@ProjectRegistry.register('dcicsnovault')
class SnovaultProject(C4Project):
    NAME = 'snovault'
    PYPI_NAME = 'dcicsnovault'
    ACCESSION_PREFIX = 'SNO'


app_project = SnovaultProject.app_project_maker()
