from dcicutils.project_utils import C4ProjectRegistry, C4Project

@C4ProjectRegistry.register('dcicsnovault')
class SnovaultProject(C4Project):
    NAMES = {'NAME': 'snovault', 'PYPI_NAME': 'dcicsnovault'}
    ACCESSION_PREFIX = 'SNO'


app_project = C4ProjectRegistry.app_project_maker()
