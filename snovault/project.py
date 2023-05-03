import os
import toml

from pkg_resources import resource_filename
from dcicutils.env_utils import EnvUtils, EnvBase

if not EnvBase.global_env_bucket_name():
    raise RuntimeError("No GLOBAL_ENV_BUCKET has been set.")

EnvUtils.init()

# This isn't the home of snovault, but the home of the snovault-based application.
# So in CGAP, for example, this would want to be the home of the CGAP application.
# If not set, it will be assumed that the current working directory is that.
APPLICATION_PROJECT_HOME = os.environ.get("APPLICATION_PROJECT_HOME", os.path.abspath(os.curdir))
PYPROJECT_TOML_FILE = os.path.join(APPLICATION_PROJECT_HOME, "pyproject.toml")
PYPROJECT_TOML = toml.load(PYPROJECT_TOML_FILE)
POETRY_DATA = PYPROJECT_TOML['tool']['poetry']
POETRY_NAME = POETRY_DATA['name']

PROJECT_NAME = POETRY_NAME.replace('dcic', '')
PROJECT_APP_NAME = EnvUtils.app_name() if PROJECT_NAME == 'encoded' else PROJECT_NAME
PROJECT_PRETTY_NAME = (PROJECT_APP_NAME.title()
                       .replace("Cgap", "CGAP").replace("Smaht", "SMaHT")
                       .replace("-Portal", "").replace("-", " "))
PROJECT_PRETTY_PORTAL_NAME = f"{PROJECT_PRETTY_NAME} Portal"
PROJECT_ACCESSION_PREFIXES = {
    'cgap': 'GAP',
    'fourfront': '4DN',
    'snovault': 'SNO',
    'encoded-core': 'COR',
}
PROJECT_ACCESSION_PREFIX = None
for key, val in PROJECT_ACCESSION_PREFIXES.items():
    if key in APPLICATION_PROJECT_HOME:
        PROJECT_ACCESSION_PREFIX = val
        break
if not PROJECT_ACCESSION_PREFIX:
    raise Exception(f"Don't know a proper accession prefix for {PROJECT_APP_NAME}.")

print("=" * 80)
print(f"APPLICATION_PROJECT_HOME={APPLICATION_PROJECT_HOME}")
print(f"PYPROJECT_TOML_FILE={PYPROJECT_TOML_FILE}")
print(f"PROJECT_NAME={PROJECT_NAME}")
print(f"PROJECT_APP_NAME={PROJECT_APP_NAME}")
print(f"PROJECT_ACCESSION_PREFIX={PROJECT_ACCESSION_PREFIX}")
print("=" * 80)


def project_filename(filename):
    # TODO: In fact we should do this based on the working dir so that when this is imported to another repo,
    #       it gets the inserts out of that repo's tests, not our own.
    return resource_filename(PROJECT_NAME, filename)


