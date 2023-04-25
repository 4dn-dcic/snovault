import os
import toml

from pkg_resources import resource_filename


# This isn't the home of snovault, but the home of the snovault-based application.
# So in CGAP, for example, this would want to be the home of the CGAP application.
# If not set, it will be assumed that the current working directory is that.
APPLICATION_PROJECT_HOME = os.environ.get("APPLICATION_PROJECT_HOME", os.path.abspath(os.curdir))
PYPROJECT_TOML_FILE = os.path.join(APPLICATION_PROJECT_HOME, "pyproject.toml")
PYPROJECT_TOML = toml.load(PYPROJECT_TOML_FILE)
POETRY_DATA = PYPROJECT_TOML['tool']['poetry']

PROJECT_NAME = POETRY_DATA['name'].replace('dcic', '')

print("=" * 80)
print(f"APPLICATION_PROJECT_HOME={APPLICATION_PROJECT_HOME}")
print(f"PYPROJECT_TOML_FILE={PYPROJECT_TOML_FILE}")
print(f"PROJECT_NAME={PROJECT_NAME}")
print("=" * 80)


def project_filename(filename):
    # TODO: In fact we should do this based on the working dir so that when this is imported to another repo,
    #       it gets the inserts out of that repo's tests, not our own.
    return resource_filename(PROJECT_NAME, filename)


