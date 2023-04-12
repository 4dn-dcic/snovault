import toml


PYPROJECT_TOML = toml.load("pyproject.toml")
POETRY_DATA = PYPROJECT_TOML['tool']['poetry']

PROJECT_NAME = POETRY_DATA['name'].replace('dcic', '')

print("=" * 80)
print(f"PROJECT_NAME={PROJECT_NAME}")
print("=" * 80)


def project_filename(filename):
    # TODO: In fact we should do this based on the working dir so that when this is imported to another repo,
    #       it gets the inserts out of that repo's tests, not our own.
    return resource_filename(PROJECT_NAME, filename)


