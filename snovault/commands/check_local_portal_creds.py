import os
import argparse

from dcicutils.lang_utils import disjoined_list
from dcicutils.misc_utils import PRINT
from dcicutils.command_utils import script_catch_errors
from dcicutils.common import APP_CGAP, APP_FOURFRONT


REPO_NAME = os.path.basename(os.path.abspath(os.curdir))

PSEUDO_APP_SNOVAULT = 'snovault'

CGAP_ENV_BUCKET = 'cgap-devtest-main-foursight-envs',
FOURFRONT_ENV_BUCKET = 'foursight-prod-envs'

APP_NAMES = [APP_CGAP, APP_FOURFRONT]

APP_BUCKET_MAPPINGS = {
    APP_CGAP: CGAP_ENV_BUCKET,
    APP_FOURFRONT: FOURFRONT_ENV_BUCKET,
    PSEUDO_APP_SNOVAULT: FOURFRONT_ENV_BUCKET,  # doesn't have a bucket of its own
}

AWS_PORTAL_CREDENTIALS_NEEDED = {
    # These are Needed for IAM credentials
    'AWS_ACCESS_KEY_ID': True,
    'AWS_SECRET_ACCESS_KEY': True,   # Needed for IAM credentials
    # These are needed only for federated temporary credentials, which we disallow in local portal deployments
    # bevcause there's too much risk they are holdovers of more powerful credentials than we want, even if temporary.
    'AWS_SESSION_TOKEN': False,
}

UNWANTED_PORTAL_ENV_VARS = ['CHECK_RUNNER', 'ACCOUNT_NUMBER', 'ENV_NAME']
NEEDED_PORTAL_ENV_VARS = ['Auth0Client', 'Auth0Secret']


def check_local_creds(appname):

    if not appname:
        for appname_key, env_bucket in APP_BUCKET_MAPPINGS.items():
            if appname_key in REPO_NAME:
                appname = appname_key
                qualifier = "App" if appname in APP_NAMES else "Pseudo-app"
                PRINT(f"No --appname given. {qualifier} {appname}, with global env bucket {env_bucket},"
                      f" is being assumed.")
                break
        else:
            PRINT(f"No --appname given and can't figure out"
                  f" if repo {REPO_NAME} is {disjoined_list(APP_BUCKET_MAPPINGS)}.")
            exit(1)

    class WarningMaker:

        count = 0

        @classmethod
        def warn(cls, *args, **kwargs):
            cls.count += 1
            return PRINT(*args, **kwargs)

    warn = WarningMaker.warn

    if appname not in APP_BUCKET_MAPPINGS:
        raise RuntimeError(f"Unknown appname {appname!r}. Expected {disjoined_list(APP_BUCKET_MAPPINGS)}.")

    global_env_bucket = os.environ.get('GLOBAL_ENV_BUCKET')

    def check_global_env_bucket(var, val):
        expected_val = APP_BUCKET_MAPPINGS.get(appname)
        if expected_val and val != expected_val:
            warn(f"{var} is {val!r}, but should be {expected_val!r}.")

    def needs_value(var):
        warn(f"The variable {var} has no value but should be set.")

    def has_unwanted_value(var):
        warn(f"The variable {var} has a non-null value but should be unset.")

    check_global_env_bucket('GLOBAL_ENV_BUCKET', global_env_bucket)
    global_bucket_env = os.environ.get('GLOBAL_BUCKET_ENV')
    if global_bucket_env:
        if global_bucket_env == global_env_bucket:
            warn("GLOBAL_BUCKET_ENV is the same as GLOBAL_ENV_BUCKET,"
                 " but you can just get rid of GLOBAL_BUCKET_ENV now.")
        elif not global_env_bucket:
            warn("You need to set GLOBAL_ENV_BUCKET, not GLOBAL_BUCKET_ENV.")
            check_global_env_bucket('GLOBAL_BUCKET_ENV', global_bucket_env)

    for aws_var, expected in AWS_PORTAL_CREDENTIALS_NEEDED.items():
        value = os.environ.get(aws_var)
        if expected and not value:
            needs_value(aws_var)
        elif not expected and value:
            has_unwanted_value(aws_var)

    for var in NEEDED_PORTAL_ENV_VARS:
        if not os.environ.get(var):
            needs_value(var)
    for var in UNWANTED_PORTAL_ENV_VARS:
        if os.environ.get(var):
            has_unwanted_value(var)

    if WarningMaker.count == 0:
        PRINT("Things look good.")


def main():
    parser = argparse.ArgumentParser(
        description='Echos version information from ~/.cgap-keys.json or override file.')
    parser.add_argument('--appname', help='Name of app to check for (cgap or ff/fourfront)', type=str, default=None)
    args = parser.parse_args()

    appname = args.appname

    with script_catch_errors():
        check_local_creds(appname=appname)


if __name__ == '__main__':
    main()
