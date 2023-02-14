import os
import argparse

from dcicutils.misc_utils import PRINT
from dcicutils.command_utils import script_catch_errors


REPO_NAME = os.path.basename(os.path.abspath(os.curdir))


def check_local_creds(appname):

    if not appname:
        if 'cgap' in REPO_NAME:
            appname = 'cgap'
        elif 'ff' in REPO_NAME or 'ffourfront' in REPO_NAME:
            appname = 'fourfront'
        else:
            PRINT("Can't figure out if this is cgap or fourfront.")
            exit(1)

    class WarningMaker:

        count = 0

        @classmethod
        def warn(cls, *args, **kwargs):
            cls.count += 1
            return PRINT(*args, **kwargs)

    warn = WarningMaker.warn

    if appname not in ['fourfront', 'ff', 'cgap']:
        raise RuntimeError(f"Unknown appname {appname!r}. Expected 'cgap' or 'fourfront' (or 'ff').")
            
    global_env_bucket = os.environ.get('GLOBAL_ENV_BUCKET')
    cgap_env_bucket = 'cgap-devtest-main-foursight-envs'
    fourfront_env_bucket = 'foursight-prod-envs'

    def check_global_env_bucket(var, val):
        if appname == 'cgap' and val != cgap_env_bucket:
            warn(f"{var} is {val!r}, but should be {cgap_env_bucket}.")
        elif appname == 'fourfront' and val != fourfront_env_bucket:
            warn(f"{var} is {val!r}, but should be {fourfront_env_bucket}.")

    check_global_env_bucket('GLOBAL_ENV_BUCKET', global_env_bucket)
    global_bucket_env = os.environ.get('GLOBAL_BUCKET_ENV')
    if global_bucket_env:
        if global_bucket_env == global_env_bucket:
            warn("GLOBAL_BUCKET_ENV is the same as GLOBAL_ENV_BUCKET,"
                 " but you can just get rid of GLOBAL_BUCKET_ENV now.")
        elif not global_env_bucket:
            warn("You need to set GLOBAL_ENV_BUCKET, not GLOBAL_BUCKET_ENV.")
            check_global_env_bucket('GLOBAL_BUCKET_ENV', global_bucket_env)
    for var in ['CHECK_RUNNER', 'ACCOUNT_NUMBER', 'ENV_NAME']:
        if os.environ.get(var):
            warn(f"The variable {var} has a non-null value but should be unset.")
    for var in ['Auth0Client', 'Auth0Secret']:
        if not os.environ.get(var):
            warn(f"The variable {var} has no value but should be set.")
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
