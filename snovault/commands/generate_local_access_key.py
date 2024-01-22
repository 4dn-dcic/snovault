# --------------------------------------------------------------------------------------------------
# Script to manually generate a new portal access-key for local (localhost) development purposes.
# --------------------------------------------------------------------------------------------------
# This is really only needed until smaht-portal has been fleshed out enough to be able to do this
# normally using the UI; as of August 2023 there no way to do this. So we generate a new access-key,
# and associated secret, and either insert it directly into your locally running portal database,
# or output JSON suitable for doing this via master-inserts/access_key.json; and either update
# your access-keys file (~/.smaht-keys.json) directly, or output JSON suitable for this file.
#
# The --user arguments is used to specify the user with which the new access-key will be associated.
# This may be either an explicit UUID or your choce, or an email address which must be present in
# the master-inserts/user.json file; this is (only) required if --update-database is specified.
#
# With no other arguments this script outputs JSON objects suitable for inserting into the
# database (via master-inserts/access_key.json), and for placing in your ~/.smaht-keys.json file.
#
# If the --update-database option is given, then the new access-key will automatically be written
# to your locally running instance of the portal, which (obviously) needs to be up/running.
#
# If the --update-keys option is given, then your ~/.smaht-keys.json file will be automatically
# updated with the new access-key (for the "smaht-local" property).
#
# The --update option may be used to specify both --update-database and --update-keys.
#
# This is intended to be run from the smaht-portal repo; there is a generate-local-access-key
# poetry script defined there to invoke this module. And just for convenience, this is also
# supported in the cgap-portal and fourfront repos; to use it from from cgap-portal or
# fourfront use the --app cgap or --app fourfront arguments, respectively.
#
# Example command:
#  generate-local-access-key
#
# Example output:
#   Creating a new local portal access-key ... Done.
#   New local portal access-key record suitable for: /Users/dmichaels/.smaht-keys.json:
#   {
#       "key": "DHU74PRA",
#       "secret": "fptkcxqdqqpbenin",
#       "server": "http://localhost:8000"
#   }
#   New local portal access-key insert record suitable for: tests/data/master-inserts/access_key.json:
#   {
#       "status": "current",
#       "user": "<your-user-uuid>",
#       "description": "Manually generated local access-key for testing.",
#       "access_key_id": "DHU74PRA",
#       "secret_access_key_hash": "SampleHash_pcHx3KtfzvHCXTpcINCm0LRvzORXBpviBy6SSJyMssmcKrsmcKrzs",
#       "uuid": "c04987b2-42e6-47fc-8247-9de7e417355b"
#   }
#
# Example command:
#   generate-local-access-key --user david_michaels@hms.harvard.edu --update
#
# Example output:
#   Creating a new local portal access-key ... Done.
#   Writing new local portal access-key to: /Users/dmichaels/.smaht-keys.json ... Done.
#   Writing new local portal access-key to locally running portal database ... Done.
# --------------------------------------------------------------------------------------------------

import argparse
from collections import namedtuple
import configparser
from datetime import datetime
import dateutil.tz
import io
import json
import os
from passlib.context import CryptContext
from passlib.registry import register_crypt_handler
import pytz
import requests
from typing import Optional, Tuple
import uuid
from dcicutils.common import AnyJsonData
from dcicutils.portal_utils import Portal
from snovault.authentication import (
    generate_password as generate_access_key_secret,
    generate_user as generate_access_key
)
from snovault.edw_hash import EDWHash
from snovault.loadxl import load_all
from ..project_app import app_project
from .captured_output import captured_output

_INSERTS_DIR = "src/encoded/tests/data/master-inserts"
_USER_INSERTS_FILE = f"{_INSERTS_DIR}/user.json"
_DEFAULT_INI_FILE = "development.ini"


def main() -> None:

    parser = argparse.ArgumentParser(description="Create local portal access-key for dev/testing purposes.")
    parser.add_argument("--user", required=False,
                        help=f"User email for which the access-key should be defined (in master-inserts/user.json); or a UUID.")
    parser.add_argument("--update", action="store_true", required=False, default=False,
                        help=f"Same as --update-database and --update-keys both.")
    parser.add_argument("--update-database", action="store_true", required=False, default=False,
                        help=f"Updates the database of your locally running portal with the new access-key.")
    parser.add_argument("--update-keys", action="store_true", required=False, default=False,
                        help=f"Updates your access-keys file (e.g. ~/.smaht-keys.json) with the new access-key (e.g. smaht-local).")
    parser.add_argument("--port", type=int, required=False, default=8000,
                        help="Port for localhost on which your local portal is running.")
    parser.add_argument("--ini", type=str, required=False, default=_DEFAULT_INI_FILE,
                        help=f"Name of the application .ini file; default is: {_DEFAULT_INI_FILE}")
    parser.add_argument("--app", choices=["smaht", "cgap", "fourfront"], required=False, default=_guess_default_app(),
                        help="App name for which the access-key should be generated; default is smaht.")
    parser.add_argument("--list", action="store_true", required=False, default=False, help="Just list access-keys.")
    parser.add_argument("--verbose", action="store_true", required=False, default=False, help="Verbose output.")
    parser.add_argument("--debug", action="store_true", required=False, default=False, help="Debugging output.")
    args = parser.parse_args()

    _ACCESS_KEYS_FILE = os.path.expanduser(f"~/.{args.app}-keys.json")
    _ACCESS_KEYS_FILE_ITEM = f"{args.app}-local"

    if args.update:
        args.update_database = True
        args.update_keys = True

    if args.update_database or args.list:
        # Just FYI an absolute minimal .ini file looks like this:
        # [app:app]
        # use = egg:encoded
        # sqlalchemy.url = postgresql://postgres@localhost:5441/postgres?host=/tmp/snovault/pgdata
        # multiauth.groupfinder = encoded.authorization.smaht_groupfinder
        # multiauth.policies = auth0 session remoteuser accesskey
        # multiauth.policy.session.namespace = mailto
        # multiauth.policy.session.use = encoded.authentication.NamespacedAuthenticationPolicy
        # multiauth.policy.session.base = pyramid.authentication.SessionAuthenticationPolicy
        # multiauth.policy.remoteuser.namespace = remoteuser
        # multiauth.policy.remoteuser.use = encoded.authentication.NamespacedAuthenticationPolicy
        # multiauth.policy.remoteuser.base = pyramid.authentication.RemoteUserAuthenticationPolicy
        # multiauth.policy.accesskey.namespace = accesskey
        # multiauth.policy.accesskey.use = encoded.authentication.NamespacedAuthenticationPolicy
        # multiauth.policy.accesskey.base = encoded.authentication.BasicAuthAuthenticationPolicy
        # multiauth.policy.accesskey.check = encoded.authentication.basic_auth_check
        # multiauth.policy.auth0.use = encoded.authentication.NamespacedAuthenticationPolicy
        # multiauth.policy.auth0.namespace = auth0
        # multiauth.policy.auth0.base = encoded.authentication.Auth0AuthenticationPolicy
        with captured_output(not args.debug):
            portal = Portal(args.ini)
    else:
        portal = None

    if args.list:
        _print_all_access_keys(portal, args.verbose)
        return

    print(f"Creating a new local portal access-key for {args.app} ... ", end="")
    access_key_user_uuid = _generate_user_uuid(args.user, portal)
    access_key_id, access_key_secret, access_key_secret_hash = _generate_access_key(args.ini)
    access_key_inserts_file_item = _generate_access_key_inserts_item(access_key_id, access_key_secret_hash, access_key_user_uuid)
    access_keys_file_item = _generate_access_keys_file_item(access_key_id, access_key_secret, args.port)
    print("Done.")

    if args.update_keys:
        print(f"Writing new local portal access-key to: {_ACCESS_KEYS_FILE} ... ", end="")
        access_keys_file_json = {}
        try:
            with io.open(_ACCESS_KEYS_FILE, "r") as access_keys_file_f:
                access_keys_file_json = json.load(access_keys_file_f)
        except Exception:
            pass
        access_keys_file_json[_ACCESS_KEYS_FILE_ITEM] = access_keys_file_item
        with io.open(_ACCESS_KEYS_FILE, "w") as access_keys_file_f:
            json.dump(access_keys_file_json, fp=access_keys_file_f, indent=4)
        print("Done.")
    if not args.update_keys or args.verbose:
        print(f"New local portal access-key record suitable for: {_ACCESS_KEYS_FILE} ...")
        print(json.dumps(access_keys_file_item, indent=4))

    if args.update_database:
        if not _is_local_portal_running(args.port):
            _exit_without_action(f"Portal must be running locally ({_get_local_portal_url(args.port)}) to do an insert.")
        print(f"Writing new local portal access-key to locally running portal database ... ", end="")
        with captured_output(not args.debug):
            _load_data(portal, access_key_inserts_file_item, data_type="access_key")
        print("Done.")
    if not args.update_database or args.verbose:
        print(f"New local portal access-key insert record suitable for: {_INSERTS_DIR}/access_key.json ...")
        print(json.dumps(access_key_inserts_file_item, indent=4))


def _generate_user_uuid(user: Optional[str], portal: Optional[Portal] = None) -> str:
    if not user:
        if portal:
            _exit_without_action(f"The --user option must specify a UUID or email in {_USER_INSERTS_FILE}")
        return "<your-user-uuid>"
    user_uuid = None
    if _is_uuid(user):
        user_uuid = user
    else:
        with io.open(_USER_INSERTS_FILE, "r") as user_inserts_f:
            user_uuid_from_inserts = [item for item in json.load(user_inserts_f) if item.get("email") == user]
            if not user_uuid_from_inserts:
                _exit_without_action(f"The given user ({user}) was not found as an email"
                                     f" in: {_USER_INSERTS_FILE}; and it is not a UUID.")
            user_uuid = user_uuid_from_inserts[0]["uuid"]
    if user_uuid and portal:
        user = portal.get(f"/{user_uuid}", raise_exception=False)
        if not user or user.status_code != 200:
            _exit_without_action(f"The given user ({user_uuid}) was not found in the locally running portal database.")
    return user_uuid


def _generate_access_key_inserts_item(access_key_id: str, access_key_secret_hash: str, user_uuid: str) -> dict:
    return {
        "status": "current",
        "user": user_uuid,
        "description": f"Manually generated local access-key for testing.",
        "access_key_id": access_key_id,
        "secret_access_key_hash": access_key_secret_hash,
        "uuid": str(uuid.uuid4())
    }


def _generate_access_keys_file_item(access_key_id: str, access_key_secret: str, port: int) -> dict:
    return {
        "key": access_key_id,
        "secret": access_key_secret,
        "server": _get_local_portal_url(port)
    }


def _generate_access_key(ini_file: str = _DEFAULT_INI_FILE) -> Tuple[str, str, str]:
    access_key_secret = generate_access_key_secret()
    return generate_access_key(), access_key_secret, _hash_secret_like_snovault(access_key_secret, ini_file)


def _hash_secret_like_snovault(secret: str, ini_file: str = _DEFAULT_INI_FILE) -> str:

    # We do NOT store the secret in plaintext in the database, but rather a hash of it; this function
    # hashes the (given) secret in the same way that the portal (snovault) does and returns this result.
    # See access_key_add in snovault/types/access_key.py and includeme in snovault/authentication.py.
    # Using that code directly from snovault is a little tricker then we want to deal with for this;
    # and/but we do make an effort to read any passlib properties which might exist in the .ini file,
    # just like snovault does; perhaps overkill; default is development.ini; change with --ini. 
    def get_passlib_properties_from_ini_file(ini_file : str = _DEFAULT_INI_FILE,
                                             section_name = "app:app",
                                             property_name_prefix = "passlib.") -> str:
        """
        Returns from the specified section of the specified .ini file the values of properties with
        the specified property name prefix, in the form of a dictionary, where the property names have
        that specified property name prefix removed, and the property value is the associated value.
        """
        properties = {}
        try:
            config = configparser.ConfigParser()
            read_files = config.read(ini_file)
            if not read_files or read_files[0] != ini_file:
                _exit_without_action(f"The given ini file ({ini_file}) cannot be read.")
            for property_name in [p for p in config.options(section_name) if p.startswith(property_name_prefix)]:
                property_value = config.get(section_name, property_name)
                properties[property_name[len(property_name_prefix):]] = property_value
        except Exception:
            pass
        return properties

    passlib_properties = get_passlib_properties_from_ini_file(ini_file)
    if not passlib_properties:
        passlib_properties = {"schemes": "edw_hash, unix_disabled"}
    register_crypt_handler(EDWHash)
    return CryptContext(**passlib_properties).hash(secret)


def _guess_default_app() -> str:
    # This should return one of: smaht-portal, cgap-portal, or fourfront
    # that is, if running within repos: smaht-portal, cgap-portal, or fourfront
    repository = app_project().REPO_NAME
    return repository if repository != "cgap-portal" else "cgap"


def _is_local_portal_running(port: int) -> None:
    try:
        return requests.get(f"{_get_local_portal_url(port)}/health").status_code == 200
    except Exception:
        return False


def _get_local_portal_url(port: int) -> None:
    return f"http://localhost:{port}"


def _load_data(portal: Portal, data: AnyJsonData, data_type: str) -> bool:
    if isinstance(data, dict):
        data = [data]
    elif not isinstance(data, list):
        return False
    if not data_type:
        return False
    data = {data_type: data}
    load_all(portal.vapp, inserts=data, docsdir=None, overwrite=True, itype=[data_type], from_json=True)
    return True


def _print_all_access_keys(portal: Portal, verbose: bool = False) -> None:
    print("All access-keys defined for locally running portal:")
    for item in _get_all_access_keys(portal):
        print(f"{item.id}", end="")
        if item.created:
            print(f" | Created: {item.created}", end="")
        if item.expires:
            print(f" | Expires: {item.expires}", end="")
        if verbose:
            print(f" | {item.uuid}", end="")
        print()


def _get_all_access_keys(portal: Portal) -> list:
    response = []
    try:
        AccessKey = namedtuple("AccessKey", ["id", "uuid", "created", "expires"])
        for access_key in portal.get(f"/access-keys").json()["@graph"]:
            response.append(AccessKey(
                id=access_key.get("access_key_id"),
                uuid=access_key.get("uuid"),
                created=_format_iso_datetime_string_to_local_datetime_string(access_key.get("date_created")),
                expires=_format_iso_datetime_string_to_local_datetime_string(access_key.get("expiration_date"))))
    except Exception:
        pass
    return sorted(response, key=lambda item: item.created, reverse=True)


# TODO: Use misc_utils.is_uuid when out of review ...
# https://github.com/4dn-dcic/utils/blob/kmp_sheet_utils_schema_hinting/dcicutils/misc_utils.py#L1371
def _is_uuid(s: str) -> bool:
    try:
        return str(uuid.UUID(s)) == s
    except Exception:
        return False


def _exit_without_action(message: str) -> None:
    print(f"\nERROR: {message}")
    exit(1)


# TODO: Move these datetime utility function to dcicutils ...

def _parse_iso_datetime_string(value: str, format: str = "%Y-%m-%dT%H:%M:%S.%f") -> datetime:
    try:
        if value.endswith("+00:00"):
            value = value[:-6]
        return pytz.utc.localize(datetime.strptime(value, format))
    except Exception:
        return ""


def _convert_iso_to_local_datetime(value: datetime, format: str = "%Y-%m-%dT%H:%M:%S.%f") -> datetime:
    try:
        return value.astimezone(dateutil.tz.tzlocal())
    except Exception:
        return ""


def _format_datetime(value: datetime, format: str = "%Y-%m-%d %H:%M:%S %Z") -> str:
    try:
        return value.strftime(format)
    except Exception:
        return ""


def _format_iso_datetime_string_to_local_datetime_string(value: str) -> str:
    return _format_datetime(_convert_iso_to_local_datetime(_parse_iso_datetime_string(value)))


if __name__ == "__main__":
    main()
