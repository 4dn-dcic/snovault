# --------------------------------------------------------------------------------------------------
# Temporary script to generate a new portal access-key for local (localhost) development purposes.
# --------------------------------------------------------------------------------------------------
# This will be needed only until the SMaHT portal has been fleshed out enough to be able to do this
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
# updated with the new access-key (for the "smaht-localhost" property).
#
# The --update option may be used to specify both --update-database and --update-keys.
# --------------------------------------------------------------------------------------------------

import argparse
import configparser
import io
import json
import os
import requests
import uuid
from typing import Optional, Tuple
from snovault.authentication import (
    generate_password as generate_access_key_secret,
    generate_user as generate_access_key
)
from snovault.dev_servers import load_data
from snovault.edw_hash import EDWHash
from passlib.context import CryptContext
from passlib.registry import register_crypt_handler
from .captured_output import captured_output


_INSERTS_DIR = "src/encoded/tests/data/master-inserts"
_USER_INSERTS_FILE = f"{_INSERTS_DIR}/user.json"
_DEFAULT_INI_FILE = "development.ini"


def main():

    parser = argparse.ArgumentParser(description="Create local portal access-key for dev/testing purposes.")
    parser.add_argument("--user", required=False,
                        help=f"User email for which the access-key should be defined (in master-inserts/user.json); or a UUID.")
    parser.add_argument("--update", action="store_true", required=False, default=False,
                        help=f"Same as --update-database and --update-keys both.")
    parser.add_argument("--update-database", action="store_true", required=False, default=False,
                        help=f"Updates the database of your locally running portal with the new access-key.")
    parser.add_argument("--update-keys", action="store_true", required=False, default=False,
                        help=f"Updates your access-keys file (e.g. ~/.smaht-keys.json) with the new access-key (e.g. smaht-localhost).")
    parser.add_argument("--port", type=int, required=False, default=8000,
                        help="Port for localhost on which your local portal is running.")
    parser.add_argument("--ini", type=str, required=False, default=_DEFAULT_INI_FILE,
                        help=f"Name of the application .ini file; default is: {_DEFAULT_INI_FILE}")
    parser.add_argument("--app", choices=["smaht", "cgap", "fourfront"], required=False, default="smaht",
                        help="App name for which the access-key should be generated; default is smaht.")
    parser.add_argument("--verbose", action="store_true", required=False, default=False, help="Verbose output.")
    parser.add_argument("--debug", action="store_true", required=False, default=False, help="Debugging output.")
    args = parser.parse_args()

    _ACCESS_KEYS_FILE = os.path.expanduser(f"~/.{args.app}-keys.json")
    _ACCESS_KEYS_FILE_ITEM = f"{args.app}-localhost"

    if args.update:
        args.update_database = True
        args.update_keys = True

    print("Creating a new local portal access-key ... ", end="")
    access_key_user_uuid = _generate_user_uuid(args.user, args.update_database)
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
            json.dump(access_keys_file_json, access_keys_file_f, indent=4)
        print("Done.")
    if not args.update_keys or args.verbose:
        print(f"New local portal access-key record suitable for: {_ACCESS_KEYS_FILE} ...")
        print(json.dumps(access_keys_file_item, indent=4))

    if args.update_database:
        if not _is_local_portal_running(args.port):
            _exit_without_action(f"Portal must be running locally ({_get_local_portal_url(args.port)}) to do an insert.")
        print(f"Writing new local portal access-key to locally running portal database ... ", end="")
        with captured_output(not args.debug):
            load_data(access_key_inserts_file_item, "access_key")
        print("Done.")
    if not args.update_database or args.verbose:
        print(f"New local portal access-key insert record suitable for: {_INSERTS_DIR}/access_key.json ...")
        print(json.dumps(access_key_inserts_file_item, indent=4))


def _generate_user_uuid(user: Optional[str], update_database: bool) -> Optional[str]:
    if not user:
        if update_database:
            _exit_without_action(f"The --user option must be used to specify a UUID or an email in: {_USER_INSERTS_FILE}")
        return "<your-user-uuid>"
    if _is_uuid(user):
        return user
    with io.open(_USER_INSERTS_FILE, "r") as user_inserts_f:
        user_uuid_from_inserts = [item for item in json.load(user_inserts_f) if item.get("email") == user]
        if not user_uuid_from_inserts:
            _exit_without_action(f"The given user ({user}) was not found as an email"
                                 f" in: {_USER_INSERTS_FILE}; and it is not a UUID.")
        return user_uuid_from_inserts[0]["uuid"]


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
    return generate_access_key(), access_key_secret, _hash_secret_like_snovault(access_key_secret)


def _hash_secret_like_snovault(secret: str) -> str:
    # We do NOT store the secret in plaintext in the database, but rather a hash of it; this function
    # hashes the (given) secret in the same way that the portal (snovault) does and returns this result.
    # See access_key_add in snovault/types/access_key.py and includeme in snovault/authentication.py.
    # Using that code directly from snovault is a little tricker then we want to deal with for this;
    # and/but we do make an effort to read any passlib properties which might exist in the .ini file,
    # just like snovault does; perhaps overkill; default is development.ini; change with --ini. 
    def get_passlib_properties_from_ini_file(ini_file_name: str = _DEFAULT_INI_FILE,
                                             section_name = "app:app",
                                             property_name_prefix = "passlib."):
        """
        Returns from the specified section of the specified .ini file the values of properties with
        the specified property name prefix, in the form of a dictionary, where the property names have
        that specified property name prefix removed, and the property value is the associated value.
        """
        properties = {}
        try:
            config = configparser.ConfigParser()
            config.read(ini_file_name)
            for property_name in [p for p in config.options(section_name) if p.startswith(property_name_prefix)]:
                property_value = config.get(section_name, property_name)
                properties[property_name[len(property_name_prefix):]] = property_value
        except Exception:
            pass
        return properties
    passlib_properties = get_passlib_properties_from_ini_file()
    if not passlib_properties:
        passlib_properties = {"schemes": "edw_hash, unix_disabled"}
    register_crypt_handler(EDWHash)
    return CryptContext(**passlib_properties).hash(secret)


def _is_local_portal_running(port: int) -> None:
    try:
        return requests.get(f"{_get_local_portal_url(port)}/health").status_code == 200
    except Exception:
        return False


def _get_local_portal_url(port: int) -> None:
    return f"http://localhost:{port}"


def _is_uuid(s: str) -> bool:
    try:
        return str(uuid.UUID(s)) == s
    except Exception:
        return False


def _exit_without_action(message: str) -> None:
    print(f"\nERROR: {message}")
    exit(1)


if __name__ == "__main__":
    main()