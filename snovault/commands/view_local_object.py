# --------------------------------------------------------------------------------------------------
# Command-line utility to retrieve and print the given object (UUID) from a locally running portal.
# --------------------------------------------------------------------------------------------------
# Example command:
#  view-local-object 4483b19d-62e7-4e7f-a211-0395343a35df
#
# Example output:
#  (smaht-portal-3.9.16) mac: view-local-object 3968e38e-c11f-472e-8531-8650e2e296d4 --yaml
#  '@context': /terms/
#  '@id': /access-keys/3968e38e-c11f-472e-8531-8650e2e296d4/
#  '@type':
#  - AccessKey
#  - Item
#  access_key_id: NSVCZ75O
#  actions:
#  - href: /access-keys/3968e38e-c11f-472e-8531-8650e2e296d4/?currentAction=create
#    name: create
#    profile: /profiles/AccessKey.json
#    title: Create
#  - href: /access-keys/3968e38e-c11f-472e-8531-8650e2e296d4/?currentAction=edit
#    name: edit
#    profile: /profiles/AccessKey.json
#    title: Edit
#  aggregated-items: {}
#  date_created: '2023-09-06T13:11:59.704005+00:00'
#  description: Manually generated local access-key for testing.
#  display_title: AccessKey from 2023-09-06
#  expiration_date: '2023-12-05T13:11:59.714106'
#  last_modified:
#    date_modified: '2023-09-06T13:11:59.711367+00:00'
#    modified_by:
#      '@id': /users/3202fd57-44d2-44fb-a131-afb1e43d8ae5/
#      '@type':
#      - User
#      - Item
#      display_title: loadxl loadxl
#      principals_allowed:
#        edit:
#        - group.admin
#        view:
#        - group.admin
# #         - group.read-only-admin
#        - remoteuser.EMBED
#        - remoteuser.INDEXER
#      status: current
#      uuid: 3202fd57-44d2-44fb-a131-afb1e43d8ae5
#  principals_allowed:
#    edit:
#    - group.admin
#    - userid.74fef71a-dfc1-4aa4-acc0-cedcb7ac1d68
#    view:
#    - group.admin
#    - group.read-only-admin
#    - remoteuser.EMBED
#    - remoteuser.INDEXER
#    - userid.74fef71a-dfc1-4aa4-acc0-cedcb7ac1d68
#  schema_version: '1'
#  status: current
#  user:
#    '@id': /users/74fef71a-dfc1-4aa4-acc0-cedcb7ac1d68/
#    '@type':
#    - User
#    - Item
#    display_title: David Michaels
#    principals_allowed:
#      edit:
#      - group.admin
#      view:
#      - group.admin
#      - group.read-only-admin
#      - remoteuser.EMBED
#      - remoteuser.INDEXER
#    status: current
#    uuid: 74fef71a-dfc1-4aa4-acc0-cedcb7ac1d68
#  uuid: 3968e38e-c11f-472e-8531-8650e2e296d4
#  validation-errors: []
# --------------------------------------------------------------------------------------------------

import argparse
import json
import sys
import yaml
from dcicutils.misc_utils import get_error_message
from snovault.loadxl import create_testapp
from snovault.commands.captured_output import captured_output, uncaptured_output


_DEFAULT_INI_FILE = "development.ini"


def main():

    parser = argparse.ArgumentParser(description="Create local portal access-key for dev/testing purposes.")
    parser.add_argument("uuid", type=str)
    parser.add_argument("--ini", type=str, required=False, default=_DEFAULT_INI_FILE,
                        help=f"Name of the application .ini file; default is: {_DEFAULT_INI_FILE}")
    parser.add_argument("--yaml", action="store_true", required=False, default=False, help="YAML output.")
    parser.add_argument("--verbose", action="store_true", required=False, default=False, help="Verbose output.")
    parser.add_argument("--debug", action="store_true", required=False, default=False, help="Debugging output.")
    args = parser.parse_args()

    try:
        if args.verbose:
            _print(f"Getting object ({args.uuid}) from local portal ... ", end="")
        data = _get_local_object(args.uuid, args.ini, args.debug)
        if args.verbose:
            _print("OK")
        if args.yaml:
            _print(yaml.dump(data))
        else:
            _print(json.dumps(data, default=str, indent=4))
    except Exception as e:
        _exit_without_action(f"Exception getting object ({args.uuid}) -> {get_error_message(e)}", False)


def _get_local_object(uuid: str, ini: str = _DEFAULT_INI_FILE, debug: bool = False) -> dict:
    try:
        response = None
        with captured_output(not debug):
            app = create_testapp(ini)
            response = app.get_with_follow(f"/{uuid}")
        if not response:
            _exit_without_action(f"Null response getting object {uuid}).")
        if response.status_code != 200:
            _exit_without_action(f"Invalid status code ({response.status_code}) getting object {uuid}).")
        if not response.json:
            _exit_without_action(f"Invalid JSON getting object {uuid}).")
        return response.json
    except Exception as e:
        if "404" in str(e) and "not found" in str(e).lower():
            _exit_without_action(f"Object not found: {uuid}")
        _exit_without_action(f"Exception getting object ({uuid}) -> {get_error_message(e)}")


def _print(*args, **kwargs):
    with uncaptured_output():
        print(*args, **kwargs)
    sys.stdout.flush()


def _exit_without_action(message: str, newline: bool = True) -> None:
    if newline:
        _print()
    _print(f"ERROR: {message}")
    exit(1)


if __name__ == "__main__":
    main()
