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
#
# Note that instead of a uuid you can also actually use a path, for example:
#   view-local-object /file-formats/vcf_gz_tbi
#
# --------------------------------------------------------------------------------------------------

import argparse
import json
import sys
from typing import Optional
import yaml
from dcicutils.misc_utils import get_error_message
from dcicutils.portal_utils import Portal
from snovault.commands.captured_output import captured_output, uncaptured_output


_DEFAULT_INI_FILE = "development.ini"


def main():

    parser = argparse.ArgumentParser(description="Create local portal access-key for dev/testing purposes.")
    parser.add_argument("uuid", type=str,
                        help=f"The uuid (or path) of the object to fetch and view. ")
    parser.add_argument("--ini", type=str, required=False, default=None,
                        help=f"Name of the application .ini file; default is: {_DEFAULT_INI_FILE}")
    parser.add_argument("--env", type=str, required=False, default=None,
                        help=f"Environment name (key from ~/.smaht-keys.json).")
    parser.add_argument("--server", type=str, required=False, default=None,
                        help=f"Environment server name (server from key in ~/.smaht-keys.json).")
    parser.add_argument("--app", type=str, required=False, default=None,
                        help=f"Application name (one of: smaht, cgap, fourfront).")
    parser.add_argument("--yaml", action="store_true", required=False, default=False, help="YAML output.")
    parser.add_argument("--verbose", action="store_true", required=False, default=False, help="Verbose output.")
    parser.add_argument("--debug", action="store_true", required=False, default=False, help="Debugging output.")
    args = parser.parse_args()

    portal = _create_portal(ini=args.ini, env=args.env, server=args.server, app=args.app, debug=args.debug)
    data = _get_local_object(portal=portal, uuid=args.uuid, verbose=args.verbose)

    if args.yaml:
        _print(yaml.dump(data))
    else:
        _print(json.dumps(data, default=str, indent=4))


def _create_portal(ini: str = _DEFAULT_INI_FILE, env: Optional[str] = None,
                   server: Optional[str] = None, app: Optional[str] = None, debug: bool = False) -> dict:
    with captured_output(not debug):
        return Portal(env, server=server, app=app) if env or app else Portal(ini or _DEFAULT_INI_FILE)


def _get_local_object(portal: Portal, uuid: str, verbose: bool = False) -> dict:
    if verbose:
        _print(f"Getting object ({uuid}) from local portal ... ", end="")
    response = None
    try:
        if not uuid.startswith("/"):
            path = f"/{uuid}"
        else:
            path = uuid
        response = portal.get(path)
    except Exception as e:
        if "404" in str(e) and "not found" in str(e).lower():
            if verbose:
                _print("Not found!")
            else:
                _print(f"Object ({uuid}) not found!")
            _exit_without_action()
        _exit_without_action(f"Exception getting object ({uuid}) -> {get_error_message(e)}", newline=verbose)
    if not response:
        _exit_without_action(f"Null response getting object {uuid}).")
    if response.status_code != 200:
        _exit_without_action(f"Invalid status code ({response.status_code}) getting object {uuid}).")
    if not response.json:
        _exit_without_action(f"Invalid JSON getting object {uuid}).")
    if verbose:
        _print("OK")
    return response.json()


def _print(*args, **kwargs):
    with uncaptured_output():
        print(*args, **kwargs)
    sys.stdout.flush()


def _exit_without_action(message: Optional[str] = None, newline: bool = True) -> None:
    if message:
        if newline:
            _print()
        _print(f"ERROR: {message}")
    exit(1)


if __name__ == "__main__":
    main()
