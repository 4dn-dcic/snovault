from __future__ import annotations

import argparse
import itertools
import json
import logging
import re
import structlog
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Union

from dcicutils import creds_utils
from dcicutils.ff_utils import get_metadata, search_metadata
from dcicutils.misc_utils import to_snake_case

from snovault.commands.utils import get_auth_key


logger = structlog.getLogger(__name__)
EPILOG = __doc__

DEFAULT_IGNORE_FIELDS = [
    "submitted_by",
    "date_created",
    "last_modified",
    "schema_version",
]
MASTER_INSERTS = "master-inserts"
INSERT_DIRECTORIES = [
    "inserts",
    MASTER_INSERTS,
    "perf-testing",
    "workbook-inserts",
    "temp-local-inserts",
    "deploy-inserts",
]
INSERTS_LOCATION = Path("src/encoded/tests/data/")
KEYS_TO_IGNORE_FOR_LINKS = set(  # Either UUIDs not needed or can look like UUIDs
    ["uuid", "title", "blob_id", "md5sum", "content_md5sum", "submitted_md5sum"]
)
UUID_IDENTIFIER = re.compile(
    r"^[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[a-f0-9]{4}-?[a-f0-9]{12}\Z",
    re.I,
)


def get_ignore_fields(ignore_fields: List[str]) -> Set[str]:
    """Get all fields to ignore when pulling inserts."""
    return set(DEFAULT_IGNORE_FIELDS + ignore_fields)


def update_inserts_from_server(
    inserts: Path,
    auth_key: Dict[str, str],
    ignore_fields: Iterable[str],
    item_types: Optional[List[str]] = None,
    update_existing: bool = False,
    from_search: Optional[str] = None,
    merge_existing: bool = True,
) -> None:
    """Update inserts for given server."""
    existing_inserts = get_existing_inserts(inserts)
    logger.info(f"Found existing inserts for {len(existing_inserts)} item types")
    existing_uuids = get_existing_uuids_to_update(
        existing_inserts, item_types=item_types, update_all=update_existing
    )
    if existing_uuids:
        logger.info(f"Found {len(existing_uuids)} existing items to update")
    search_uuids = get_uuids_from_search(from_search, auth_key)
    if search_uuids:
        logger.info(f"Found {len(search_uuids)} items from search")
    base_uuids = get_base_uuids(existing_uuids, search_uuids)
    logger.info("Collecting inserts from portal. This may take a while...")
    inserts_from_portal = get_inserts_from_portal(base_uuids, auth_key, ignore_fields)
    logger.info(
        f"Found inserts for {len(inserts_from_portal)} item types from portal:"
        f" {[item_type_inserts.item_type for item_type_inserts in inserts_from_portal]}"
    )
    inserts_to_write = get_inserts_to_write(
        inserts, inserts_from_portal, existing_inserts, merge_existing=merge_existing
    )
    logger.info(f"Writing inserts for {len(inserts_to_write)} item types to {inserts}")
    write_inserts(inserts_to_write, inserts)


@dataclass(frozen=True)
class Insert:
    uuid: str
    properties: Dict[str, Any]

    def __hash__(self):
        return hash(self.uuid)

    def __eq__(self, other: Any):
        if not isinstance(other, Insert):
            return False
        return self.uuid == other.uuid

    def __repr__(self):
        return f"Insert({self.uuid})"

    def update(
        self,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Insert:
        """Update insert attributes."""
        if not properties or properties == self.properties:
            return self
        return Insert(
            uuid=self.uuid,
            properties=properties,
        )


@dataclass(frozen=True)
class ItemTypeInserts:
    item_type: str
    uuids: Set[str]
    inserts: Iterator[Insert]

    def __repr__(self):
        return f"{self.__class__.__name__}({self.item_type})"

    def add_insert(self, insert: Insert) -> ItemTypeInserts:
        """Update inserts for a given item type."""
        if insert.uuid in self.uuids:
            return self
        return ItemTypeInserts(
            item_type=self.item_type,
            uuids=self.uuids | {insert.uuid},
            inserts=itertools.chain(self.inserts, [insert]),
        )


def get_existing_uuids_to_update(
    existing_inserts: List[ItemTypeInserts],
    item_types: Optional[List[str]] = None,
    update_all: bool = False,
) -> Set[str]:
    """Get all existing inserts to update from given directory.

    Note: Pulling existing inserts into memory
    """
    if update_all:
        return set(
            itertools.chain(
                *[item_type_inserts.uuids for item_type_inserts in existing_inserts]
            )
        )
    return get_uuids_for_item_types(existing_inserts, item_types)


def get_existing_inserts(inserts_path: Path) -> List[ItemTypeInserts]:
    """Get all existing inserts from given directory.

    Ignore any item types with no inserts.
    """
    inserts_files = inserts_path.glob("*.json")
    item_type_inserts = [
        get_item_type_inserts(insert_file) for insert_file in inserts_files
    ]
    return get_non_empty_item_type_inserts(item_type_inserts)


def get_non_empty_item_type_inserts(
    item_type_inserts: List[ItemTypeInserts],
) -> List[ItemTypeInserts]:
    """Get all non-empty item type inserts."""
    return [
        item_type_insert
        for item_type_insert in item_type_inserts
        if item_type_insert.uuids
    ]


def get_item_type_inserts(insert_file: Path) -> ItemTypeInserts:
    """Get all inserts for a given item type.

    Ensure inserts are sorted by UUID for later comparison with portal
    inserts.
    """
    item_type = insert_file.stem
    uuids = set()
    inserts = []
    file_inserts = get_inserts_from_file(insert_file)
    for insert in sort_inserts_by_uuid(file_inserts):
        uuids |= {insert.uuid}
        inserts = itertools.chain(inserts, [insert])
    return ItemTypeInserts(
        item_type=item_type,
        uuids=uuids,
        inserts=inserts,
    )


def create_insert(insert: Dict[str, Any]) -> Insert:
    """Create Insert from JSON."""
    return Insert(
        uuid=get_uuid(insert),
        properties=insert,
    )


def get_inserts_from_file(insert_file: Path) -> Iterator[Insert]:
    """Load inserts from file."""
    with insert_file.open("r") as f:
        inserts = json.load(f)
    return (create_insert(insert) for insert in inserts)


def sort_inserts_by_uuid(inserts: Iterable[Insert]) -> List[Insert]:
    """Sort all inserts by UUID."""
    return sorted(inserts, key=lambda insert: insert.uuid)


def get_uuid(item: Dict[str, Any]) -> str:
    return item.get("uuid", "")


def get_uuids_for_item_types(
    existing_inserts: List[ItemTypeInserts],
    item_types: Optional[List[str]] = None,
) -> Set[str]:
    """Get all inserts to update from given directory."""
    if item_types:
        item_types_to_keep = set([to_snake_case(target) for target in item_types])
        return set(
            itertools.chain(
                *[
                    item_type_inserts.uuids
                    for item_type_inserts in existing_inserts
                    if item_type_inserts.item_type in item_types_to_keep
                ]
            )
        )
    return set()


def get_uuids_from_search(
    search_query: Union[str, None], auth_key: Dict[str, str]
) -> List[str]:
    """Get all uuids from a given search query."""
    if not search_query:
        return []
    query = format_search_query(search_query)
    return [get_uuid(item) for item in search_metadata(query, key=auth_key)]


def format_search_query(search_query: str) -> str:
    """Format provided search query.

    Include UUID add-on to limit response size and use heuristics to
    format query as needed (e.g. so no need to add boilerplate start of
    "search/").
    """
    if "field=uuid" not in search_query:
        search_query += "&field=uuid"
    if "/" in search_query:  # Likely formatted appropriately
        return search_query
    if not search_query.startswith("?"):
        return f"search/?{search_query}"
    return f"search/{search_query}"


def get_base_uuids(
    existing_uuids: Set[str],
    search_uuids: List[str],
) -> Set[str]:
    """Get all base UUIDs to start pulling inserts from portal."""
    return existing_uuids | set(search_uuids)


def get_inserts_from_portal(
    uuids: Iterable[str], auth_key: Dict[str, str], ignore_fields: Set[str]
) -> List[ItemTypeInserts]:
    """Get all inserts to write.

    Start from base UUIDs and branch to all linked items.
    """
    item_type_inserts = []
    seen = set()
    uuids_to_get = set(uuids)
    while uuids_to_get:
        current_uuids_to_get = uuids_to_get - seen
        uuids_to_get = set()
        for uuid in current_uuids_to_get:
            item_type = get_item_type(uuid, auth_key)
            insert = get_insert(uuid, auth_key, ignore_fields)
            item_type_inserts = update_item_type_inserts(
                item_type_inserts, item_type, insert
            )
            seen |= {uuid}
            uuids_to_get |= get_links(insert) - seen
    return item_type_inserts


def get_item(uuid: str, auth_key: Dict[str, str], frame: str = "raw") -> Dict[str, Any]:
    """Get item for a given UUID."""
    add_on = f"frame={frame}" if frame else ""
    return get_metadata(uuid, key=auth_key, add_on=add_on)


def get_insert(uuid: str, auth_key: Dict[str, str], ignore_fields: Set[str]) -> Insert:
    """Get insert for a given item."""
    item = get_item(uuid, auth_key)
    properties = get_insert_properties(item, ignore_fields)
    return create_insert(properties)


def get_item_type(uuid: str, auth_key: Dict[str, str]) -> str:
    """Get item type for a given item.

    Ensure snake_cased to match insert file names.
    """
    item = get_item(uuid, auth_key, frame="object")
    return to_snake_case(item["@type"][0])


def get_insert_properties(
    item: Dict[str, Any], ignore_fields: Set[str]
) -> Dict[str, Any]:
    """Get all properties for a given item."""
    return {key: value for key, value in item.items() if key not in ignore_fields}


def update_item_type_inserts(
    item_type_inserts: List[ItemTypeInserts],
    item_type: str,
    insert: Insert,
) -> List[ItemTypeInserts]:
    """Update item type inserts with new insert.

    If item type already exists, add insert to existing item type inserts.
    Otherwise, create new item type inserts.
    """
    existing_item_types = {
        item_type_inserts.item_type for item_type_inserts in item_type_inserts
    }
    if item_type in existing_item_types:
        return [
            item_type_insert.add_insert(insert)
            if item_type_insert.item_type == item_type
            else item_type_insert
            for item_type_insert in item_type_inserts
        ]
    new_item_type_inserts = ItemTypeInserts(
        item_type=item_type,
        uuids={insert.uuid},
        inserts=(insert,),
    )
    return item_type_inserts + [new_item_type_inserts]


def get_links(item: Any) -> Set[str]:
    """Get all links for a given item."""
    if isinstance(item, Insert):
        return get_links_from_insert(item)
    if isinstance(item, dict):
        return get_links_from_dict(item)
    if isinstance(item, list):
        return get_links_from_list(item)
    if isinstance(item, str):
        if is_uuid(item):
            return {item}
    return set()


def get_links_from_insert(item: Insert) -> Set[str]:
    """Get all links for a given insert."""
    return get_links_from_dict(item.properties)


def get_links_from_dict(item: Dict[str, Any]) -> Set[str]:
    """Get all links for a given dictionary."""
    return {
        link
        for key, value in item.items()
        if key not in KEYS_TO_IGNORE_FOR_LINKS
        for link in get_links(value)
    }


def get_links_from_list(item: List[Any]) -> Set[str]:
    """Get all links for a given list."""
    return {link for value in item for link in get_links(value)}


def is_uuid(value: str) -> bool:
    """Check if a given value is a UUID."""
    return bool(UUID_IDENTIFIER.match(value))


def get_inserts_to_write(
    inserts_path: Path,
    inserts_from_portal: List[ItemTypeInserts],
    existing_inserts: List[ItemTypeInserts],
    merge_existing: bool = True,
) -> List[ItemTypeInserts]:
    """Get all inserts to write.

    Update portal inserts with existing inserts information, if present,
    and remove conflicts with master-inserts (unless updating them).
    """
    inserts_to_write = get_item_type_inserts_to_write(
        inserts_from_portal, existing_inserts, merge_existing=merge_existing
    )
    if is_master_inserts(inserts_path):
        return inserts_to_write
    return get_inserts_without_conflicts(inserts_to_write)


def is_master_inserts(inserts_path: Path) -> bool:
    """Check if inserts are master-inserts."""
    return inserts_path.stem == MASTER_INSERTS


def get_item_type_inserts_to_write(
    inserts_from_portal: List[ItemTypeInserts],
    existing_inserts_to_update: List[ItemTypeInserts],
    merge_existing: bool = True,
) -> List[ItemTypeInserts]:
    """Update portal inserts with existing inserts data, if present.

    Merge existing properties as directed.
    """
    if not existing_inserts_to_update:
        return inserts_from_portal
    existing_item_types_to_inserts = {
        item_type_inserts.item_type: item_type_inserts
        for item_type_inserts in existing_inserts_to_update
    }
    return [
        get_updated_item_type_inserts(
            portal_item_type_inserts,
            existing_item_types_to_inserts[portal_item_type_inserts.item_type],
            merge_existing=merge_existing,
        )
        if portal_item_type_inserts.item_type in existing_item_types_to_inserts
        else portal_item_type_inserts
        for portal_item_type_inserts in inserts_from_portal
    ]


def get_updated_item_type_inserts(
    inserts_from_portal: ItemTypeInserts,
    existing_inserts: ItemTypeInserts,
    merge_existing: bool = True,
) -> ItemTypeInserts:
    """Combine portal inserts with existing inserts.

    Walk through portal and existing inserts, collecting all inserts to
    write for a given item type.

    Keep all inserts found only from portal or only in existing inserts
    exactly as they are.

    For inserts found in both portal and existing inserts, keep
    non-overlapping existing properties if directed. Otherwise, only
    keep portal insert properties.

    Note: Existing inserts are assumed to be sorted by UUID (performed
    when loaded from file).
    """
    sorted_portal_inserts = (
        insert for insert in sort_inserts_by_uuid(inserts_from_portal.inserts)
    )
    sorted_existing_inserts = existing_inserts.inserts
    uuids = set()
    inserts = []
    portal_insert = get_next_insert(sorted_portal_inserts)
    existing_insert = get_next_insert(sorted_existing_inserts)
    while portal_insert or existing_insert:
        if not portal_insert:
            insert_to_add = existing_insert
            existing_insert = get_next_insert(sorted_existing_inserts)
        elif not existing_insert:
            insert_to_add = portal_insert
            portal_insert = get_next_insert(sorted_portal_inserts)
        elif portal_insert.uuid < existing_insert.uuid:
            insert_to_add = portal_insert
            portal_insert = get_next_insert(sorted_portal_inserts)
        elif portal_insert.uuid > existing_insert.uuid:
            insert_to_add = existing_insert
            existing_insert = get_next_insert(sorted_existing_inserts)
        else:  # portal_insert.uuid == existing_insert.uuid
            insert_to_add = get_updated_insert(
                portal_insert, existing_insert, merge_existing=merge_existing
            )
            portal_insert = get_next_insert(sorted_portal_inserts)
            existing_insert = get_next_insert(sorted_existing_inserts)
        uuids |= {insert_to_add.uuid}
        inserts = itertools.chain(inserts, [insert_to_add])
    return ItemTypeInserts(
        item_type=inserts_from_portal.item_type,
        uuids=uuids,
        inserts=inserts,
    )


def get_next_insert(inserts: Iterator[Insert]) -> Union[Insert, None]:
    """Get next insert from an iterable."""
    return next(inserts, None)


def get_updated_insert(
    portal_insert: Insert,
    existing_insert: Insert,
    merge_existing: bool = True,
) -> Insert:
    """Update existing insert with portal insert properties.

    If directed, keep existing properties not present in portal insert.
    """
    if merge_existing:
        properties = {
            **existing_insert.properties,
            **portal_insert.properties,
        }
        return portal_insert.update(properties=properties)
    return portal_insert


def get_inserts_without_conflicts(
    item_type_inserts: List[ItemTypeInserts],
) -> List[ItemTypeInserts]:
    """Remove all conflicts with master-inserts."""
    master_inserts = get_existing_inserts(INSERTS_LOCATION.joinpath(MASTER_INSERTS))
    master_inserts_item_types = {
        item_type_inserts.item_type: item_type_inserts
        for item_type_inserts in master_inserts
    }
    item_type_inserts_without_conflicts = [
        get_inserts_without_conflicts_for_item_type(
            item_type_inserts, master_inserts_item_types[item_type_inserts.item_type]
        )
        if are_item_type_inserts_present_and_overlapping(
            item_type_inserts, master_inserts_item_types
        )
        else item_type_inserts
        for item_type_inserts in item_type_inserts
    ]
    return get_non_empty_item_type_inserts(item_type_inserts_without_conflicts)


def get_inserts_without_conflicts_for_item_type(
    portal_item_type_inserts: ItemTypeInserts,
    master_item_type_inserts: ItemTypeInserts,
) -> ItemTypeInserts:
    """Remove conflicts with master-inserts for a given item type.

    Assumes both portal and master inserts are sorted by UUID.
    """
    uuids = set()
    inserts = []
    for insert in sort_inserts_by_uuid(portal_item_type_inserts.inserts):
        if is_insert_in_master_inserts(insert, master_item_type_inserts):
            continue
        uuids |= {insert.uuid}
        inserts = itertools.chain(inserts, [insert])
    return ItemTypeInserts(
        item_type=portal_item_type_inserts.item_type,
        uuids=uuids,
        inserts=inserts,
    )


def is_insert_in_master_inserts(
    insert: Insert,
    master_item_type_inserts: ItemTypeInserts,
) -> bool:
    """Check if a given insert is in master-inserts.

    Assumes master inserts are sorted by UUID and portal inserts arrive
    in order as well.
    """
    if insert.uuid in master_item_type_inserts.uuids:
        for master_insert in master_item_type_inserts.inserts:
            if master_insert.uuid == insert.uuid:
                if master_insert.properties == insert.properties:
                    logging.info(
                        f"Skipping {insert.uuid} for"
                        f" {master_item_type_inserts.item_type} as"
                        f" already in master-inserts"
                    )
                else:
                    logging.warning(
                        f" Skipping {insert.uuid} for"
                        f" {master_item_type_inserts.item_type} as"
                        f" conflicts with master-inserts"
                    )
                return True
    return False


def are_item_type_inserts_present_and_overlapping(
    item_type_inserts: ItemTypeInserts,
    comparison_inserts: Dict[str, ItemTypeInserts],
) -> bool:
    """Check if item type inserts are present in and overlap inserts."""
    return item_type_inserts.item_type in comparison_inserts and do_inserts_overlap(
        item_type_inserts, comparison_inserts[item_type_inserts.item_type]
    )


def do_inserts_overlap(
    item_type_inserts_1: ItemTypeInserts,
    item_type_inserts_2: ItemTypeInserts,
) -> bool:
    """Check if portal inserts overlap with existing inserts."""
    return bool(item_type_inserts_1.uuids & item_type_inserts_2.uuids)


def write_inserts(item_type_inserts: List[ItemTypeInserts], inserts_path: Path) -> None:
    """Write all inserts to given directory."""
    for item_type_insert in item_type_inserts:
        write_inserts_for_type(
            item_type_insert.item_type, item_type_insert.inserts, inserts_path
        )


def write_inserts_for_type(
    item_type: str,
    inserts_for_type: Iterable[Insert],
    inserts_path: Path,
) -> None:
    """Write all inserts for a given item type to given directory."""
    insert_file = inserts_path.joinpath(f"{item_type}.json")
    inserts = sort_inserts_by_uuid(inserts_for_type)
    to_write = [insert.properties for insert in inserts]
    with insert_file.open("w") as file_handle:
        json.dump(to_write, file_handle, indent=4)
    logger.info(f"Wrote {len(inserts)} inserts for {item_type} to {insert_file}")


def main():
    """Update inserts from a given portal."""
    logging.basicConfig()
    logging.getLogger("encoded").setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(
        description="Update Inserts",
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--env", default="data", help="Environment to update from. Defaults to data"
    )
    parser.add_argument(
        "--portal",
        help="Portal to update inserts from. Defaults to smaht",
        choices=creds_utils._KEY_MANAGERS.keys(),
        default="smaht",
    )
    parser.add_argument(
        "--dest",
        default="temp-local-inserts",
        help="Destination inserts directory. Defaults to temp-local-inserts.",
    )
    parser.add_argument(
        "--item",
        nargs="+",
        help="Existing item type(s) (e.g. file_fastq) to update inserts for.",
    )
    parser.add_argument(
        "--update",
        help=(
            "Update all existing inserts with data from portal."
            " Overrides --item flag."
        ),
        action="store_true",
    )
    parser.add_argument(
        "--ignore",
        nargs="+",
        default=DEFAULT_IGNORE_FIELDS,
        help="Properties to ignore when pulling inserts",
    )
    parser.add_argument(
        "--search",
        help=(
            "Query to find new items to add to inserts, e.g."
            " 'type=FileFastq&status=uploaded'."
        ),
        type=str,
    )
    parser.add_argument(
        "--refresh",
        help=(
            "Replace existing inserts with new inserts from portal, i.e. don't keep"
            " properties from existing inserts not present in portal."
        ),
        action="store_true",
    )
    args = parser.parse_args()

    ignore_fields = get_ignore_fields(args.ignore)
    auth_key = get_auth_key(args.portal, args.env)
    if args.dest not in INSERT_DIRECTORIES:
        proceed = input(
            f"Destination {args.dest} not in common choices: {INSERT_DIRECTORIES}."
            f"\nContinue? (y/n): "
        )
        if not proceed.lower().startswith("y"):
            logger.info("Exiting.")
            return
    inserts_path = INSERTS_LOCATION.joinpath(args.dest)
    if not inserts_path.exists():
        inserts_path.mkdir()
    update_inserts_from_server(
        inserts_path,
        auth_key,
        ignore_fields,
        item_types=args.item,
        update_existing=args.update,
        from_search=args.search,
        merge_existing=not args.refresh,
    )


if __name__ == "__main__":
    main()
