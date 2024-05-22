from __future__ import annotations

import argparse
import itertools
import json
import logging
import re
import structlog
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple, Union

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
INSERT_DIRECTORIES = [
    "inserts",
    "master-inserts",
    "perf-testing",
    "workbook-inserts",
    "temp-local-inserts",
    "deploy-inserts",
]
INSERTS_LOCATION = Path("src/encoded/tests/data/")
KEYS_TO_IGNORE_FOR_LINKS = set(  # Either UUIDs not needed or can look like UUIDs
    ["uuid", "title", "blob_id", "md5sum", "content_md5sum"]
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
    from_search: Optional[str] = None,
) -> None:
    """Update inserts for given server."""
    existing_inserts_to_update = get_existing_inserts_to_update(inserts, item_types)
    if existing_inserts_to_update:
        logger.info(
            f"Found {len(existing_inserts_to_update)} existing inserts to update"
        )
    else:
        logger.info("No existing inserts to update")
    search_uuids = get_uuids_from_search(from_search, auth_key)
    if search_uuids:
        logger.info(f"Found {len(search_uuids)} items from search")
    base_uuids_to_get = get_base_uuids_to_get(existing_inserts_to_update, search_uuids)
    inserts_from_portal = get_inserts_from_portal(
        base_uuids_to_get, auth_key, ignore_fields
    )
    if inserts_from_portal:
        logger.info(f"Fetched {len(inserts_from_portal)} inserts from portal")
    else:
        logger.warning("No inserts retrieved from portal. Exiting.")
        return
    inserts_to_write = get_inserts_to_write(
        inserts_from_portal, existing_inserts_to_update
    )
    if inserts_to_write:
        logger.info(f"Writing {len(inserts_to_write)} inserts to {inserts}")
    write_inserts(inserts_to_write, inserts)


@dataclass(frozen=True)
class Insert:
    DEFAULT_INDEX = 10000  # Default to high number for sorting

    item_type: str
    uuid: str
    properties: Dict[str, Any]
    index: int = DEFAULT_INDEX

    def __hash__(self):
        return hash(self.uuid)

    def __eq__(self, other: Any):
        if not isinstance(other, Insert):
            return False
        return self.uuid == other.uuid

    def __repr__(self):
        return f"Insert({self.item_type}, {self.uuid})"

    def update(
        self,
        properties: Optional[Dict[str, Any]] = None,
        index: int = DEFAULT_INDEX,
    ) -> Insert:
        """Update insert attributes."""
        if not properties and index is None:
            return self
        if properties == self.properties and index == self.index:
            return self
        return Insert(
            item_type=self.item_type,
            uuid=self.uuid,
            properties=properties or self.properties,
            index=index or self.index,
        )


def get_existing_inserts_to_update(
    inserts_path: Path,
    target_types: Optional[List[str]] = None,
) -> List[Insert]:
    """Get all existing inserts to update from given directory."""
    existing_inserts = get_existing_inserts(inserts_path)
    return get_inserts_to_update(existing_inserts, target_types)


def get_existing_inserts(inserts_path: Path) -> List[Insert]:
    """Get all existing inserts from given directory."""
    inserts_files = inserts_path.glob("*.json")
    return [
        insert for insert_file in inserts_files for insert in get_inserts(insert_file)
    ]


def get_inserts(insert_file: Path) -> List[Insert]:
    """Get all inserts from given file."""
    item_type = insert_file.stem
    with insert_file.open("r") as f:
        inserts = json.load(f)
    return [
        Insert(
            item_type=item_type,
            uuid=get_uuid(insert),
            properties=insert,
            index=idx,
        )
        for idx, insert in enumerate(inserts)
    ]


def get_uuid(item: Dict[str, Any]) -> str:
    return item.get("uuid", "")


def get_inserts_to_update(
    existing_inserts: List[Insert],
    target_types: Optional[List[str]] = None,
) -> List[Insert]:
    """Get all inserts to update from given directory."""
    if target_types:
        return [
            insert for insert in existing_inserts if insert.item_type in target_types
        ]
    return existing_inserts


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


def get_base_uuids_to_get(
    existing_inserts: Iterable[Insert],
    search_uuids: Set[str],
) -> List[str]:
    """Get all base uuids to get from given existing inserts and search uuids."""
    return set([insert.uuid for insert in existing_inserts]) | set(search_uuids)


def get_inserts_from_portal(
    uuids: Iterable[str], auth_key: Dict[str, str], ignore_fields: Set[str]
) -> Set[Insert]:
    """Get all inserts to write.

    Start from base UUIDs and branch to all linked items.
    """
    result = set()
    seen = set()
    uuids_to_get = set(uuids)
    while uuids_to_get:
        current_uuids_to_get = uuids_to_get - seen
        uuids_to_get = set()
        for uuid in current_uuids_to_get:
            insert = get_insert(uuid, auth_key, ignore_fields)
            result |= {insert}
            seen |= {uuid}
            uuids_to_get |= get_links(insert) - seen
    return result


def get_item(uuid: str, auth_key: Dict[str, str], frame: str = "raw") -> Dict[str, Any]:
    """Get item for a given UUID."""
    add_on = f"frame={frame}" if frame else ""
    return get_metadata(uuid, key=auth_key, add_on=add_on)


def get_insert(uuid: str, auth_key: Dict[str, str], ignore_fields: Set[str]) -> Insert:
    """Get insert for a given item."""
    item = get_item(uuid, auth_key)
    return Insert(
        item_type=get_item_type(item, auth_key),
        uuid=get_uuid(item),
        properties=get_insert_properties(item, ignore_fields),
    )


def get_item_type(item: Dict[str, Any], auth_key: Dict[str, str]) -> str:
    """Get item type for a given item."""
    item = get_item(get_uuid(item), auth_key, frame="object")
    return to_snake_case(item["@type"][0])


def get_insert_properties(
    item: Dict[str, Any], ignore_fields: Set[str]
) -> Dict[str, Any]:
    """Get all properties for a given item."""
    return {key: value for key, value in item.items() if key not in ignore_fields}


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
    inserts_from_portal: Iterable[Insert],
    existing_inserts_to_update: Iterable[Insert],
) -> Iterable[Insert]:
    """Get all inserts to write.

    Update portal inserts with existing inserts information, if present,
    and remove conflicts with master-inserts.
    """
    inserts_updated_with_existing = get_inserts_with_existing_data(
        inserts_from_portal, existing_inserts_to_update
    )
    return get_inserts_without_conflicts(inserts_updated_with_existing)


def get_inserts_with_existing_data(
    inserts_from_portal: Iterable[Insert],
    existing_inserts_to_update: Iterable[Insert],
) -> Iterable[Insert]:
    """Update portal inserts with existing inserts data, if present."""
    if not existing_inserts_to_update:
        return inserts_from_portal
    existing_inserts_map = {
        insert.uuid: insert for insert in existing_inserts_to_update
    }
    return [
        get_updated_insert(insert, existing_inserts_map[insert.uuid])
        if insert.uuid in existing_inserts_map
        else insert
        for insert in inserts_from_portal
    ]


def get_updated_insert(insert: Insert, existing_insert: Insert) -> Insert:
    """Update portal insert with data from existing insert.

    Preserve existing index and properties not present in portal insert.
    """
    return insert.update(
        properties={**existing_insert.properties, **insert.properties},
        index=existing_insert.index,
    )


def get_inserts_without_conflicts(inserts: Iterable[Insert]) -> Iterable[Insert]:
    """Remove all conflicts with master-inserts."""
    master_inserts = get_existing_inserts(INSERTS_LOCATION.joinpath("master-inserts"))
    master_inserts_map = {insert.uuid: insert for insert in master_inserts}
    return [
        insert
        for insert in inserts
        if not is_conflict_with_master_insert(insert, master_inserts_map)
    ]


def is_conflict_with_master_insert(
    insert: Insert, master_inserts_map: Dict[str, Insert]
) -> bool:
    """Check if a given insert conflicts with master-inserts."""
    if insert.uuid not in master_inserts_map:
        return False
    master_insert = master_inserts_map[insert.uuid]
    if are_inserts_equal(insert, master_insert):
        logger.info(
            f"Dropped insert {insert.uuid} from {insert.item_type} as already present"
            f" in master-inserts"
        )
        return False
    logger.warning(
        f"Dropped insert {insert.uuid} from {insert.item_type} due to conflict"
        f" with master-inserts."
    )
    return True


def are_inserts_equal(insert1: Insert, insert2: Insert) -> bool:
    """Check if two inserts have same properties."""
    return insert1.properties == insert2.properties


def write_inserts(inserts: Iterable[Insert], inserts_path: Path) -> None:
    """Write all inserts to given directory."""
    for item_type, inserts_for_type in group_inserts_by_type(inserts):
        write_inserts_for_type(item_type, inserts_for_type, inserts_path)
        logger.info(f"Wrote {len(inserts_for_type)} {item_type} inserts")


def get_insert_item_type(insert: Insert) -> str:
    """Get item type for a given insert."""
    return insert.item_type


def get_insert_item_type_and_index(insert: Insert) -> Tuple[str, int]:
    """Get item type and index for a given insert."""
    return insert.item_type, insert.index


def group_inserts_by_type(
    inserts: Iterable[Insert],
) -> Iterator[Tuple[str, Iterator[Insert]]]:
    """Group all inserts by item type."""
    sorted_inserts = sorted(inserts, key=get_insert_item_type_and_index)
    return (
        (item_type, list(inserts))
        for item_type, inserts in itertools.groupby(
            sorted_inserts, key=get_insert_item_type
        )
    )


def write_inserts_for_type(
    item_type: str,
    inserts_for_type: Iterable[Insert],
    inserts_path: Path,
) -> None:
    """Write all inserts for a given item type to given directory."""
    insert_file = inserts_path.joinpath(f"{item_type}.json")
    with insert_file.open("w") as file_handle:
        json.dump(
            [insert.properties for insert in inserts_for_type],
            file_handle,
            indent=4,
        )


def main():
    """Update the inserts from a given portal.

    Use `--item-type` to update existing inserts for specific item types.
    Use `--from-search` to update inserts from a search result.
    """
    logging.basicConfig()
    logging.getLogger("encoded").setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(  # noqa
        description="Update Inserts",
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--env", default="data", help="Environment to update from. Defaults to data"
    )
    parser.add_argument(
        "--portal",
        help="Portal to update inserts from. Defaults to SMaHT",
        choices=creds_utils._KEY_MANAGERS.keys(),
        default="smaht",
    )
    parser.add_argument(
        "--dest",
        default="temp-local-inserts",
        choices=INSERT_DIRECTORIES,
        help="Destination inserts directory. Defaults to temp-local-inserts.",
    )
    parser.add_argument(
        "--item-type",
        nargs="+",
        help=(
            "Existing item type (e.g. file_fastq) to update inserts for."
            " Defaults to all types found in destination directory.",
        ),
    )
    parser.add_argument(
        "--ignore-field",
        nargs="+",
        default=DEFAULT_IGNORE_FIELDS,
        help="Properties to ignore when pulling inserts",
    )
    parser.add_argument(
        "--from-search",
        help="Query to find new items to add to inserts",
        type=str,
    )
    args = parser.parse_args()

    ignore_fields = get_ignore_fields(args.ignore_field)
    auth_key = get_auth_key(args.portal, args.env)
    inserts_path = INSERTS_LOCATION.joinpath(args.dest)
    if not inserts_path.exists():
        inserts_path.mkdir()
    update_inserts_from_server(
        inserts_path,
        auth_key,
        ignore_fields,
        item_types=args.item_type,
        from_search=args.from_search,
    )


if __name__ == "__main__":
    main()
