from dcicutils.misc_utils import utc_now_str
from snovault.schema_validation import NO_DEFAULT
from .server_defaults_user import get_userid


def get_now():
    """ Wrapper for the server_default 'now' above so it is not called through SERVER_DEFAULTS in our code """
    return utc_now_str()


def add_last_modified(properties, **kwargs):
    """
        Uses the above two functions to add the last_modified information to the item
        May have no effect
        Allow someone to override the request userid (none in this case) by passing in a different uuid
        CONSIDER: `last_modified` (and `last_text_edited`) are not really 'server defaults' but rather system-managed fields.
    """

    userid = kwargs.get("userid", None)
    field_name_portion = kwargs.get("field_name_portion", "modified")

    last_field_name = "last_" + field_name_portion  # => last_modified
    by_field_name = field_name_portion + "_by"      # => modified_by
    date_field_name = "date_" + field_name_portion  # => date_modified

    try:
        last_modified = {
            by_field_name: get_userid(),
            date_field_name: get_now(),
        }
    except AttributeError:  # no request in scope ie: we are outside the core application.
        if userid:
            last_modified = {
                by_field_name: userid,
                date_field_name: get_now(),
            }
            properties[last_field_name] = last_modified
    else:
        # get_userid returns NO_DEFAULT if no userid
        if last_modified[by_field_name] != NO_DEFAULT:
            properties[last_field_name] = last_modified
