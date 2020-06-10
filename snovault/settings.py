from dcicutils.misc_utils import get_setting_from_context

# Global snovault settings file, contained in the Settings class
# Declare default values for options below as part of the class, set them as needed in
# 'includeme' below.


def includeme(config):
    settings = config.registry.settings
    nested_is_set = get_setting_from_context(settings, 'mappings.use_nested', env_var='MAPPINGS_USE_NESTED')
    if nested_is_set:
        Settings.MAPPINGS_USE_NESTED = True


class Settings:
    MAPPINGS_USE_NESTED = False