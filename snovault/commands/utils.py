from dcicutils import creds_utils


def get_auth_key(portal: str, env: str) -> str:
    """Get the auth key for the given portal and environment."""
    key_manager = creds_utils.KeyManager.create(portal)
    return key_manager.get_keydict_for_env(env)
