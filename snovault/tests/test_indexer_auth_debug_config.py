"""
Configuration regression: Pyramid authorization debugging must stay disabled in
the production-like configs that snovault ships.

`pyramid.debug_authorization` wraps every permission check in an extra
`authdebug_view` tween. It is a developer diagnostic only; leaving it on in a
production/indexer configuration adds per-view overhead to the exact hot path the
indexer drives (`@@object`/`@@embedded` renders run one secured subrequest per
embedded item), for no functional benefit.

Snovault itself does not host the authoritative production/indexer INI -- that
lives in the consumer application (e.g. smaht-portal). What snovault ships are the
shared application defaults (`base.ini`) and the deployment templates
(`development.ini.template`, `test.ini.template`). This test locks in that none of
those enable the flag (it is either absent -> Pyramid default False, or explicitly
false), so an accidental `pyramid.debug_authorization = true` cannot slip into a
shipped config unnoticed.

Note: the pytest app fixture (`testappfixtures._app_settings`) deliberately sets
`pyramid.debug_authorization = True` so the authorization-debug code path is
exercised during tests. That is intentional test behavior and is not touched here.

This is a static config check (it does not itself use Elasticsearch), so it runs
in the fast, non-ES (`not indexing`) partition.
"""
import configparser
import os

import pytest
import snovault
from pyramid.settings import asbool


# snovault.__file__ -> <repo>/snovault/__init__.py; two dirnames -> <repo>.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(snovault.__file__)))

# Shipped, production-like configs. All must keep authorization debugging off.
SHIPPED_CONFIG_FILES = [
    'base.ini',
    'development.ini.template',
    'test.ini.template',
]


def _debug_authorization_values(ini_path):
    """Return every authorization-debug setting or PasteDeploy override.

    PasteDeploy permits settings in the app section and ``set`` overrides in
    pipeline/composite sections. Inspect every section so the regression cannot
    be bypassed by moving the same setting outside ``app:app``.
    """
    parser = configparser.ConfigParser(interpolation=None)
    with open(ini_path) as f:
        parser.read_file(f)
    values = []
    for section in parser.sections():
        for option, value in parser.items(section):
            normalized_option = option[4:] if option.startswith('set ') else option
            if normalized_option == 'pyramid.debug_authorization':
                values.append((section, option, value))
    return values


def test_debug_authorization_values_finds_pastedeploy_override(tmp_path):
    ini_path = tmp_path / 'override.ini'
    ini_path.write_text(
        '[pipeline:indexer]\n'
        'pipeline = app\n'
        'set pyramid.debug_authorization = true\n'
    )
    assert _debug_authorization_values(str(ini_path)) == [
        ('pipeline:indexer', 'set pyramid.debug_authorization', 'true'),
    ]


@pytest.mark.parametrize('config_file', SHIPPED_CONFIG_FILES)
def test_shipped_config_does_not_enable_debug_authorization(config_file):
    ini_path = os.path.join(REPO_ROOT, config_file)
    assert os.path.exists(ini_path), f'expected shipped config missing: {ini_path}'
    values = _debug_authorization_values(ini_path)
    # Absent (-> Pyramid default False) or explicitly falsy are both acceptable;
    # a truthy value anywhere in a shipped config is the regression.
    enabled = [(section, option, value)
               for section, option, value in values if asbool(value)]
    assert not enabled, (
        f'{config_file} enables pyramid.debug_authorization at {enabled!r}; it '
        f'must stay disabled in production-like configuration.'
    )
