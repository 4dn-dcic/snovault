"""Unit tests for explicit selective deployment mapping behavior."""

from types import SimpleNamespace

import pytest

from ..commands import create_mapping_on_deploy


pytestmark = [pytest.mark.unit]


def deploy_args(**overrides):
    values = {
        'wipe_es': False,
        'clear_queue': False,
        'selective_reindex': False,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_selective_deploy_option_enables_signature_comparison(monkeypatch):
    calls = []
    monkeypatch.setattr(
        create_mapping_on_deploy,
        'get_deployment_config',
        lambda app: {'ENV_NAME': 'smaht-production', 'WIPE_ES': False},
    )
    monkeypatch.setattr(
        create_mapping_on_deploy, 'run_create_mapping',
        lambda app, **kwargs: calls.append(kwargs),
    )
    monkeypatch.setattr(create_mapping_on_deploy, 'loadxl_order', lambda: ['file'])

    create_mapping_on_deploy._run_create_mapping(
        object(), deploy_args(selective_reindex=True)
    )

    assert calls == [{
        'check_first': True,
        'purge_queue': False,
        'item_order': ['file'],
        'selective_reindex': True,
    }]


def test_existing_non_wipe_deploy_behavior_remains_mapping_only(monkeypatch):
    calls = []
    monkeypatch.setattr(
        create_mapping_on_deploy,
        'get_deployment_config',
        lambda app: {'ENV_NAME': 'production', 'WIPE_ES': False},
    )
    monkeypatch.setattr(
        create_mapping_on_deploy, 'run_create_mapping',
        lambda app, **kwargs: calls.append(kwargs),
    )
    monkeypatch.setattr(create_mapping_on_deploy, 'loadxl_order', lambda: [])

    # A deployment caller with a pre-option args object remains compatible.
    args = SimpleNamespace(wipe_es=False, clear_queue=True)
    create_mapping_on_deploy._run_create_mapping(object(), args)

    assert calls[0]['check_first'] is True
    assert calls[0]['selective_reindex'] is False
    assert calls[0]['purge_queue'] is True


def test_explicit_wipe_preserves_full_reindex(monkeypatch):
    calls = []
    monkeypatch.setattr(
        create_mapping_on_deploy,
        'get_deployment_config',
        lambda app: {'ENV_NAME': 'production', 'WIPE_ES': False},
    )
    monkeypatch.setattr(
        create_mapping_on_deploy, 'run_create_mapping',
        lambda app, **kwargs: calls.append(kwargs),
    )
    monkeypatch.setattr(create_mapping_on_deploy, 'loadxl_order', lambda: [])

    create_mapping_on_deploy._run_create_mapping(
        object(), deploy_args(wipe_es=True)
    )

    assert calls[0]['check_first'] is False
    assert calls[0]['selective_reindex'] is False


def test_selective_option_cannot_downgrade_configured_wipe(monkeypatch):
    monkeypatch.setattr(
        create_mapping_on_deploy,
        'get_deployment_config',
        lambda app: {'ENV_NAME': 'test', 'WIPE_ES': True},
    )

    with pytest.raises(SystemExit) as error:
        create_mapping_on_deploy._run_create_mapping(
            object(), deploy_args(selective_reindex=True)
        )

    assert error.value.code == 1
