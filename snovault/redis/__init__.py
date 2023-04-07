# __init___.py for redis


def includeme(config):
    config.include('.redis_connection')
