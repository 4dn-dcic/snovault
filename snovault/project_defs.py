from .project import (
    ProjectRegistry,
    Project as _Project  # renamed to avoid confusing programmers using discovery into thinking this is an export
)


@ProjectRegistry.register('dcicsnovault')
class SnovaultProject(_Project):
    NAME = 'snovault'
