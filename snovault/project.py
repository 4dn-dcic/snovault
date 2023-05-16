import os
import toml

from pkg_resources import resource_filename
from dcicutils.env_utils import EnvUtils
from dcicutils.misc_utils import classproperty


# This isn't the home of snovault, but the home of the snovault-based application.
# So in CGAP, for example, this would want to be the home of the CGAP application.
# If not set, it will be assumed that the current working directory is that.
APPLICATION_PROJECT_HOME = os.environ.get("APPLICATION_PROJECT_HOME", os.path.abspath(os.curdir))
_PYPROJECT_TOML_FILE = os.path.join(APPLICATION_PROJECT_HOME, "pyproject.toml")
PYPROJECT_TOML_FILE = _PYPROJECT_TOML_FILE if os.path.exists(_PYPROJECT_TOML_FILE) else None
PYPROJECT_TOML = toml.load(PYPROJECT_TOML_FILE) if PYPROJECT_TOML_FILE else None
POETRY_DATA = PYPROJECT_TOML['tool']['poetry'] if PYPROJECT_TOML else None

_DECLARED_PYPROJECT_NAME = os.environ.get("APPLICATION_PYPROJECT_NAME")
_INFERRED_PYPROJECT_NAME = POETRY_DATA['name'] if POETRY_DATA else None
if _DECLARED_PYPROJECT_NAME and _INFERRED_PYPROJECT_NAME and _DECLARED_PYPROJECT_NAME != _INFERRED_PYPROJECT_NAME:
    raise RuntimeError(f"APPLICATION_PYPROJECT_NAME={_DECLARED_PYPROJECT_NAME!r} but {PYPROJECT_TOML_FILE}"
                       f" says it should be {_INFERRED_PYPROJECT_NAME!r}")
PYPROJECT_NAME = _DECLARED_PYPROJECT_NAME or _INFERRED_PYPROJECT_NAME


def project_filename(filename):
    # TODO: In fact we should do this based on the working dir so that when this is imported to another repo,
    #       it gets the inserts out of that repo's tests, not our own.
    return resource_filename(Project.PACKAGE_NAME, filename)


class ProjectRegistry:

    REGISTERED_PROJECTS = {}

    @classmethod
    def register(cls, name):
        """
        Registers a class to be used based on the name in the top of pyproject.toml.
        Note that this means that cgap-portal and fourfront will both register as 'encoded',
        as in:

            @Project.register('encoded')
            class FourfrontProject(EncodedCoreProject):
                PRETTY_NAME = "Fourfront"

        Since fourfront and cgap-portal don't occupy the same space, no confusion should result.
        """
        def _wrap_class(the_class):
            the_class_name = the_class.__name__
            if not issubclass(the_class, Project):
                raise ValueError(f"The class {the_class_name} must inherit from Project.")
            lower_registry_name = name.lower()
            for x in ['cgap-portal', 'fourfront', 'smaht']:
                if x in lower_registry_name:
                    # It's an easy error to make, but the name of the project from which we're gaining foothold
                    # in pyproject.toml is 'encoded', not 'cgap-portal', etc., so the name 'encoded' will be
                    # needed for bootstrapping. So it should look like
                    # -kmp 15-May-2023
                    raise ValueError(f"Please use ProjectRegistry.register('encoded'),"
                                     f" not ProjectRegistry.register({name!r})."
                                     f" This registration is just for bootstrapping."
                                     f" The class can still be {the_class_name}.")
            cls.REGISTERED_PROJECTS[name] = the_class
            return the_class
        return _wrap_class

    @classmethod
    def _lookup(cls, name):
        """
        Returns the project object with the given name.

        :param name: a string name that was used in a ProjectRegistry.register decorator

        NOTE: There is no need for this function to be called outside of this class except for testing.
              Really only one of these should be instantiated per running application, and that's
              done automatically by this class.
        """
        project_class = cls.REGISTERED_PROJECTS.get(name)
        return project_class

    @classmethod
    def _make_project(cls):
        """
        Creates and returns an instantiated project object for the current project.

        The project to use can be specified by setting the environment variable APPLICATION_PROJECT_HOME
        to a particular directory that contains the pyproject.toml file to use.
        If no such variable is set, the current working directory is used.

        NOTE: There is no need for this function to be called outside of this class except for testing.
              Really only one of these should be instantiated per running application, and that's
              done automatically by this class.
        """
        project_class = cls._lookup(PYPROJECT_NAME)
        assert issubclass(project_class, Project)
        project: Project = project_class()
        return project  # instantiate and return

    _app_project = None
    _initialized = False

    @classmethod
    def initialize(cls):
        if cls._initialized:
            raise RuntimeError(f"{cls.__name__}.initialize() was called more than once.")
        cls._app_project = cls._make_project()
        cls._initalized = True
        app_project: Project = cls.app_project
        return app_project  # It's initialized now, so we use the proper interface

    @classproperty
    def app_project(cls):  # noQA - PyCharm thinks we should use 'self'
        """
        Once the project is initialized, ProjectRegistry.app_project returns the application object
        that should be used to dispatch project-dependent behavior.
        """
        if cls._app_project is None:
            # You need to put a call to
            raise RuntimeError(f"Attempt to access {cls.__name__}.project before .initialize() called.")
        return cls._app_project


class Project:
    """
    A class that should be a superclass of all classes registered using ProjectRegistry.register

    All such classes have these names:
      .NAME - The name of the project in pyproject.toml
      .PACKAGE_NAME - The pypi name of the project, useful for pkg_resources, for example.
      .PRETTY_NAME - The pretty name of the package name
      .APP_NAME - The ame of the project application (see dcicutils.common and the orchestrated app in EnvUtils)
      .APP_PRETTY_NAME - The pretty name of the project application.

    Some sample usess of pre-defined attributes of a Project that may help motivate the choice of attribute names:

      registered  |
         name     |     NAME     |  PACKAGE_NAME  |  PRETTY NAME |  APP_NAME | APP_PRETTY_NAME
     -------------+--------------+----------------+--------------+-----------+----------------
     snovault     | dcicsnovault |  snovault      | Snovault     | snovault  | Snovault
     encoded-core | encoded-core |  encoded-core  | Encoded Core | core      | Core
     encoded      | cgap-portal  |  cgap-portal   | CGAP Portal  | cgap      | CGAP
     encoded      | fourfront    |  fourfront     | Fourfront    | fourfront | Fourfront
     encoded      | smaht-portal |  smaht-portal  | SMaHT Portal | smaht     | SMaHT

     The registered name is the one used with the ProjectRegistry.register() decorator.
    """

    NAME = 'project'

    @classmethod
    def _prettify(cls, name):
        return name.title().replace("Cgap", "CGAP").replace("Smaht", "SMaHT").replace("-", " ")

    @classproperty
    def PACKAGE_NAME(cls):  # noQA - PyCharm wants the variable name to be self
        return cls.NAME.replace('dcic', '')

    @classproperty
    def PRETTY_NAME(cls):  # noQA - PyCharm wants the variable name to be self
        return cls._prettify(cls.PACKAGE_NAME)

    @classproperty
    def APP_NAME(cls):  # noQA - PyCharm wants the variable name to be self
        return cls.PACKAGE_NAME.replace('-portal', '').replace('encoded-', '')

    @classproperty
    def APP_PRETTY_NAME(cls):  # noQA - PyCharm wants the variable name to be self
        return cls._prettify(cls.APP_NAME)

    @classproperty
    def app_project(cls):  # noQA - PyCharm wants the variable name to be self
        """
        Project.app_project returns the actual instantiated project for app-specific behavior,
        which might be of this class or one of its subclasses.

        This access will fail if the project has not been initialized.
        """
        return ProjectRegistry.app_project

    @classmethod
    def initialize_app_project(cls, initialize_env_utils=True):
        if initialize_env_utils:
            EnvUtils.init()
        project: Project = ProjectRegistry.initialize()
        cls.show_herald()
        return project

    @classmethod
    def show_herald(cls):
        print("=" * 80)
        print(f"APPLICATION_PROJECT_HOME == {APPLICATION_PROJECT_HOME!r}")
        print(f"PYPROJECT_TOML_FILE == {PYPROJECT_TOML_FILE!r}")
        print(f"PYPROJECT_NAME == {PYPROJECT_NAME!r}")
        the_app_project = Project.app_project
        the_app_project_class = the_app_project.__class__
        the_app_project_class_name = the_app_project_class.__name__
        assert (Project.app_project
                == app_project()
                == the_app_project_class.app_project
                == the_app_project.app_project), (
            "Project consistency check failed."
        )
        print(f"{the_app_project_class_name}.app_project == Project.app_project == app_project() == {app_project()!r}")
        print(f"app_project().NAME == {app_project().NAME!r}")
        print(f"app_project().PRETTY_NAME == {app_project().PRETTY_NAME!r}")
        print(f"app_project().PACKAGE_NAME == {app_project().PACKAGE_NAME!r}")
        print(f"app_project().APP_NAME == {app_project().APP_NAME!r}")
        print(f"app_project().APP_PRETTY_NAME == {app_project().APP_PRETTY_NAME!r}")
        print("=" * 80)


def app_project(initialize=False):
    if initialize:
        Project.initialize_app_project()
    return ProjectRegistry.app_project