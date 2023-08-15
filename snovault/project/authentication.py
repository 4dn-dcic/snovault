# Authentication related functions which may be overriden by an implementing app,
# e.g. Foursight or CGAP portal, using the dcicutils project_utils mechanism.
from dcicutils.misc_utils import ignored
from ..authentication import login, logout


class SnovaultProjectAuthentication:

    @staticmethod
    def login(context, request, *, samesite):
        return login(context, request, samesite=samesite)

    @staticmethod
    def logout(context, request):
        return logout(context, request)

    @staticmethod
    def namespaced_authentication_policy_authenticated_userid(namespaced_authentication_policy, request, set_user_info_property):
        return namespaced_authentication_policy._authenticated_userid_implementation(request, set_user_info_property)

    @staticmethod
    def namespaced_authentication_policy_unauthenticated_userid(namespaced_authentication_policy, request):
        return namespaced_authentication_policy._unauthenticated_userid_implementation(request)

    @staticmethod
    def note_auth0_authentication_policy_unauthenticated_userid(auth0_authentication_policy, request, email, id_token):
        pass

    def env_allows_auto_registration(self, env_name):
        """ Default: Allow user registration everywhere """
        ignored(env_name)
        return True

    # TODO: Maybe something like ...
    # def __init__(self):
    #    self.login_policy =


# TODO: Maybe something like ...
# def SnovaultNamespacedAuthenticationPolicy:
#     def __init__(self, app_project):
#         self.app_project = app_project
#     def authenticated_userid(self, namespaced_authentication_policy, request, set_user_info_property):
#         return namespaced_authentication_policy._authenticated_userid_implementation(request, set_user_info_property)
