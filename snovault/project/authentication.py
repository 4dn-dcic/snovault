# Authentication related functions which may be overriden by an implementing app,
# e.g. Foursight or CGAP portal, using the dcicutils project_utils mechanism.

from ..authentication import login, logout

class SnovaultProjectAuthentication:

    def login(self, context, request, *, samesite):
        return login(context, request, samesite=samesite)

    def logout(self, context, request):
        return logout(context, request)

    def namespaced_authentication_policy_authenticated_userid(self, namespaced_authentication_policy, request, set_user_info_property):
        return namespaced_authentication_policy._authenticated_userid_implementation(request, set_user_info_property)

    def namespaced_authentication_policy_unauthenticated_userid(self, namespaced_authentication_policy, request):
        return namespaced_authentication_policy._unauthenticated_userid_implementation(request)

    def note_auth0_authentication_policy_unauthenticated_userid(self, auth0_authentication_policy, request, email, id_token):
        pass

    # TODO: Maybe something like ...
    # def __init__(self):
    #    self.login_policy = 


# TODO: Maybe something like ...
# def SnovaultNamespacedAuthenticationPolicy:
#     def __init__(self, app_project):
#         self.app_project = app_project
#     def authenticated_userid(self, namespaced_authentication_policy, request, set_user_info_property):
#         return namespaced_authentication_policy._authenticated_userid_implementation(request, set_user_info_property)
